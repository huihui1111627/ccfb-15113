package proto

import (
	"fmt"
	"sort"
)

// ---- heartbeat & failure suspicion ----

func (m *Member) timerHeartbeat() {
	defer m.sched(HeartbeatPeriod, m.timerHeartbeat)
	if m.phase == PhaseJoining {
		// Joiner keeps asking to come in instead.
		m.broadcastJoinReq()
		return
	}
	if m.phase != PhaseNormal {
		return
	}
	// Direct, unreliable heartbeat. Suspicions require many consecutive
	// missed rounds (see timerSuspectScan), so ordinary loss never causes a
	// false view change; keeping heartbeats off the reliable FIFO channel
	// means a data gap cannot head-of-line-block the liveness signal.
	hb := Heartbeat{View: m.view, From: m.ID}
	for _, to := range m.members {
		if to != m.ID {
			m.sendDirect(to, &hb)
		}
	}
}

func (m *Member) handleHeartbeat(h Heartbeat) {
	if m.phase == PhaseJoining {
		return
	}
	if h.View == m.view {
		m.heartbeats[h.From] = m.now()
	}
}

// timerSuspectScan implements a simple coordinator rule: only the lowest-id
// member of the installed view initiates view changes. It suspects a peer
// after SuspectTimeout ticks without a heartbeat, computes the survivor set,
// and starts the flush. If the coordinator itself is partitioned, the next
// lowest survivor times out its flush-start reception and takes over.
func (m *Member) timerSuspectScan() {
	defer m.sched(HeartbeatPeriod, m.timerSuspectScan)
	if m.phase != PhaseNormal || m.ID != m.members[0] {
		return
	}
	// Grace period after a view install: liveness state has to rebuild from
	// scratch, so suspecting anyone here would fire on healthy members and
	// trigger a cascade of spurious view changes.
	if m.now()-m.viewInstalledAt < SuspectTimeout*2 {
		return
	}
	now := m.now()
	dead := []int{}
	for _, id := range m.members {
		if id == m.ID {
			continue
		}
		if now-m.heartbeats[id] > SuspectTimeout {
			dead = append(dead, id)
		}
	}
	if len(dead) > 0 {
		survivors := make([]int, 0, len(m.members)-len(dead))
		for _, id := range m.members {
			if !containsInt(dead, id) {
				survivors = append(survivors, id)
			}
		}
		m.startFlush(survivors, nil, "suspect "+intsText(dead))
	}
}

// ---- joining ----

// InitJoining marks a freshly started incarnation as a joiner.
func (m *Member) InitJoining() {
	m.phase = PhaseJoining
	m.sched(HeartbeatPeriod, m.broadcastJoinReq)
}

func (m *Member) broadcastJoinReq() {
	if m.phase != PhaseJoining {
		return
	}
	jr := JoinReq{NewID: m.ID, From: m.ID}
	// The joiner does not know the membership yet: the simulator gives it a
	// candidate list via joinCandidates; send to every known live id.
	for _, to := range m.joinCandidates() {
		m.sendDirect(to, &jr)
	}
}

func (m *Member) handleJoinReq(j JoinReq) {
	if m.phase != PhaseNormal || m.ID != m.members[0] {
		// Only the coordinator admits joiners; non-coordinators ignore.
		// A flushing coordinator re-evaluates after its current flush.
		return
	}
	if containsInt(m.members, j.NewID) {
		return
	}
	m.startFlush(append([]int(nil), m.members...), []int{j.NewID},
		fmt.Sprintf("join m%d", j.NewID))
}

// ---- flush protocol ----

func (m *Member) startFlush(survivors, joiners []int, reason string) {
	if m.phase == PhaseFlushing {
		return
	}
	m.phase = PhaseFlushing
	m.flushCoord = m.ID
	m.flushSurvivors = append([]int(nil), survivors...)
	m.flushJoiners = append([]int(nil), joiners...)
	m.flushStates = map[int]FlushState{}
	m.flushUnion = map[MsgID]Envelope{}
	m.activeFC = nil
	m.installAcks = map[int]bool{}
	m.stateAcked = false
	m.completeWaits = 0
	m.flushFetched = map[MsgID]bool{}
	m.flushSeenStart = map[int]bool{}
	m.nextViewID = m.view + 1
	sort.Ints(m.flushSurvivors)
	sort.Ints(m.flushJoiners)
	m.rec.Record(m.now(), m.ID, EvViewChange, nil, m.view,
		fmt.Sprintf("flush start view %d->%d (%s); survivors=%v joiners=%v",
			m.view, m.nextViewID, reason, m.flushSurvivors, m.flushJoiners), nil)
	// The coordinator is itself a survivor and contributes its own state
	// directly; only the other survivors report over the network.
	m.flushStates[m.ID] = m.snapshotOwnState()
	m.sched(FlushTimer, m.timerFlushRetry)
	m.sendFlushStart()
}

// snapshotOwnState builds the coordinator's FlushState from its local
// holdings. Crucially it includes not just envelopes that reached the upper
// layers (envsByMsg) but every AppData envelope sitting in the FIFO repair
// cache behind an unrepaired head gap — otherwise a crashed sender's message
// that a survivor holds only in its cache would be wrongly dropped from the
// flush union.
func (m *Member) snapshotOwnState() FlushState {
	envs := m.allHeldAppEnvelopes()
	delivered := []MsgID{}
	finalOf := map[MsgID]int64{}
	for id := range m.appByID {
		if _, pending := m.headPending[id]; !pending {
			if _, buffered := m.causalBuf[id]; !buffered {
				delivered = append(delivered, id)
				if no, ok := m.finals[id]; ok {
					finalOf[id] = no
				}
			}
		}
	}
	return FlushState{View: m.view, From: m.ID, Received: envs, Delivered: delivered, FinalOf: finalOf}
}

// allHeldAppEnvelopes returns every AppData envelope the member holds, whether
// it has cleared the FIFO layer or is still cached behind a gap.
func (m *Member) allHeldAppEnvelopes() []Envelope {
	seen := map[MsgID]bool{}
	envs := []Envelope{}
	add := func(env Envelope) {
		app, ok := env.Payload.(*AppData)
		if !ok || seen[app.ID] {
			return
		}
		seen[app.ID] = true
		envs = append(envs, env)
	}
	for _, env := range m.envsByMsg {
		add(env)
	}
	for _, cache := range m.cache {
		for _, env := range cache {
			add(env)
		}
	}
	sort.Slice(envs, func(i, j int) bool {
		a, b := envs[i], envs[j]
		if a.Origin != b.Origin {
			return a.Origin < b.Origin
		}
		return a.FIFOSeq < b.FIFOSeq
	})
	return envs
}

func (m *Member) sendFlushStart() {
	acked := make([]int, 0, len(m.flushStates))
	for id := range m.flushStates {
		acked = append(acked, id)
	}
	sort.Ints(acked)
	fs := FlushStart{
		NewView:   m.nextViewID,
		OldView:   m.view,
		Coord:     m.ID,
		Survivors: append([]int(nil), m.flushSurvivors...),
		Joiners:   append([]int(nil), m.flushJoiners...),
		Acked:     acked,
	}
	for _, to := range m.flushSurvivors {
		if to == m.ID {
			continue
		}
		m.sendDirect(to, &fs)
	}
	for _, to := range m.flushJoiners {
		m.sendDirect(to, &fs)
	}
}

// timerFlushRetry retransmits whichever flush message the coordinator is
// currently waiting on, and aborts the flush if a survivor itself died while
// flushing (the suspect scan will later re-trigger with the new set).
func (m *Member) timerFlushRetry() {
	if m.phase != PhaseFlushing || m.flushCoord != m.ID {
		return // view installed (or leadership lost): timer chain stops
	}
	// Survivor liveness check.
	for _, id := range m.flushSurvivors {
		if id != m.ID && !m.tr.IsAlive(id) {
			// Restart the whole flush against the reduced survivor set.
			alive := []int{}
			for _, x := range m.flushSurvivors {
				if m.tr.IsAlive(x) {
					alive = append(alive, x)
				}
			}
			m.phase = PhaseNormal
			m.startFlush(alive, m.flushJoiners, "survivor died during flush")
			return
		}
	}
	if len(m.flushStates) < len(m.flushSurvivors) {
		// Broadcast FlushStart (carries Acked feedback) and additionally unicast
		// a state pull to each survivor still missing, so a persistently dropped
		// state report does not stall the flush indefinitely.
		m.sendFlushStart()
		for _, id := range m.flushSurvivors {
			if id == m.ID {
				continue
			}
			if _, ok := m.flushStates[id]; !ok {
				m.sendDirect(id, &FlushStateReq{NewView: m.nextViewID, From: m.ID})
			}
		}
		m.sched(FlushTimer, m.timerFlushRetry)
		return
	}
	if !m.flushDataComplete() {
		m.sched(FlushTimer, m.timerFlushRetry)
		return
	}
	// Closure is complete: build it once, then keep retransmitting until every
	// new-view member acknowledges installation. The coordinator stays in the
	// flushing phase until then so this timer chain keeps running.
	if m.activeFC == nil {
		m.activeFC = m.buildFlushComplete()
		// The coordinator counts itself as installed once it builds the
		// closure (its own reconciliation runs at application time below).
		m.installAcks[m.ID] = true
	}
	fc := m.activeFC
	for _, to := range fc.Members {
		if to == m.ID || !m.tr.IsAlive(to) {
			continue
		}
		if !m.installAcks[to] {
			m.sendDirect(to, fc)
		}
	}
	// Self-install exactly once, after the closure is available.
	if !m.completedOld[fc.OldView] {
		m.installNewView(fc)
	}
	if len(m.installAcks) >= len(fc.Members) {
		return // everyone installed; stop the retransmit chain
	}
	m.sched(FlushTimer, m.timerFlushRetry)
}

// handleInstallAck records that a member has applied the active FlushComplete.
func (m *Member) handleInstallAck(a InstallAck) {
	if m.phase != PhaseFlushing || m.flushCoord != m.ID || m.activeFC == nil {
		return
	}
	if a.NewView != m.activeFC.NewView {
		return
	}
	m.installAcks[a.From] = true
}

func (m *Member) handleFlushStart(fs FlushStart) {
	// Two cases: an old-view survivor, or a joiner.
	isSurvivor := containsInt(fs.Survivors, m.ID)
	isJoiner := containsInt(fs.Joiners, m.ID)
	if !isSurvivor && !isJoiner {
		return
	}
	if m.phase == PhaseFlushing {
		// Retransmitted FlushStart from the same coordinator: it is both a
		// liveness signal and, via its Acked list, confirmation that our state
		// report has been received.
		if m.flushCoord == fs.Coord {
			m.flushCoordHeard = m.now()
			if containsInt(fs.Acked, m.ID) {
				m.stateAcked = true
			}
		}
		return // already engaged in this (or another) flush
	}
	if isSurvivor && fs.OldView < m.view {
		return // stale flush from an already-superseded view
	}
	m.phase = PhaseFlushing
	m.flushCoord = fs.Coord
	m.flushSurvivors = append([]int(nil), fs.Survivors...)
	m.flushJoiners = append([]int(nil), fs.Joiners...)
	m.nextViewID = fs.NewView
	m.flushCoordHeard = m.now()
	m.rec.Record(m.now(), m.ID, EvViewChange, nil, m.view,
		fmt.Sprintf("flush start received, view ->%d", fs.NewView), nil)
	m.sched(FlushTimer, m.timerFlushAckRetry)
	m.sendFlushState()
}

// timerFlushAckRetry: a non-coordinator keeps re-sending its state report.
// If the coordinator has gone silent (crash or network partition), the lowest
// surviving member promotes itself and starts a fresh flush under a new view
// id — the old coordinator's stale FlushStart/Complete packets are rejected
// because their view id no longer matches.
func (m *Member) timerFlushAckRetry() {
	if m.phase != PhaseFlushing {
		return // installed: timer chain stops
	}
	// Coordinator is gone only if it is dead OR its flush traffic has gone
	// silent well beyond a couple of retransmit rounds. Heartbeats pause
	// during a flush, so the FlushStart retransmit (flushCoordHeard) is the
	// real liveness signal here.
	coordSilent := !m.tr.IsAlive(m.flushCoord) ||
		m.now()-m.flushCoordHeard > SuspectTimeout
	if coordSilent {
		alive := []int{}
		for _, x := range m.flushSurvivors {
			if m.tr.IsAlive(x) {
				alive = append(alive, x)
			}
		}
		sort.Ints(alive)
		if len(alive) > 0 && alive[0] == m.ID {
			m.phase = PhaseNormal
			// Fence off the old coordinator: it was using nextViewID for its
			// flush, so take over under nextViewID+1. Any late FlushStart or
			// FlushComplete carrying the old number is rejected as stale.
			takeOverView := m.nextViewID + 1
			m.view = takeOverView
			m.nextViewID = takeOverView + 1
			m.startFlush(alive, m.flushJoiners, "coordinator silent, took over")
			return
		}
	}
	// Keep re-sending our state until the coordinator echoes us in Acked.
	// Once acknowledged, the remaining wait is for the FlushComplete; if that
	// packet keeps getting dropped we actively re-pull it instead of stalling.
	if m.ID != m.flushCoord {
		if !m.stateAcked {
			m.sendFlushState()
		} else {
			m.completeWaits++
			if m.completeWaits%2 == 0 {
				m.sendDirect(m.flushCoord, &CompleteReq{NewView: m.nextViewID, From: m.ID})
			}
		}
	}
	m.sched(FlushTimer, m.timerFlushAckRetry)
}

func (m *Member) sendFlushState() {
	// Include cached-but-not-yet-FIFO-delivered envelopes so a crashed
	// sender's head-gap messages still enter the flush union.
	envs := m.allHeldAppEnvelopes()
	delivered := []MsgID{}
	finalOf := map[MsgID]int64{}
	for id := range m.appByID {
		if _, stillPending := m.headPending[id]; !stillPending {
			if _, buffered := m.causalBuf[id]; !buffered {
				delivered = append(delivered, id)
				if no, ok := m.finals[id]; ok {
					finalOf[id] = no
				}
			}
		}
	}
	fs := FlushState{
		View:      m.view,
		From:      m.ID,
		Received:  envs,
		Delivered: delivered,
		FinalOf:   finalOf,
	}
	m.sendDirect(m.flushCoord, &fs)
}

func (m *Member) handleFlushState(fs FlushState) {
	if m.phase != PhaseFlushing || m.flushCoord != m.ID {
		return
	}
	if !containsInt(m.flushSurvivors, fs.From) {
		return
	}
	m.flushStates[fs.From] = fs
	// The state report already carries every AppData envelope the survivor
	// holds, so the coordinator can build the union directly — no second
	// fetch round trip that could be lost and stall the flush.
	for _, env := range fs.Received {
		if mid := msgIDOf(env); mid != nil {
			if _, ok := m.flushUnion[*mid]; !ok {
				m.flushUnion[*mid] = env
			}
		}
	}
}

// flushDataComplete reports whether every survivor's state has arrived. The
// union is built incrementally in handleFlushState, so once the state count
// matches the survivor count the closure has everything it needs.
func (m *Member) flushDataComplete() bool {
	return len(m.flushStates) >= len(m.flushSurvivors)
}

func containsInt(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func intsText(xs []int) string {
	s := ""
	for i, x := range xs {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("m%d", x)
	}
	return s
}
