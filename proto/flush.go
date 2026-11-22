package proto

import (
	"fmt"
	"sort"
)

// buildFlushComplete constructs the authoritative closure of the old view.
//
// The union of envelopes held by any survivor defines exactly the messages
// that survive the view. Everything else (a crashed sender's message that no
// survivor ever received) is void: the coordinator asks the runtime for the
// full send list, computes the void set, and FlushComplete carries it so
// surviving senders get the OnVoid callback and the UI/verifier can audit it.
func (m *Member) buildFlushComplete() *FlushComplete {
	// 1. Union of app messages known to any survivor. flushUnion is populated
	//    incrementally as FlushState reports arrive; fold in the coordinator's
	//    own holdings (including FIFO-cached envelopes behind a head gap).
	union := map[MsgID]Envelope{}
	for id, env := range m.flushUnion {
		union[id] = env
	}
	for _, env := range m.allHeldAppEnvelopes() {
		if mid := msgIDOf(env); mid != nil {
			if _, ok := union[*mid]; !ok {
				union[*mid] = env
			}
		}
	}

	// 2. Void set = every message sent in the old view but absent from the
	//    union. The runtime is the only place that knows sends which died with
	//    their sender before reaching anyone.
	allSends := m.lookupAllSends(m.view)
	void := []MsgID{}
	for _, id := range allSends {
		if _, ok := union[id]; !ok {
			void = append(void, id)
		}
	}
	sort.Slice(void, func(i, j int) bool { return msgIDLess(void[i], void[j]) })

	// 3. Build the authoritative final order for the flush closure.
	//
	// A message that has ALREADY been delivered keeps its final number: that
	// number is observable in the application's delivery stream and must not
	// change. Every message not yet delivered — whether it was still waiting
	// on round 2 or merely held in a causal/FIFO buffer — gets a fresh number
	// above all numbers already consumed by deliveries. Those fresh numbers are
	// assigned in one vector-clock topological pass over the undelivered set,
	// which guarantees every causal predecessor receives a smaller number.
	consumedMax := int64(0)
	// A message counts as delivered if ANY survivor reports it in its
	// Delivered list OR the coordinator itself has already delivered it. The
	// whole point of the flush is to make that set identical on everyone, so
	// the union of reported deliveries is the authoritative "already consumed"
	// set and its numbers must be preserved.
	deliveredHere := map[MsgID]bool{}
	for _, st := range m.flushStates {
		for _, id := range st.Delivered {
			deliveredHere[id] = true
		}
	}
	for id := range union {
		_, pending := m.headPending[id]
		_, buffered := m.causalBuf[id]
		_, known := m.appByID[id]
		if known && !pending && !buffered {
			deliveredHere[id] = true
		}
	}
	for id := range deliveredHere {
		if no, ok := m.finals[id]; ok && no > consumedMax {
			consumedMax = no
		}
	}

	appsForTopo := map[MsgID]AppData{}
	undeliveredIDs := make([]MsgID, 0, len(union))
	for id, env := range union {
		if app, ok := env.Payload.(*AppData); ok {
			appsForTopo[id] = *app
		} else if app, ok := m.appByID[id]; ok {
			appsForTopo[id] = app
		}
		if !deliveredHere[id] {
			undeliveredIDs = append(undeliveredIDs, id)
		}
	}
	sortedUndelivered := topoSortByVC(undeliveredIDs, appsForTopo)
	order := make([]FinalOrderEntry, 0, len(union))
	for id := range deliveredHere {
		no, ok := m.finals[id]
		if !ok {
			// Coordinator did not track this final (e.g. the origin crashed),
			// but the message is already delivered somewhere with a number.
			// Recover it from any survivor state's FinalOrder knowledge; fall
			// back to a stable placeholder that will be reconciled below.
			no = m.lookupReportedFinal(id)
		}
		order = append(order, FinalOrderEntry{ID: id, FinalNo: no})
		if no > consumedMax {
			consumedMax = no
		}
	}
	nextNo := consumedMax + 1
	for _, id := range sortedUndelivered {
		order = append(order, FinalOrderEntry{ID: id, FinalNo: nextNo})
		nextNo++
	}

	envs := make([]Envelope, 0, len(union))
	for _, env := range union {
		envs = append(envs, env)
	}
	sort.Slice(envs, func(i, j int) bool {
		a, b := envs[i], envs[j]
		if a.Origin != b.Origin {
			return a.Origin < b.Origin
		}
		return a.FIFOSeq < b.FIFOSeq
	})

	members := append(append([]int(nil), m.flushSurvivors...), m.flushJoiners...)
	sort.Ints(members)
	return &FlushComplete{
		NewView:    m.nextViewID,
		OldView:    m.view,
		Coord:      m.ID,
		Members:    members,
		Union:      envs,
		FinalOrder: order,
		Void:       void,
	}
}

func msgIDLess(a, b MsgID) bool {
	if a.Origin != b.Origin {
		return a.Origin < b.Origin
	}
	return a.Seq < b.Seq
}

// topoSortByVC returns ids in an order where every causal predecessor comes
// first (Kahn over the happens-before relation encoded in the VCs). Concurrent
// messages break ties by MsgID for determinism.
func topoSortByVC(ids []MsgID, apps map[MsgID]AppData) []MsgID {
	remaining := map[MsgID]bool{}
	for _, id := range ids {
		remaining[id] = true
	}
	out := make([]MsgID, 0, len(ids))
	for len(remaining) > 0 {
		ready := []MsgID{}
		for id := range remaining {
			app := apps[id]
			cleared := true
			for dep := range remaining {
				if dep == id {
					continue
				}
				if apps[dep].VC.HappensBefore(app.VC) {
					cleared = false
					break
				}
			}
			if cleared {
				ready = append(ready, id)
			}
		}
		// VCs form a partial order, so at least one minimal element always
		// exists; tie-break concurrent messages by MsgID.
		sort.Slice(ready, func(i, j int) bool { return msgIDLess(ready[i], ready[j]) })
		pick := ready[0]
		out = append(out, pick)
		delete(remaining, pick)
	}
	return out
}

// lookupReportedFinal finds a final number for an already-delivered message
// whose Final packet the coordinator never tracked (typically the origin
// crashed before round 2 completed). Survivors report the number in their
// FlushState.FinalOf, which is authoritative for an already-delivered message.
func (m *Member) lookupReportedFinal(id MsgID) int64 {
	if no, ok := m.finals[id]; ok {
		return no
	}
	for _, st := range m.flushStates {
		if no, ok := st.FinalOf[id]; ok {
			return no
		}
	}
	return 0
}

// SendsLister is implemented by the runtime to give the coordinator the full
// list of messages that were sent in a given view, including ones whose sender
// crashed before anyone received them.
type SendsLister interface {
	SendsInView(view int64) []MsgID
}

func (m *Member) lookupAllSends(view int64) []MsgID {
	if lister, ok := m.tr.(SendsLister); ok {
		return lister.SendsInView(view)
	}
	return nil
}

// VoidNotifier lets the runtime learn void decisions even for senders that
// crashed (their callback can no longer run locally).
type VoidNotifier interface {
	NotifyVoid(msg MsgID, oldView int64)
}

func (m *Member) handleFlushComplete(fc FlushComplete) {
	if m.completedOld[fc.OldView] {
		// Duplicate on a retransmit: re-ack so the coordinator eventually
		// sees our installation even if the first ack was lost.
		if m.flushCoord != m.ID {
			m.sendDirect(m.flushCoord, &InstallAck{NewView: fc.NewView, From: m.ID})
		}
		return
	}
	isMember := false
	for _, x := range fc.Members {
		if x == m.ID {
			isMember = true
			break
		}
	}
	if !isMember {
		return
	}
	// Apply the closure (survivor reconciliation or fresh joiner install).
	m.installNewView(&fc)
	// Tell the coordinator so it can stop retransmitting.
	if fc.Coord != m.ID {
		m.sendDirect(fc.Coord, &InstallAck{NewView: fc.NewView, From: m.ID})
	}
}

// installNewView applies the closure and resets per-view state. For surviving
// members the union is, by construction, a superset of what they held: any
// missing app message is first pumped through the causal and total layers so
// their delivered set becomes identical to every other survivor's.
func (m *Member) installNewView(fc *FlushComplete) {
	m.completedOld[fc.OldView] = true

	// 1. Void callbacks for messages this member itself sent, and runtime-wide
	//    notification for the crashed-sender cases.
	for _, id := range fc.Void {
		if m.ownSends[id] {
			m.app.OnVoid(m.ID, id, fc.OldView)
		}
		if notifier, ok := m.tr.(VoidNotifier); ok {
			notifier.NotifyVoid(id, fc.OldView)
		}
		m.rec.Record(m.now(), m.ID, EvVoid, &id, fc.OldView, "void during flush", nil)
	}

	// 2. Reconcile the OLD view so every survivor has delivered exactly the
	//    union message set, in exactly the coordinator's total order. Already
	//    delivered messages are skipped; missing ones are first registered
	//    with the causal layer (which advances the vector clock, preserving
	//    causal bookkeeping for the UI) and then delivered straight from the
	//    authoritative FinalOrder, bypassing the provisional-gated pump whose
	//    round-1 state does not exist for messages recovered from peers.
	//    Joining members skip all of this: requirement 7 gives them no history.
	isJoiner := m.phase == PhaseJoining
	if !isJoiner {
		appIndex := map[MsgID]AppData{}
		for _, env := range fc.Union {
			if app, ok := env.Payload.(*AppData); ok {
				appIndex[app.ID] = *app
				if _, ok := m.envsByMsg[app.ID]; !ok {
					m.envsByMsg[app.ID] = env
				}
			}
		}
		// Messages held locally but absent from the union payloads (e.g. a
		// message parked in headPending whose envelope the coordinator only
		// saw via control traffic) still need their AppData for delivery.
		for id, app := range m.appByID {
			if _, ok := appIndex[id]; !ok {
				appIndex[id] = app
			}
		}
		// A message counts as already delivered only once it has left both the
		// causal buffer and the total-order pending set.
		deliveredSet := map[MsgID]bool{}
		for id := range m.appByID {
			_, pending := m.headPending[id]
			_, buffered := m.causalBuf[id]
			if !pending && !buffered {
				deliveredSet[id] = true
			}
		}
		// Authoritative order, ties already broken deterministically by the
		// coordinator's topological construction.
		order := append([]FinalOrderEntry(nil), fc.FinalOrder...)
		sort.SliceStable(order, func(i, j int) bool {
			if order[i].FinalNo != order[j].FinalNo {
				return order[i].FinalNo < order[j].FinalNo
			}
			return order[i].ID.Origin < order[j].ID.Origin
		})
		for _, e := range order {
			if deliveredSet[e.ID] {
				continue
			}
			app, ok := appIndex[e.ID]
			if !ok {
				continue
			}
			// Whether this message was missing entirely, parked in the causal
			// buffer, or held in the total-order pending set, the flush closure
			// is authoritative: advance the vector clock, clear every hold-back
			// queue, and record a causal delivery so the UI timeline stays
			// consistent. The topological FinalOrder guarantees every dependency
			// is processed earlier in this same loop.
			if _, known := m.appByID[e.ID]; !known {
				m.appByID[e.ID] = app
			}
			delete(m.causalBuf, e.ID)
			app.VC.CopyInto(m.clock)
			m.clock[app.ID.Origin] = app.ID.Seq
			m.rec.Record(m.now(), m.ID, EvCausalDeliver, &app.ID, fc.OldView,
				"flush-replay vc="+vcText(app.VC), app.VC.Clone())
			delete(m.headPending, e.ID)
			delete(m.waitingLog, e.ID)
			m.finals[e.ID] = e.FinalNo
			m.rec.Record(m.now(), m.ID, EvTotalDeliver, &e.ID, fc.OldView,
				fmt.Sprintf("flush-replay final=%d", e.FinalNo), nil)
			m.app.OnDeliver(m.ID, fc.OldView, app, m.clock.Clone(), e.FinalNo)
		}
	}

	// 3. Install the new view with a fully fresh per-view protocol state.
	//    Vector clocks, FIFO counters and total-order counters are scoped to a
	//    view: old-view messages were all reconciled above, so nothing needs
	//    to carry over. Survivors and joiners alike start clean, which is also
	//    what guarantees a returning member cannot reuse stale state (item 7).
	m.view = fc.NewView
	m.members = append([]int(nil), fc.Members...)
	sort.Ints(m.members)
	m.phase = PhaseNormal
	m.viewInstalledAt = m.now()
	m.heartbeats = map[int]int64{}
	for _, x := range m.members {
		m.heartbeats[x] = m.now()
	}
	m.recvExpect = map[int]int64{}
	m.announcedHigh = map[int]int64{}
	m.cache = map[int]map[int64]Envelope{}
	m.causalBuf = map[MsgID]AppData{}
	m.provIssued = map[MsgID]int64{}
	m.headPending = map[MsgID]bool{}
	m.finals = map[MsgID]int64{}
	m.acks = map[MsgID]map[int]Provisional{}
	m.waitingLog = map[MsgID]bool{}
	m.flushStates = map[int]FlushState{}
	m.flushFetched = map[MsgID]bool{}
	m.flushSeenStart = map[int]bool{}
	m.provCounter = 0
	m.clock = VC{}
	m.appByID = map[MsgID]AppData{}
	m.envsByMsg = map[MsgID]Envelope{}
	m.ownSends = map[MsgID]bool{}
	m.sendSeq = 0
	m.appSeq = 0
	m.rec.Record(m.now(), m.ID, EvViewChange, nil, m.view,
		fmt.Sprintf("view %d installed, members=%v", m.view, m.members), nil)
}
