package proto

import (
	"fmt"
	"sort"
)

// Transport abstracts the fault-injecting fabric (sim.Network in production,
// a fake in unit tests).
type Transport interface {
	Send(from, to int, deliver func(pkt any), pkt any)
	IsAlive(id int) bool
}

// AppCallbacks lets the protocol notify the program that drives it.
type AppCallbacks interface {
	// OnDeliver is a total-order application delivery.
	OnDeliver(member int, view int64, msg AppData, vc VC, finalNo int64)
	// OnVoid reports a message this member sent that was declared void during
	// a flush because no surviving member ever received it.
	OnVoid(member int, msg MsgID, view int64)
	// OnBroadcastRejected tells the app its send could not be accepted because
	// the member is in a flush.
	OnBroadcastRejected(member int, content string)
}

// Phase of the view-change state machine.
type Phase int

const (
	PhaseNormal Phase = iota
	PhaseJoining      // incarnation up, waiting to be installed
	PhaseFlushing     // flush of the old view in progress
)

// Timer period constants (virtual ticks). Heartbeats are unreliable direct
// packets, and the suspect timeout spans many heartbeat rounds so that even
// heavy loss cannot produce a false suspicion.
const (
	HeartbeatPeriod = 5
	NackPeriod      = 7
	FlushTimer      = 12
	SuspectTimeout  = 90 // 18 missed heartbeat rounds; survives 50% loss
)

// Member is one group member / protocol stack instance.
type Member struct {
	ID      int
	rec     *Recorder
	tr      Transport
	app     AppCallbacks
	now     func() int64
	sched   func(delay int64, fn func()) // timer registration

	// ---- view / membership ----
	view      int64
	members   []int // sorted, current installed view
	phase     Phase
	heartbeats map[int]int64 // last heartbeat time per member (current view)
	nextViewID int64

	// ---- FIFO reliable broadcast ----
	sendSeq    int64                  // next FIFO seq (covers every envelope type)
	appSeq     int64                  // app-only counter: feeds MsgID and the VC
	recvExpect   map[int]int64          // next FIFO seq expected per origin
	announcedHigh map[int]int64         // highest FIFO seq advertised per origin
	cache        map[int]map[int64]Envelope // origin -> seq -> envelope (repair store)
	// app messages sent by this member, indexed by id, for void callbacks
	ownSends map[MsgID]bool

	// ---- causal layer ----
	clock      VC                 // delivered vector clock
	causalBuf  map[MsgID]AppData  // held app messages waiting on dependencies
	appByID    map[MsgID]AppData  // every app message causally delivered (for flush fetch)
	envsByMsg  map[MsgID]Envelope // envelope that carried each app message

	// ---- total order layer ----
	provCounter int64                       // local monotonically increasing provisional counter
	provIssued  map[MsgID]int64             // provisional seq I issued per message
	headPending map[MsgID]bool              // messages causally in but no final yet
	finals      map[MsgID]int64             // final seq known per message
	acks        map[MsgID]map[int]Provisional // ack sets collected by the origin
	waitingLog  map[MsgID]bool              // total_wait already logged (dedupe UI)

	// ---- view change / flush ----
	flushCoord      int
	flushSurvivors  []int
	flushJoiners    []int
	flushStates     map[int]FlushState
	flushUnion      map[MsgID]Envelope // coordinator's authoritative union for the active flush
	activeFC        *FlushComplete     // coordinator's built closure, retained for retransmit
	installAcks     map[int]bool       // members that applied the active FlushComplete
	stateAcked      bool               // survivor: coordinator confirmed our FlushState
	completeWaits   int                // survivor: ticks spent waiting for FlushComplete
	flushFetched    map[MsgID]bool
	flushSeenStart  map[int]bool // members whose FlushStart we know received ours
	flushCoordHeard int64        // last time the coordinator's flush traffic was seen
	viewInstalledAt int64        // when the current view was installed (grace)
	completedOld    map[int64]bool
	lastHbView      int64
	joinTargetView  int64
	joinCand        []int // candidate live members, used by joiners
}

// SetJoinCandidates gives a joining incarnation the addresses to ask.
func (m *Member) SetJoinCandidates(ids []int) { m.joinCand = append([]int(nil), ids...) }

func (m *Member) joinCandidates() []int { return append([]int(nil), m.joinCand...) }

// NewMember builds an un-started member. startView/members install the first
// view without a flush (the group bootstrap).
func NewMember(id int, view int64, members []int, tr Transport, app AppCallbacks, rec *Recorder,
	now func() int64, sched func(delay int64, fn func())) *Member {
	m := &Member{
		ID:          id,
		rec:         rec,
		tr:          tr,
		app:         app,
		now:         now,
		sched:       sched,
		view:        view,
		members:     append([]int(nil), members...),
		phase:       PhaseNormal,
		heartbeats:  map[int]int64{},
		nextViewID:  view + 1,
		recvExpect:  map[int]int64{},
		announcedHigh: map[int]int64{},
		cache:       map[int]map[int64]Envelope{},
		ownSends:    map[MsgID]bool{},
		clock:       VC{},
		causalBuf:   map[MsgID]AppData{},
		appByID:     map[MsgID]AppData{},
		envsByMsg:   map[MsgID]Envelope{},
		provIssued:  map[MsgID]int64{},
		headPending: map[MsgID]bool{},
		finals:      map[MsgID]int64{},
		acks:        map[MsgID]map[int]Provisional{},
		waitingLog:  map[MsgID]bool{},
		flushStates: map[int]FlushState{},
		flushFetched: map[MsgID]bool{},
		flushSeenStart: map[int]bool{},
		completedOld: map[int64]bool{},
	}
	for _, x := range members {
		m.heartbeats[x] = now()
	}
	m.viewInstalledAt = now()
	return m
}

// Start registers the periodic timers.
func (m *Member) Start() {
	m.sched(HeartbeatPeriod, m.timerHeartbeat)
	m.sched(NackPeriod, m.timerNackScan)
	m.sched(HeartbeatPeriod, m.timerSuspectScan)
}

// View accessors used by the simulator / web layer.
func (m *Member) View() int64   { return m.view }
func (m *Member) Phase() Phase  { return m.phase }
func (m *Member) Members() []int { return append([]int(nil), m.members...) }
func (m *Member) Clock() VC     { return m.clock.Clone() }

// CausalBufferSnapshot returns the messages currently held by the causal layer.
func (m *Member) CausalBufferSnapshot() []AppData {
	out := make([]AppData, 0, len(m.causalBuf))
	for _, a := range m.causalBuf {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID.Origin != out[j].ID.Origin {
			return out[i].ID.Origin < out[j].ID.Origin
		}
		return out[i].ID.Seq < out[j].ID.Seq
	})
	return out
}

// TotalPendingSnapshot returns messages known causally but not yet
// total-order delivered, with provisional/final status for the UI.
type TotalPending struct {
	Msg        MsgID
	Provisional int64 // 0 if not yet issued by this member
	Final      int64 // 0 if unknown
	HeadBlocked bool  // true if this is the head-of-line holding up delivery
}

// TotalPendingSnapshot exposes the total-order hold-back state.
func (m *Member) TotalPendingSnapshot() []TotalPending {
	out := make([]TotalPending, 0, len(m.headPending))
	var head MsgID
	var headProv int64 = 1<<62
	for id := range m.headPending {
		p := m.provIssued[id]
		if p > 0 && p < headProv {
			headProv, head = p, id
		}
		out = append(out, TotalPending{Msg: id, Provisional: p, Final: m.finals[id]})
	}
	for i := range out {
		if out[i].Msg == head {
			out[i].HeadBlocked = true
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Provisional != out[j].Provisional {
			return out[i].Provisional < out[j].Provisional
		}
		return out[i].Msg.Origin < out[j].Msg.Origin
	})
	return out
}

// Broadcast is the application entry point. Rejected (flush in progress)
// broadcasts are reported back through OnBroadcastRejected and dropped.
func (m *Member) Broadcast(content string) {
	if m.phase != PhaseNormal {
		m.app.OnBroadcastRejected(m.ID, content)
		m.rec.Record(m.now(), m.ID, EvBroadcastDeny, nil, m.view, content, nil)
		return
	}
	m.sendSeq++ // FIFO seq covers every envelope type
	m.appSeq++  // vector clocks and MsgIDs count application messages only
	id := MsgID{Origin: m.ID, Seq: m.appSeq}
	vc := m.clock.Clone()
	vc[m.ID] = m.appSeq
	app := AppData{ID: id, VC: vc, Content: content}
	env := Envelope{View: m.view, Origin: m.ID, FIFOSeq: m.sendSeq, Payload: &app}
	m.ownSends[id] = true
	m.rec.Record(m.now(), m.ID, EvSend, &id, m.view, content, vc.Clone())
	m.broadcastEnv(env)
	// Local loopback uses the same FIFO ingress as any remote arrival. The
	// envelope enters the cache there, so recvExpect advances exactly once.
	m.HandleEnvelope(env)
}

func (m *Member) cacheFor(origin int) map[int64]Envelope {
	c, ok := m.cache[origin]
	if !ok {
		c = map[int64]Envelope{}
		m.cache[origin] = c
	}
	return c
}

// broadcastEnv sends an envelope over the reliable channel to every member of
// the current view (the sender's own copy is looped back separately via
// HandleEnvelope).
func (m *Member) broadcastEnv(env Envelope) {
	for _, to := range m.members {
		if to == m.ID {
			continue
		}
		m.sendEnvelope(to, env)
	}
}

func (m *Member) sendEnvelope(to int, env Envelope) {
	e := env
	// The closure must invoke the *destination's* ingress. The runtime
	// delivers the packet to `to`, and the transport invokes this callback in
	// that member's event turn.
	m.tr.Send(m.ID, to, func(pkt any) {
		dest := m.lookupMember(to)
		if dest != nil {
			dest.HandleEnvelope(pkt.(Envelope))
		}
	}, e)
}

// MemberLookup is implemented by runtimes that can resolve a destination
// member for network delivery callbacks.
type MemberLookup interface {
	MemberByID(id int) *Member
}

func (m *Member) lookupMember(id int) *Member {
	if id == m.ID {
		return m
	}
	if lookup, ok := m.tr.(MemberLookup); ok {
		return lookup.MemberByID(id)
	}
	return nil
}

// HandleEnvelope is the reliable-channel ingress (initial sends and repairs).
func (m *Member) HandleEnvelope(env Envelope) {
	if env.View != m.view || m.phase == PhaseJoining {
		return
	}
	origin := env.Origin
	cache := m.cacheFor(origin)
	if _, dup := cache[env.FIFOSeq]; dup {
		// Duplicate delivery: keep the first, no reprocessing.
		return
	}
	cache[env.FIFOSeq] = env
	m.tryFifoPump(origin)
}

// tryFifoPump releases everything contiguous from the expected seq onwards.
func (m *Member) tryFifoPump(origin int) {
	cache := m.cacheFor(origin)
	next := m.recvExpect[origin] // zero value 0 means "expect seq 1"
	if next == 0 {
		next = 1
	}
	for {
		env, ok := cache[next]
		if !ok {
			break
		}
		m.recvExpect[origin] = next + 1
		m.rec.Record(m.now(), m.ID, EvReliableRecv, msgIDOf(env), m.view,
			fmt.Sprintf("fifo seq=%d from=%d", next, origin), nil)
		m.fifoDeliver(env)
		next++
	}
}

func msgIDOf(env Envelope) *MsgID {
	switch p := env.Payload.(type) {
	case *AppData:
		id := p.ID
		return &id
	case *Provisional:
		id := p.For
		return &id
	case *Final:
		id := p.For
		return &id
	}
	return nil
}

// fifoDeliver hands an in-order envelope to the layer above.
func (m *Member) fifoDeliver(env Envelope) {
	switch p := env.Payload.(type) {
	case *AppData:
		m.envsByMsg[p.ID] = env
		m.causalReceive(*p)
	case *Provisional:
		m.totalReceiveProvisional(*p)
	case *Final:
		m.totalReceiveFinal(*p)
	case *HB:
		m.noteHeartbeat(p.From, p.View)
	}
}

// noteHeartbeat refreshes liveness for a heartbeat that arrived over the
// reliable channel.
func (m *Member) noteHeartbeat(from int, view int64) {
	if view == m.view {
		m.heartbeats[from] = m.now()
	}
}

// ---- NACK machinery ----

func (m *Member) timerNackScan() {
	defer m.sched(NackPeriod, m.timerNackScan)
	if m.phase == PhaseJoining {
		return
	}
	// Advertise our own highest FIFO sequence so peers can detect tail loss.
	if m.phase == PhaseNormal && m.sendSeq > 0 {
		ann := FIFOAnnounce{View: m.view, Origin: m.ID, HighSeq: m.sendSeq}
		for _, to := range m.members {
			if to != m.ID {
				m.sendDirect(to, &ann)
			}
		}
	}
	// Ask each origin for gaps between the next contiguous seq we expect and
	// the highest seq we have any evidence of (cached envelope or announce).
	for _, origin := range m.members {
		cache := m.cacheFor(origin)
		next := m.recvExpect[origin]
		if next == 0 {
			next = 1
		}
		high := m.announcedHigh[origin]
		for seq := range cache {
			if seq > high {
				high = seq
			}
		}
		if high >= next {
			nack := Nack{View: m.view, Origin: origin, From: m.ID, Begin: next, End: high}
			m.rec.Record(m.now(), m.ID, EvNackSent, nil, m.view,
				fmt.Sprintf("request %d..%d from %d", next, high, origin), nil)
			m.sendDirect(origin, &nack)
		}
	}
}

// HandleDirect is the ingress for point-to-point control packets.
func (m *Member) HandleDirect(pkt any) {
	switch p := pkt.(type) {
	case *Nack:
		m.handleNack(*p)
	case *Repair:
		m.handleRepair(*p)
	case *Heartbeat:
		m.handleHeartbeat(*p)
	case *JoinReq:
		m.handleJoinReq(*p)
	case *FlushStart:
		m.handleFlushStart(*p)
	case *FlushState:
		m.handleFlushState(*p)
	case *FlushComplete:
		m.handleFlushComplete(*p)
	case *InstallAck:
		m.handleInstallAck(*p)
	case *FlushStateReq:
		if m.phase == PhaseFlushing && m.flushCoord == p.From {
			m.stateAcked = false
			m.sendFlushState()
		}
	case *CompleteReq:
		// Re-push a retained closure to a member still waiting. The coordinator
		// keeps activeFC after installing its own view so late requests from a
		// member whose Complete packet was dropped still get an answer.
		if m.activeFC != nil && p.NewView == m.activeFC.NewView &&
			m.flushCoord == m.ID {
			m.sendDirect(p.From, m.activeFC)
		}
	case *FIFOAnnounce:
		m.handleAnnounce(*p)
	default:
		panic(fmt.Sprintf("unknown direct packet %T", p))
	}
}

func (m *Member) handleNack(n Nack) {
	if n.View != m.view || m.phase == PhaseJoining {
		return
	}
	cache := m.cacheFor(n.Origin)
	for seq := n.Begin; seq <= n.End; seq++ {
		if env, ok := cache[seq]; ok {
			m.rec.Record(m.now(), m.ID, EvRepairSent, msgIDOf(env), m.view,
				fmt.Sprintf("re-send seq=%d to %d", seq, n.From), nil)
			m.sendDirect(n.From, &Repair{Env: env})
		}
	}
}

func (m *Member) handleRepair(r Repair) {
	if r.Env.View != m.view || m.phase == PhaseJoining {
		return
	}
	m.rec.Record(m.now(), m.ID, EvRepairRecv, msgIDOf(r.Env), m.view, "", nil)
	m.HandleEnvelope(r.Env)
}

// announcedHigh tracks the highest FIFO seq each origin has advertised; it
// lets the NACK scan cover tail gaps it could not otherwise observe.
// (Field lives here to keep transport state together.)

func (m *Member) handleAnnounce(a FIFOAnnounce) {
	if a.View != m.view || m.phase == PhaseJoining {
		return
	}
	if a.HighSeq > m.announcedHigh[a.Origin] {
		m.announcedHigh[a.Origin] = a.HighSeq
		// Immediate gap check: the periodic scan would catch it, but reacting
		// here shaves a round trip on tail loss.
		next := m.recvExpect[a.Origin]
		if next == 0 {
			next = 1
		}
		if a.HighSeq >= next {
			nack := Nack{View: m.view, Origin: a.Origin, From: m.ID, Begin: next, End: a.HighSeq}
			m.rec.Record(m.now(), m.ID, EvNackSent, nil, m.view,
				fmt.Sprintf("announce-driven request %d..%d from %d", next, a.HighSeq, a.Origin), nil)
			m.sendDirect(a.Origin, &nack)
		}
	}
}

func (m *Member) sendDirect(to int, pkt Direct) {
	m.tr.Send(m.ID, to, func(p any) {
		dest := m.lookupMember(to)
		if dest != nil {
			dest.HandleDirect(p)
		}
	}, pkt)
}
