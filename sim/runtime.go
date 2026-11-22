// Package sim assembles the protocol members, the fault-injecting network and
// a scripted scenario into one deterministic run.
package sim

import (
	"fmt"
	"sort"

	"kitecomm/proto"
)

// ScenarioEvent is one scripted fault or workload action at a virtual time.
type ScenarioEvent struct {
	At    int64
	Kind  string // see the Kind* constants below
	Member int   // target member (or -1 for global events)
	Member2 int  // second target (partition pair), -1 when unused
	Value  float64
	Text   string
}

const (
	EvCrash        = "crash"
	EvRejoin       = "rejoin"        // crashed member comes back as a NEW id
	EvPartition    = "partition"     // isolate Member from everyone
	EvHeal         = "heal"          // clear partition on Member
	EvGlobalLoss   = "global_loss"   // Value = cluster-wide extra loss
	EvLinkLoss     = "link_loss"     // Member->Member2 extra loss = Value
	EvLinkClear    = "link_clear"
	EvAppSend      = "app_send"      // Member broadcasts Text
	EvStop         = "stop"          // end the run at At
)

// Scenario is a fully self-contained test case.
type Scenario struct {
	Name       string
	Seed       int64
	Members    int // initial group size
	BaseLoss   float64
	MinDelay   int64
	MaxDelay   int64
	Events     []ScenarioEvent
	AppTraffic []AppTrafficSpec // periodic background broadcasts
}

// AppTrafficSpec makes a member broadcast every Period ticks, Count times.
type AppTrafficSpec struct {
	Member int
	Period int64
	Count  int
}

// AppRecord is one application-visible outcome.
type AppRecord struct {
	Time    int64
	Member  int
	View    int64
	Msg     proto.MsgID
	Content string
	VC      proto.VC
	FinalNo int64
}

// VoidRecord is a void decision observed through the runtime.
type VoidRecord struct {
	Time int64
	Msg  proto.MsgID
	View int64
}

// CrashEvent records a member incarnation going down (for the verifier's
// survivor-set logic).
type CrashEvent struct {
	Time int64
	ID   int
}

// Runtime implements proto.Transport, proto.SendsLister and proto.VoidNotifier.
type Runtime struct {
	sched    *Scheduler
	net      *Network
	sc       Scenario
	rec      *proto.Recorder
	members  map[int]*proto.Member // live incarnations only
	allEver  map[int]bool          // every id ever allocated
	nextID   int
	sendLog  map[proto.MsgID]int64 // msg id -> send time (for the verifier)
	sendView map[proto.MsgID]int64 // msg id -> view it was sent in
	sendsInV map[int64][]proto.MsgID
	delivers []AppRecord
	voids    []VoidRecord
	denied   []AppRecord
	crashes  []CrashEvent
	stopAt   int64
	sentScanned int
	// quietTarget: virtual time by which all scripted events have fired. The
	// run then keeps going until every member's hold-back queues are empty,
	// capped at quietDeadline, so loss-driven NACK repair gets time to
	// converge before the verifier inspects state.
	quietTarget   int64
	quietDeadline int64
}

// NewRuntime builds everything but does not start the clock.
func NewRuntime(sc Scenario) *Runtime {
	sched := NewScheduler()
	net := NewNetwork(sched, sc.Seed)
	rt := &Runtime{
		sched:    sched,
		net:      net,
		sc:       sc,
		rec:      proto.NewRecorder(),
		members:  map[int]*proto.Member{},
		allEver:  map[int]bool{},
		nextID:   1,
		sendLog:  map[proto.MsgID]int64{},
		sendView: map[proto.MsgID]int64{},
		sendsInV: map[int64][]proto.MsgID{},
	}
	// Bootstrap the initial view with members numbered 1..N.
	ids := make([]int, sc.Members)
	for i := range ids {
		ids[i] = i + 1
		rt.nextID = i + 2
		rt.allEver[ids[i]] = true
	}
	for _, id := range ids {
		m := proto.NewMember(id, 1, ids, rt, rt, rt.rec,
			func() int64 { return sched.Now() },
			func(delay int64, fn func()) { sched.Schedule(delay, fn) })
		rt.members[id] = m
		net.SetAlive(id, true)
	}
	for _, id := range ids {
		rt.members[id].Start()
	}
	rt.configureLinks()
	rt.scheduleTraffic()
	rt.scheduleEvents()
	// Convergence watchdog: after the last scripted event, keep pumping until
	// all live members have empty total/causal hold-back queues (or cap).
	rt.quietTarget = rt.lastScriptedTime()
	rt.quietDeadline = rt.quietTarget + 400
	rt.sched.Schedule(rt.quietTarget+1, rt.quietWatchdog)
	return rt
}

func (rt *Runtime) lastScriptedTime() int64 {
	var last int64
	for _, ev := range rt.sc.Events {
		if ev.At > last {
			last = ev.At
		}
	}
	// Background traffic can run past explicit events.
	for _, t := range rt.sc.AppTraffic {
		if end := 3 + int64(t.Count-1)*t.Period; end > last {
			last = end
		}
	}
	return last
}

// quietWatchdog re-arms until every live member's queues drain or the cap is
// reached. A member that is mid-flush is allowed to finish its view change.
func (rt *Runtime) quietWatchdog() {
	if rt.sched.Now() >= rt.quietDeadline {
		rt.sched.Stop()
		return
	}
	drained := true
	for _, m := range rt.members {
		if len(m.TotalPendingSnapshot()) > 0 || len(m.CausalBufferSnapshot()) > 0 {
			drained = false
		}
		if m.Phase() != proto.PhaseNormal {
			drained = false
		}
	}
	if drained {
		// Give in-flight packets one more generous window to land and repair.
		rt.sched.Schedule(30, func() { rt.sched.Stop() })
		return
	}
	rt.sched.Schedule(8, rt.quietWatchdog)
}

func (rt *Runtime) configureLinks() {
	for from := range rt.allEver {
		for to := range rt.allEver {
			if from == to {
				continue
			}
			rt.net.SetLink(from, to, rt.sc.BaseLoss, rt.sc.MinDelay, rt.sc.MaxDelay)
		}
	}
}

func (rt *Runtime) scheduleTraffic() {
	for _, t := range rt.sc.AppTraffic {
		for i := 0; i < t.Count; i++ {
			idx := i
			rt.sched.Schedule(3+int64(idx)*t.Period, func() {
				if m, ok := rt.members[t.Member]; ok {
					m.Broadcast(fmt.Sprintf("m%d-t%d", t.Member, idx))
				}
			})
		}
	}
}

func (rt *Runtime) scheduleEvents() {
	for _, ev := range rt.sc.Events {
		e := ev
		rt.sched.Schedule(e.At, func() { rt.applyEvent(e) })
		if e.Kind == EvStop {
			rt.stopAt = e.At
		}
	}
}

func (rt *Runtime) applyEvent(ev ScenarioEvent) {
	switch ev.Kind {
	case EvCrash:
		rt.crash(ev.Member)
	case EvRejoin:
		rt.rejoin(ev.Member)
	case EvPartition:
		rt.net.SetPartitioned(ev.Member, true)
	case EvHeal:
		rt.net.SetPartitioned(ev.Member, false)
	case EvGlobalLoss:
		rt.net.SetGlobalLoss(ev.Value)
	case EvLinkLoss:
		l := rt.net.Link(ev.Member, ev.Member2)
		l.ExtraLoss = ev.Value
	case EvLinkClear:
		l := rt.net.Link(ev.Member, ev.Member2)
		l.ExtraLoss = 0
	case EvAppSend:
		if m, ok := rt.members[ev.Member]; ok {
			m.Broadcast(ev.Text)
		}
	case EvStop:
		// Explicit stop is advisory; the quiet watchdog owns shutdown so loss
		// recovery always gets its convergence window.
	}
}

func (rt *Runtime) crash(id int) {
	m, ok := rt.members[id]
	if !ok {
		return
	}
	rt.rec.Record(rt.sched.Now(), id, proto.EvCrash, nil, m.View(), "crashed", nil)
	rt.crashes = append(rt.crashes, CrashEvent{Time: rt.sched.Now(), ID: id})
	delete(rt.members, id)
	rt.net.SetAlive(id, false)
}

// rejoin creates a brand-new incarnation with a fresh id (requirement 7:
// returning members cannot keep old state). The returned id is also logged.
func (rt *Runtime) rejoin(oldID int) int {
	if rt.net.IsAlive(oldID) {
		return -1 // old incarnation still up
	}
	newID := rt.nextID
	rt.nextID++
	rt.allEver[newID] = true
	// Links from/to the new id.
	for id := range rt.allEver {
		if id != newID {
			rt.net.SetLink(newID, id, rt.sc.BaseLoss, rt.sc.MinDelay, rt.sc.MaxDelay)
			rt.net.SetLink(id, newID, rt.sc.BaseLoss, rt.sc.MinDelay, rt.sc.MaxDelay)
		}
	}
	candidates := make([]int, 0, len(rt.members))
	for id := range rt.members {
		candidates = append(candidates, id)
	}
	sort.Ints(candidates)
	m := proto.NewMember(newID, 0, nil, rt, rt, rt.rec,
		func() int64 { return rt.sched.Now() },
		func(delay int64, fn func()) { rt.sched.Schedule(delay, fn) })
	m.SetJoinCandidates(candidates)
	m.InitJoining()
	rt.members[newID] = m
	rt.net.SetAlive(newID, true)
	rt.rec.Record(rt.sched.Now(), newID, proto.EvJoin, nil, 0,
		fmt.Sprintf("re-join of crashed m%d as fresh member", oldID), nil)
	return newID
}

// ---- Transport interface ----

func (rt *Runtime) Send(from, to int, deliver func(pkt any), pkt any) {
	rt.net.Send(from, to, func(p Packet) { deliver(p) }, pkt)
}

func (rt *Runtime) IsAlive(id int) bool { return rt.net.IsAlive(id) }

// MemberByID resolves a live incarnation so protocol send closures can invoke
// the destination's ingress in the destination's event turn.
func (rt *Runtime) MemberByID(id int) *proto.Member { return rt.members[id] }

// ---- SendsLister ----

func (rt *Runtime) SendsInView(view int64) []proto.MsgID {
	return append([]proto.MsgID(nil), rt.sendsInV[view]...)
}

// ---- VoidNotifier ----

func (rt *Runtime) NotifyVoid(msg proto.MsgID, oldView int64) {
	// Dedupe: every surviving member's install invokes this for the same set.
	for _, v := range rt.voids {
		if v.Msg == msg && v.View == oldView {
			return
		}
	}
	rt.voids = append(rt.voids, VoidRecord{Time: rt.sched.Now(), Msg: msg, View: oldView})
}

// ---- AppCallbacks ----

func (rt *Runtime) OnDeliver(member int, view int64, msg proto.AppData, vc proto.VC, finalNo int64) {
	rt.delivers = append(rt.delivers, AppRecord{
		Time: rt.sched.Now(), Member: member, View: view, Msg: msg.ID,
		Content: msg.Content, VC: vc.Clone(), FinalNo: finalNo,
	})
	if _, logged := rt.sendLog[msg.ID]; !logged {
		// Sender-side logging happens in noteSend; deliveries of messages
		// whose sender crashed still appear here, but sendLog is filled at
		// broadcast time below.
	}
}

func (rt *Runtime) OnVoid(member int, msg proto.MsgID, view int64) {
	// Runtime-wide dedupe happens in NotifyVoid; nothing extra needed.
}

func (rt *Runtime) OnBroadcastRejected(member int, content string) {
	rt.denied = append(rt.denied, AppRecord{Time: rt.sched.Now(), Member: member, Content: content})
}

// NoteSend is called by the test driver's wrapper to record sends. The
// protocol's Broadcast goes through members directly, so the runtime hooks
// send recording via the event trace instead (see Run/SendLog builder).

// Run executes the scenario. The send log is maintained incrementally by a
// lightweight watchdog that scans new recorder events, because the flush
// protocol needs SendsInView while the run is still in progress (it cannot
// wait for post-run reconstruction).
func (rt *Runtime) Run() {
	rt.sched.Schedule(1, rt.sendLogWatchdog)
	rt.sched.Run()
	rt.buildSendLog()
}

// sendLogWatchdog incrementally mirrors EvSend events into the send logs so
// the coordinator can query sends-in-view at any flush.
func (rt *Runtime) sendLogWatchdog() {
	for rt.sentScanned < len(rt.rec.Events) {
		ev := rt.rec.Events[rt.sentScanned]
		rt.sentScanned++
		if ev.Kind != proto.EvSend || ev.Msg == nil {
			continue
		}
		id := *ev.Msg
		if _, ok := rt.sendLog[id]; ok {
			continue
		}
		rt.sendLog[id] = ev.Time
		rt.sendView[id] = ev.View
		rt.sendsInV[ev.View] = append(rt.sendsInV[ev.View], id)
	}
	if !rt.sched.IsStopped() {
		rt.sched.Schedule(3, rt.sendLogWatchdog)
	}
}

func (rt *Runtime) buildSendLog() {
	for _, ev := range rt.rec.Events {
		if ev.Kind != proto.EvSend || ev.Msg == nil {
			continue
		}
		id := *ev.Msg
		if _, ok := rt.sendLog[id]; ok {
			continue
		}
		rt.sendLog[id] = ev.Time
		rt.sendView[id] = ev.View
		rt.sendsInV[ev.View] = append(rt.sendsInV[ev.View], id)
	}
}

// Result accessors used by verifier and web layers.
func (rt *Runtime) Recorder() *proto.Recorder     { return rt.rec }
func (rt *Runtime) Delivers() []AppRecord          { return rt.delivers }
func (rt *Runtime) Voids() []VoidRecord            { return rt.voids }
func (rt *Runtime) Denied() []AppRecord            { return rt.denied }
func (rt *Runtime) EndTime() int64                 { return rt.sched.Now() }
func (rt *Runtime) Members() map[int]*proto.Member  { return rt.members }
func (rt *Runtime) SendLog() map[proto.MsgID]int64  { return rt.sendLog }
func (rt *Runtime) SendView() map[proto.MsgID]int64 { return rt.sendView }
func (rt *Runtime) Scenario() Scenario              { return rt.sc }
func (rt *Runtime) Crashes() []CrashEvent           { return rt.crashes }
