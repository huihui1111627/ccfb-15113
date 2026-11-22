package proto

// EventKind enumerates the observable events in a member's lifecycle.
type EventKind string

const (
	EvSend           EventKind = "send"             // app broadcast accepted
	EvReliableRecv   EventKind = "reliable_recv"    // FIFO layer accepted envelope
	EvNackSent       EventKind = "nack_sent"
	EvRepairSent     EventKind = "repair_sent"
	EvRepairRecv     EventKind = "repair_recv"
	EvCausalBuffer   EventKind = "causal_buffer"    // parked waiting for dependencies
	EvCausalReady    EventKind = "causal_ready"     // released from the buffer
	EvCausalDeliver  EventKind = "causal_deliver"
	EvProvSent       EventKind = "prov_sent"
	EvProvRecv       EventKind = "prov_recv"
	EvFinalSent      EventKind = "final_sent"
	EvFinalRecv      EventKind = "final_recv"
	EvTotalWait      EventKind = "total_wait"       // blocked behind head-of-line
	EvTotalDeliver   EventKind = "total_deliver"
	EvViewChange     EventKind = "view_change"      // view change process steps
	EvVoid           EventKind = "void"
	EvBroadcastDeny  EventKind = "broadcast_deny"   // app tried to send during flush
	EvCrash          EventKind = "crash"
	EvJoin           EventKind = "join"
)

// Event is one recorded, time-stamped occurrence.
type Event struct {
	Time   int64
	Member int
	Kind   EventKind
	Msg    *MsgID  // associated application message, when applicable
	Detail string  // human-readable detail
	VC     VC      // snapshot of relevant clock, when applicable
	View   int64
}

// Recorder stores the full event trace of a run, in append order.
type Recorder struct {
	Events []Event
}

// NewRecorder creates an empty trace.
func NewRecorder() *Recorder { return &Recorder{} }

func (r *Recorder) add(ev Event) { r.Events = append(r.Events, ev) }

// Record is called by members; nil recorder is tolerated so unit tests can
// run without one.
func (r *Recorder) Record(t int64, member int, kind EventKind, msg *MsgID, view int64, detail string, vc VC) {
	if r == nil {
		return
	}
	r.add(Event{Time: t, Member: member, Kind: kind, Msg: msg, View: view, Detail: detail, VC: vc})
}
