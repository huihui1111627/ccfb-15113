// Package proto implements the group communication stack:
//
//	FIFO reliable broadcast (receiver-driven NACK repair)
//	  -> causal broadcast (vector timestamps + hold-back buffer)
//	    -> total order (two-round provisional/final sequence numbers)
//	      -> view changes with flush
//
// Every member is an event-driven actor driven by a sim.Scheduler; no member
// goroutine ever blocks, which keeps whole runs deterministic.
package proto

// MsgID uniquely identifies one application broadcast for all time, across
// views and across re-joins (a re-joined process gets a fresh member id, so
// ids never collide).
type MsgID struct {
	Origin int // member id of the original sender
	Seq    int64 // sender-local sequence number
}

// VC is a vector clock. Maps stay sparse: absent entries read as zero.
type VC map[int]int64

// Clone returns an independent copy.
func (v VC) Clone() VC {
	c := make(VC, len(v))
	for k, x := range v {
		c[k] = x
	}
	return c
}

// CopyInto merges v into dst taking component-wise maxima.
func (v VC) CopyInto(dst VC) {
	for k, x := range v {
		if x > dst[k] {
			dst[k] = x
		}
	}
}

// Equal reports component-wise equality.
func (v VC) Equal(o VC) bool {
	for k, x := range v {
		if o[k] != x {
			return false
		}
	}
	for k, x := range o {
		if v[k] != x {
			return false
		}
	}
	return false
}

// HappensBefore reports v < o (v <= o component-wise and !=).
func (v VC) HappensBefore(o VC) bool {
	strict := false
	for k, x := range v {
		if x > o[k] {
			return false
		}
		if x < o[k] {
			strict = true
		}
	}
	if !strict {
		for k, x := range o {
			if v[k] < x {
				strict = true
				break
			}
		}
	}
	return strict
}

// Payload kinds carried inside Envelope (the reliable broadcast channel).
type Payload interface{ payloadTag() }

// AppData is an application broadcast on its way through the stack.
type AppData struct {
	ID      MsgID
	VC      VC // sender's vector clock at send time
	Content string
}

// Provisional is round 1 of the total-order protocol: a member's tentative
// sequence number for one application message.
type Provisional struct {
	For  MsgID
	From int   // member issuing the provisional number
	Seq  int64 // provisional sequence number
	VC   VC    // causal clock of the issuer when it causally received For
}

// Final is round 2: the sender's chosen final order for its message
// (maximum of all provisional numbers).
type Final struct {
	For     MsgID
	FinalNo int64
}

// HB is a liveness heartbeat carried on the reliable broadcast channel, so
// dropped heartbeats are repaired by NACK instead of causing false suspicions.
type HB struct {
	From int
	View int64
}

func (*AppData) payloadTag()     {}
func (*Provisional) payloadTag() {}
func (*Final) payloadTag()       {}
func (*HB) payloadTag()          {}

// Envelope is the reliable broadcast unit. FIFO order is defined by
// (Origin, FIFOSeq); the same envelope is re-sent both initially and as a
// repair for a NACK.
type Envelope struct {
	View    int64
	Origin  int
	FIFOSeq int64
	Payload Payload
}

// Direct packets use point-to-point sends (also fault-injected) rather than
// the reliable broadcast channel.
type Direct interface{ directTag() }

// Nack requests retransmission of a FIFO range from Origin.
type Nack struct {
	View    int64
	Origin  int
	From    int
	Begin   int64
	End     int64 // inclusive
}

// Repair is a point-to-point re-send of one envelope.
type Repair struct {
	Env Envelope
}

// Heartbeat announces liveness and the sender's current view.
type Heartbeat struct {
	View int64
	From int
}

// JoinReq is sent by a fresh incarnation that wants to enter the group.
type JoinReq struct {
	NewID int
	From  int
}

// FlushStart begins a view change: survivors stop accepting new broadcasts and
// report their received/delivered state back to the coordinator. Acked lists
// survivors whose FlushState the coordinator has already received, so a
// survivor can stop re-sending its (large) state report once acknowledged and
// the retransmit loop converges even under packet loss.
type FlushStart struct {
	NewView    int64
	OldView    int64
	Coord      int
	Survivors  []int // members of the new view, excluding joiners
	Joiners    []int
	Acked      []int // survivors whose FlushState has reached the coordinator
}

// FlushState is a survivor's state report for the flush.
type FlushState struct {
	View       int64
	From       int
	Received   []Envelope // every envelope the survivor holds for the old view
	Delivered  []MsgID    // application messages already delivered in the old view
	FinalOf    map[MsgID]int64 // final number for each delivered message
	KnownVoid  []MsgID    // void messages already learned earlier in the flush
}

// FlushFetch asks one member to re-send specific envelopes (it holds them, the
// coordinator does not).
type FlushFetch struct {
	View int64
	Want []MsgID
}

// FlushData returns fetched envelopes.
type FlushData struct {
	View int64
	From int
	Envs []Envelope
}

// FlushComplete installs the new view. It carries the authoritative old-view
// closure: union of all surviving envelopes, final order assignments for
// anything still pending, and the void set.
type FlushComplete struct {
	NewView    int64
	OldView    int64
	Coord      int
	Members    []int // full membership of the new view
	Union      []Envelope
	FinalOrder []FinalOrderEntry
	Void       []MsgID
}

// FIFOAnnounce advertises a sender's highest FIFO sequence so receivers can
// detect a tail loss (the newest envelope never arrived) and NACK for it.
// Without this, a receiver cannot ask for a gap whose upper bound it has
// never observed.
type FIFOAnnounce struct {
	View    int64
	Origin  int
	HighSeq int64
}

// InstallAck tells the coordinator that a member has applied a FlushComplete,
// so the coordinator can stop retransmitting it once everyone has installed.
type InstallAck struct {
	NewView int64
	From    int
}

// FlushStateReq is the coordinator's unicast pull for one survivor's
// FlushState, used when periodic state reports keep getting dropped.
type FlushStateReq struct {
	NewView int64
	From    int // coordinator
}

// CompleteReq is a survivor's unicast request for the FlushComplete it has
// not yet received, so a persistently dropped complete packet is re-pulled
// instead of leaving the member stuck in the flushing phase.
type CompleteReq struct {
	NewView int64
	From    int // requesting member
}

// FinalOrderEntry assigns a final total-order key to one message during flush.
type FinalOrderEntry struct {
	ID      MsgID
	FinalNo int64
}

func (*Nack) directTag()         {}
func (*Repair) directTag()       {}
func (*Heartbeat) directTag()    {}
func (*JoinReq) directTag()      {}
func (*FlushStart) directTag()   {}
func (*FlushState) directTag()   {}
func (*FlushFetch) directTag()   {}
func (*FlushData) directTag()    {}
func (*FlushComplete) directTag() {}
func (*FIFOAnnounce) directTag() {}
func (*InstallAck) directTag()   {}
func (*FlushStateReq) directTag() {}
func (*CompleteReq) directTag()  {}
