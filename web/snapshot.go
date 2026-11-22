// Package web serves the message-delivery visualization. It runs a chosen
// deterministic scenario, captures the full event trace, and exposes both a
// static snapshot API and a single-page HTML dashboard.
package web

import (
	"sort"

	"kitecomm/proto"
	"kitecomm/sim"
)

// Snapshot is the JSON payload rendered by the dashboard.
type Snapshot struct {
	Scenario  string       `json:"scenario"`
	EndTime   int64        `json:"endTime"`
	Members   []MemberView `json:"members"`
	Events    []EventView  `json:"events"`
	Messages  []MsgView    `json:"messages"`
	Views     []ViewSpan   `json:"views"`
	Stats     StatsView    `json:"stats"`
	Scenarios []string     `json:"scenarios"`
}

// MemberView is one member's current state and per-view delivery sequence.
type MemberView struct {
	ID           int             `json:"id"`
	Alive        bool            `json:"alive"`
	Phase        string          `json:"phase"`
	View         int64           `json:"view"`
	Clock        map[int]int64   `json:"clock"`
	Delivered    []DeliveryView  `json:"delivered"`
	CausalBuffer []MsgRef        `json:"causalBuffer"`
	TotalPending []PendingView   `json:"totalPending"`
}

// DeliveryView is one total-order delivery record.
type DeliveryView struct {
	Time    int64           `json:"time"`
	View    int64           `json:"view"`
	Msg     MsgRef          `json:"msg"`
	Content string          `json:"content"`
	FinalNo int64           `json:"finalNo"`
}

// MsgRef identifies a message compactly.
type MsgRef struct {
	Origin int   `json:"origin"`
	Seq    int64 `json:"seq"`
}

// PendingView is a message held in the total-order layer.
type PendingView struct {
	Msg         MsgRef `json:"msg"`
	Provisional int64  `json:"provisional"`
	Final       int64  `json:"final"`
	HeadBlocked bool   `json:"headBlocked"`
}

// EventView is one row in the global event timeline.
type EventView struct {
	Time   int64  `json:"time"`
	Member int    `json:"member"`
	Kind   string `json:"kind"`
	Msg    *MsgRef `json:"msg,omitempty"`
	Detail string `json:"detail"`
	View   int64  `json:"view"`
}

// MsgView is the per-message delivery detail shown when a message is clicked.
type MsgView struct {
	Msg        MsgRef          `json:"msg"`
	Content    string          `json:"content"`
	Sender     int             `json:"sender"`
	SendTime   int64           `json:"sendTime"`
	SendView   int64           `json:"sendView"`
	Void       bool            `json:"void"`
	PerMember  []MsgMemberView `json:"perMember"`
}

// MsgMemberView explains where a message spent time at one member.
type MsgMemberView struct {
	Member       int    `json:"member"`
	ReliableRecv int64  `json:"reliableRecv"` // 0 = never reached the FIFO layer
	CausalEnter  int64  `json:"causalEnter"`
	CausalLeave  int64  `json:"causalLeave"`
	CausalWait   int64  `json:"causalWait"`   // ticks held in the causal buffer
	BlockedOn    string `json:"blockedOn"`    // dependency / head it waited on
	TotalWait    int64  `json:"totalWait"`    // ticks held in the total layer
	DeliveredAt  int64  `json:"deliveredAt"`  // 0 = not delivered here
	FinalNo      int64  `json:"finalNo"`
}

// ViewSpan marks an installed view's membership over time.
type ViewSpan struct {
	View    int64 `json:"view"`
	Members []int `json:"members"`
}

// StatsView summarizes the run.
type StatsView struct {
	Sent      int `json:"sent"`
	Delivered int `json:"delivered"`
	Void      int `json:"void"`
	Nacks     int `json:"nacks"`
}

func refOf(id proto.MsgID) MsgRef { return MsgRef{Origin: id.Origin, Seq: id.Seq} }

// BuildSnapshot runs a scenario and projects its trace + final state into the
// dashboard payload.
func BuildSnapshot(sc sim.Scenario) *Snapshot {
	rt := sim.NewRuntime(sc)
	rt.Run()
	rec := rt.Recorder()

	snap := &Snapshot{Scenario: sc.Name, EndTime: rt.EndTime()}

	// ---- member final state ----
	liveIDs := make([]int, 0, len(rt.Members()))
	for id := range rt.Members() {
		liveIDs = append(liveIDs, id)
	}
	sort.Ints(liveIDs)
	ever := everMemberIDs(rec)
	for _, id := range ever {
		m, alive := rt.Members()[id]
		mv := MemberView{
			ID: id, Alive: alive,
			CausalBuffer: []MsgRef{}, TotalPending: []PendingView{}, Delivered: []DeliveryView{},
		}
		if alive {
			mv.Phase = phaseName(m.Phase())
			mv.View = m.View()
			mv.Clock = m.Clock()
			mv.TotalPending = []PendingView{}
			for _, p := range m.TotalPendingSnapshot() {
				mv.TotalPending = append(mv.TotalPending, PendingView{
					Msg: refOf(p.Msg), Provisional: p.Provisional,
					Final: p.Final, HeadBlocked: p.HeadBlocked,
				})
			}
			mv.CausalBuffer = []MsgRef{}
			for _, a := range m.CausalBufferSnapshot() {
				mv.CausalBuffer = append(mv.CausalBuffer, refOf(a.ID))
			}
		}
		snap.Members = append(snap.Members, mv)
	}

	// ---- delivery records per member ----
	delivByMember := map[int][]DeliveryView{}
	for _, d := range rt.Delivers() {
		delivByMember[d.Member] = append(delivByMember[d.Member], DeliveryView{
			Time: d.Time, View: d.View, Msg: refOf(d.Msg),
			Content: d.Content, FinalNo: d.FinalNo,
		})
	}
	for i := range snap.Members {
		if dl, ok := delivByMember[snap.Members[i].ID]; ok {
			snap.Members[i].Delivered = dl
		}
	}

	// ---- full event timeline ----
	for _, ev := range rec.Events {
		evv := EventView{
			Time: ev.Time, Member: ev.Member, Kind: string(ev.Kind),
			Detail: ev.Detail, View: ev.View,
		}
		if ev.Msg != nil {
			r := refOf(*ev.Msg)
			evv.Msg = &r
		}
		snap.Events = append(snap.Events, evv)
	}

	// ---- per-message deep dive ----
	snap.Messages = buildMessageViews(rt)

	// ---- view spans ----
	seenViews := map[int64][]int{}
	for _, ev := range rec.Events {
		if ev.Kind == proto.EvViewChange && len(ev.Detail) > 0 {
			// membership is parsed from member final views; keep view ids here
			seenViews[ev.View] = nil
		}
	}
	viewIDs := make([]int64, 0, len(seenViews))
	for v := range seenViews {
		viewIDs = append(viewIDs, v)
	}
	sort.Slice(viewIDs, func(i, j int) bool { return viewIDs[i] < viewIDs[j] })
	for _, v := range viewIDs {
		snap.Views = append(snap.Views, ViewSpan{View: v})
	}

	snap.Stats = StatsView{
		Sent:      len(rt.SendLog()),
		Delivered: len(rt.Delivers()),
		Void:      len(rt.Voids()),
	}
	for _, ev := range rec.Events {
		if ev.Kind == proto.EvNackSent {
			snap.Stats.Nacks++
		}
	}

	all := sim.Catalog()
	snap.Scenarios = make([]string, len(all))
	for i, c := range all {
		snap.Scenarios[i] = c.Name
	}
	return snap
}

func everMemberIDs(rec *proto.Recorder) []int {
	set := map[int]bool{}
	for _, ev := range rec.Events {
		set[ev.Member] = true
	}
	out := make([]int, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func phaseName(p proto.Phase) string {
	switch p {
	case proto.PhaseNormal:
		return "normal"
	case proto.PhaseJoining:
		return "joining"
	case proto.PhaseFlushing:
		return "flushing"
	}
	return "unknown"
}

// buildMessageViews reconstructs, for every sent message, where it was held at
// each member and what it was blocked on — the data behind the click-to-inspect
// requirement (item 8).
func buildMessageViews(rt *sim.Runtime) []MsgView {
	rec := rt.Recorder()
	contents := map[proto.MsgID]string{}
	sendView := map[proto.MsgID]int64{}
	sendTime := map[proto.MsgID]int64{}
	for _, ev := range rec.Events {
		if ev.Kind == proto.EvSend && ev.Msg != nil {
			contents[*ev.Msg] = ev.Detail
			sendView[*ev.Msg] = ev.View
			sendTime[*ev.Msg] = ev.Time
		}
	}
	voidSet := map[proto.MsgID]bool{}
	for _, v := range rt.Voids() {
		voidSet[v.Msg] = true
	}

	// Gather per-member timestamps from the trace.
	type acc struct {
		reliable, cEnter, cLeave, delivered, totalEnter int64
		finalNo                                         int64
		blockedOn                                       string
	}
	type key struct {
		member int
		msg    proto.MsgID
	}
	tbl := map[key]*acc{}
	get := func(member int, msg proto.MsgID) *acc {
		k := key{member, msg}
		a, ok := tbl[k]
		if !ok {
			a = &acc{}
			tbl[k] = a
		}
		return a
	}
	for _, ev := range rec.Events {
		if ev.Msg == nil {
			continue
		}
		a := get(ev.Member, *ev.Msg)
		switch ev.Kind {
		case proto.EvReliableRecv:
			if a.reliable == 0 {
				a.reliable = ev.Time
			}
		case proto.EvCausalBuffer:
			if a.cEnter == 0 {
				a.cEnter = ev.Time
			}
			a.blockedOn = ev.Detail
		case proto.EvCausalReady:
			a.cLeave = ev.Time
		case proto.EvTotalWait:
			if a.blockedOn == "" {
				a.blockedOn = ev.Detail
			}
			if a.totalEnter == 0 {
				a.totalEnter = ev.Time
			}
		case proto.EvTotalDeliver:
			a.delivered = ev.Time
		}
	}
	for _, d := range rt.Delivers() {
		get(d.Member, d.Msg).finalNo = d.FinalNo
	}

	ids := make([]proto.MsgID, 0, len(sendTime))
	for id := range sendTime {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Origin != ids[j].Origin {
			return ids[i].Origin < ids[j].Origin
		}
		return ids[i].Seq < ids[j].Seq
	})

	out := make([]MsgView, 0, len(ids))
	for _, id := range ids {
		mv := MsgView{
			Msg: refOf(id), Content: contents[id], Sender: id.Origin,
			SendTime: sendTime[id], SendView: sendView[id], Void: voidSet[id],
		}
		for _, member := range everMemberIDs(rec) {
			a := get(member, id)
			mmv := MsgMemberView{
				Member: member, ReliableRecv: a.reliable,
				CausalEnter: a.cEnter, CausalLeave: a.cLeave,
				BlockedOn: a.blockedOn, DeliveredAt: a.delivered,
				FinalNo: a.finalNo,
			}
			if a.cEnter > 0 && a.cLeave >= a.cEnter {
				mmv.CausalWait = a.cLeave - a.cEnter
			}
			if a.totalEnter > 0 && a.delivered >= a.totalEnter {
				mmv.TotalWait = a.delivered - a.totalEnter
			}
			mv.PerMember = append(mv.PerMember, mmv)
		}
		out = append(out, mv)
	}
	return out
}
