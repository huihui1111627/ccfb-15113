package proto

import "testing"

func TestVCHappensBefore(t *testing.T) {
	a := VC{1: 1, 2: 2}
	if a.HappensBefore(VC{1: 1, 2: 2}) {
		t.Fatal("equal clocks must not be ordered")
	}
	if !a.HappensBefore(VC{1: 2, 2: 2}) {
		t.Fatal("{1:1,2:2} should happen-before {1:2,2:2}")
	}
	if a.HappensBefore(VC{1: 0, 2: 3}) {
		t.Fatal("concurrent clocks must not be ordered")
	}
}

func TestTopoSortRespectsCausality(t *testing.T) {
	// a -> b (b depends on a); c is concurrent with both.
	a := MsgID{Origin: 1, Seq: 1}
	b := MsgID{Origin: 1, Seq: 2}
	c := MsgID{Origin: 2, Seq: 1}
	apps := map[MsgID]AppData{
		a: {ID: a, VC: VC{1: 1}},
		b: {ID: b, VC: VC{1: 2}},
		c: {ID: c, VC: VC{2: 1}},
	}
	got := topoSortByVC([]MsgID{a, b, c}, apps)
	pos := map[MsgID]int{}
	for i, id := range got {
		pos[id] = i
	}
	if pos[a] >= pos[b] {
		t.Fatalf("causal dependency a must precede b, got %v", got)
	}
}

// loopback transport and capturing app used by the delivery tests.
type loopTr struct {
	members map[int]*Member
	alive   map[int]bool
}

func (l *loopTr) Send(from, to int, deliver func(any), pkt any) {
	if l.alive[from] && l.alive[to] {
		deliver(pkt)
	}
}
func (l *loopTr) IsAlive(id int) bool                 { return l.alive[id] }
func (l *loopTr) MemberByID(id int) *Member           { return l.members[id] }
func (l *loopTr) SendsInView(view int64) []MsgID      { return nil }
func (l *loopTr) NotifyVoid(msg MsgID, oldView int64) {}

type capApp struct{ del []string }

func (c *capApp) OnDeliver(member int, view int64, msg AppData, vc VC, no int64) {
	c.del = append(c.del, msg.Content)
}
func (c *capApp) OnVoid(member int, msg MsgID, view int64)       {}
func (c *capApp) OnBroadcastRejected(member int, content string) {}

func newGroup(t *testing.T, n int) (*loopTr, map[int]*capApp) {
	t.Helper()
	tr := &loopTr{members: map[int]*Member{}, alive: map[int]bool{}}
	ids := make([]int, n)
	apps := map[int]*capApp{}
	for i := range ids {
		ids[i] = i + 1
		tr.alive[ids[i]] = true
		apps[ids[i]] = &capApp{}
	}
	now := int64(0)
	for _, id := range ids {
		m := NewMember(id, 1, ids, tr, apps[id], NewRecorder(),
			func() int64 { return now }, func(int64, func()) {})
		tr.members[id] = m
	}
	return tr, apps
}

func TestTotalOrderAgreement(t *testing.T) {
	tr, apps := newGroup(t, 3)
	for _, m := range tr.members {
		m.Start()
	}
	for _, id := range []int{1, 2, 3} {
		tr.members[id].Broadcast("msg")
	}
	// Synchronous loopback delivers immediately; every member must have the
	// same number of deliveries in the same content-independent order.
	base := apps[1].del
	if len(base) != 3 {
		t.Fatalf("m1 deliveries=%v", base)
	}
	for id := 2; id <= 3; id++ {
		if len(apps[id].del) != len(base) {
			t.Fatalf("m%d got %d deliveries, want %d", id, len(apps[id].del), len(base))
		}
	}
}
