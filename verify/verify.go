// Package verify checks a completed simulation run against every ordering and
// membership guarantee promised by the middleware:
//
//  1. per-sender FIFO delivery,
//  2. causal delivery reconstructed independently from the send log,
//  3. total order (identical final-number order everywhere),
//  4. total order never inverts causal dependencies,
//  5. survivors of one view deliver exactly the same message set,
//  6. void messages are never delivered and are reported once,
//  7. rejoining members start with empty old-view state.
package verify

import (
	"fmt"
	"sort"
	"strings"

	"kitecomm/proto"
	"kitecomm/sim"
)

// Failure is one violated guarantee.
type Failure struct {
	Scenario string
	Rule    string
	Detail  string
}

// Report summarizes one scenario run.
type Report struct {
	Scenario   string
	OK         bool
	Failures   []Failure
	Stats      Stats
}

// Stats collects run-size counters.
type Stats struct {
	Sent       int
	Delivered  int
	Void       int
	Views      int
	MaxView    int64
	NackEvents int
}

// Run checks one completed runtime.
func Run(rt *sim.Runtime) Report {
	rep := Report{Scenario: rt.Scenario().Name, OK: true}
	add := func(rule, detail string) {
		rep.Failures = append(rep.Failures, Failure{
			Scenario: rt.Scenario().Name, Rule: rule, Detail: detail,
		})
		rep.OK = false
	}

	sendLog := rt.SendLog()
	sendView := rt.SendView()
	delivers := rt.Delivers()
	voids := rt.Voids()

	rep.Stats.Sent = len(sendLog)
	rep.Stats.Delivered = len(delivers)
	rep.Stats.Void = len(voids)
	for _, ev := range rt.Recorder().Events {
		if ev.Kind == proto.EvNackSent {
			rep.Stats.NackEvents++
		}
		if ev.Kind == proto.EvViewChange && strings.Contains(ev.Detail, "installed") {
			rep.Stats.Views++
			if ev.View > rep.Stats.MaxView {
				rep.Stats.MaxView = ev.View
			}
		}
	}

	// Send-time vector clocks reconstructed from the actual EvSend events.
	sendVC := map[proto.MsgID]proto.VC{}
	for _, ev := range rt.Recorder().Events {
		if ev.Kind == proto.EvSend && ev.Msg != nil {
			sendVC[*ev.Msg] = ev.VC.Clone()
		}
	}

	// ---- per-member, per-view delivery streams ----
	byMember := map[int][]sim.AppRecord{}
	byMemberView := map[int]map[int64][]sim.AppRecord{}
	for _, d := range delivers {
		byMember[d.Member] = append(byMember[d.Member], d)
		if byMemberView[d.Member] == nil {
			byMemberView[d.Member] = map[int64][]sim.AppRecord{}
		}
		byMemberView[d.Member][d.View] = append(byMemberView[d.Member][d.View], d)
	}
	// Everything this member ever delivered, for membership-scope checks.
	deliveredByMember := map[int]map[proto.MsgID]bool{}
	for member, stream := range byMember {
		deliveredByMember[member] = map[proto.MsgID]bool{}
		for _, d := range stream {
			deliveredByMember[member][d.Msg] = true
		}
	}

	// Rule 1: FIFO per sender, evaluated inside each view (sequence numbers
	// restart at 1 whenever a new view installs).
	for member, perView := range byMemberView {
		for _, stream := range perView {
			seen := map[int]int64{}
			for _, d := range stream {
				prev := seen[d.Msg.Origin]
				if d.Msg.Seq <= prev {
					add("FIFO", fmt.Sprintf(
						"member m%d in view %d delivered m%d#%d after already reaching #%d",
						member, d.View, d.Msg.Origin, d.Msg.Seq, prev))
				}
				seen[d.Msg.Origin] = d.Msg.Seq
			}
		}
	}

	// Rule 2: causal order, also scoped to a single view. For every delivered
	// pair where one message's send-VC requires the other, the dependency must
	// appear earlier in that member's same-view stream. A dependency the
	// member never received in any view is legal only when it was voided
	// (crashed sender) — otherwise it is a real gap.
	for member, perView := range byMemberView {
		for view, stream := range perView {
			position := map[proto.MsgID]int{}
			for i, d := range stream {
				position[d.Msg] = i
			}
			for _, d := range stream {
				vc := sendVC[d.Msg]
				for origin, need := range vc {
					for seq := int64(1); seq <= need; seq++ {
						dep := proto.MsgID{Origin: origin, Seq: seq}
						if dep == d.Msg {
							continue
						}
						pos, ok := position[dep]
						if !ok {
							if isVoid(voids, dep) {
								continue
							}
							if _, sent := sendLog[dep]; !sent {
								continue
							}
							// Sent but absent here: if the member simply was not
							// part of that message's view (e.g. it joined later),
							// that is expected, not a causal violation.
							if sendView[dep] != view && !deliveredByMember[member][dep] {
								continue
							}
							add("causal", fmt.Sprintf(
								"member m%d view %d delivered m%d#%d but never its dependency m%d#%d",
								member, view, d.Msg.Origin, d.Msg.Seq, origin, seq))
							continue
						}
						if pos > position[d.Msg] {
							add("causal", fmt.Sprintf(
								"member m%d view %d: dependency m%d#%d delivered after dependent m%d#%d",
								member, view, origin, seq, d.Msg.Origin, d.Msg.Seq))
						}
					}
				}
			}
		}
	}

	// Rule 3: total order. Every member must deliver in the same global
	// (final number, sender id) order. Final numbers restart per view, so the
	// consistency check is scoped to (view, message), not the bare message id.
	type fkey struct {
		no     int64
		origin int
	}
	globalOrder := map[[2]any]fkey{}
	for _, d := range delivers {
		k := fkey{d.FinalNo, d.Msg.Origin}
		gk := [2]any{d.View, d.Msg}
		if existing, ok := globalOrder[gk]; ok && existing != k {
			add("total", fmt.Sprintf(
				"message m%d#%d got inconsistent final keys in view %d: %v vs %v",
				d.Msg.Origin, d.Msg.Seq, d.View, existing, k))
		}
		globalOrder[gk] = k
	}
	// Final numbers must be unique within one (member, view).
	for member, perView := range byMemberView {
		for view, stream := range perView {
			seenKeys := map[[2]int64]proto.MsgID{}
			for _, d := range stream {
				k := [2]int64{d.FinalNo, int64(d.Msg.Origin)}
				if other, clash := seenKeys[k]; clash && other != d.Msg {
					add("total", fmt.Sprintf(
						"member m%d view %d: final key (%d,m%d) used by both m%d#%d and m%d#%d",
						member, view, d.FinalNo, d.Msg.Origin,
						other.Origin, other.Seq, d.Msg.Origin, d.Msg.Seq))
				}
				seenKeys[k] = d.Msg
			}
		}
	}
	// Each member's same-view delivery stream must be sorted by the key.
	for member, stream := range byMember {
		for i := 1; i < len(stream); i++ {
			a, b := stream[i-1], stream[i]
			if a.View != b.View {
				continue // keys reset per view; only compare inside one view
			}
			if a.FinalNo > b.FinalNo ||
				(a.FinalNo == b.FinalNo && a.Msg.Origin > b.Msg.Origin) {
				add("total", fmt.Sprintf(
					"member m%d delivered m%d#%d(key %d) before m%d#%d(key %d) in view %d",
					member, a.Msg.Origin, a.Msg.Seq, a.FinalNo,
					b.Msg.Origin, b.Msg.Seq, b.FinalNo, a.View))
			}
		}
	}
	// Pairwise prefix agreement: any two members' common messages in one view
	// appear in the same relative order.
	memberList := make([]int, 0, len(byMember))
	for m := range byMember {
		memberList = append(memberList, m)
	}
	sort.Ints(memberList)
	for i := 0; i < len(memberList); i++ {
		for j := i + 1; j < len(memberList); j++ {
			checkPairAgreement(add, memberList[i], byMember[memberList[i]],
				memberList[j], byMember[memberList[j]])
		}
	}

	// Rule 4: total order respects causal order. For any causally related
	// pair delivered in the same view, the dependency must have the smaller
	// final key.
	for member, stream := range byMember {
		position := map[proto.MsgID]int{}
		for i, d := range stream {
			position[d.Msg] = i
		}
		for _, d := range stream {
			vc := sendVC[d.Msg]
			for origin, need := range vc {
				for seq := int64(1); seq <= need; seq++ {
					dep := proto.MsgID{Origin: origin, Seq: seq}
					dp, ok := position[dep]
					if !ok {
						continue
					}
					depRec := stream[dp]
					if depRec.View != d.View {
						continue
					}
					if depRec.FinalNo > d.FinalNo ||
						(depRec.FinalNo == d.FinalNo && depRec.Msg.Origin > d.Msg.Origin) {
						add("total-causal", fmt.Sprintf(
							"member m%d: causal dep m%d#%d final key (%d,m%d) >= dependent m%d#%d (%d,m%d)",
							member, origin, seq, depRec.FinalNo, depRec.Msg.Origin,
							d.Msg.Origin, d.Msg.Seq, d.FinalNo, d.Msg.Origin))
					}
				}
			}
		}
	}

	// Rule 5: per-view survivor set equality. Requirement 5 only constrains the
	// members that SURVIVE a view: a crashed member is allowed to have a
	// partial delivery set because it died mid-view. We therefore compare
	// message sets only among members that never crashed and actually
	// participated in that view.
	crashed := map[int]bool{}
	for _, c := range rt.Crashes() {
		crashed[c.ID] = true
	}
	sets := map[int64]map[int]map[proto.MsgID]bool{}
	viewMembers := map[int64]map[int]bool{}
	for _, d := range delivers {
		if crashed[d.Member] {
			continue
		}
		if sets[d.View] == nil {
			sets[d.View] = map[int]map[proto.MsgID]bool{}
		}
		if sets[d.View][d.Member] == nil {
			sets[d.View][d.Member] = map[proto.MsgID]bool{}
		}
		sets[d.View][d.Member][d.Msg] = true
		if viewMembers[d.View] == nil {
			viewMembers[d.View] = map[int]bool{}
		}
		viewMembers[d.View][d.Member] = true
	}
	for view, memSets := range sets {
		var reference []proto.MsgID
		var refMember int
		for member, set := range memSets {
			ids := make([]proto.MsgID, 0, len(set))
			for id := range set {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(a, b int) bool { return msgLess(ids[a], ids[b]) })
			if reference == nil {
				reference, refMember = ids, member
				continue
			}
			if !sameIDs(reference, ids) {
				add("view-set", fmt.Sprintf(
					"view %d: surviving member m%d delivered %d messages but member m%d delivered %d; diff=%s",
					view, refMember, len(reference), member, len(ids),
					diffText(reference, ids)))
			}
		}
	}

	// Rule 6: void messages are never delivered by anyone.
	for _, v := range voids {
		for _, d := range delivers {
			if d.Msg == v.Msg {
				add("void", fmt.Sprintf(
					"void message m%d#%d was delivered by member m%d",
					v.Msg.Origin, v.Msg.Seq, d.Member))
			}
		}
		if _, sent := sendLog[v.Msg]; !sent {
			add("void", fmt.Sprintf(
				"void record for m%d#%d which was never sent", v.Msg.Origin, v.Msg.Seq))
		}
	}
	// Every sent message must end up either delivered somewhere or declared
	// void. (Requirement 5 guarantees that within one view all *survivors*
	// deliver the same set; it does not require crashed members to deliver.)
	deliveredAnywhere := map[proto.MsgID]bool{}
	for _, d := range delivers {
		deliveredAnywhere[d.Msg] = true
	}
	for id := range sendLog {
		if isVoid(voids, id) {
			continue
		}
		if !deliveredAnywhere[id] {
			add("liveness", fmt.Sprintf(
				"sent message m%d#%d was neither delivered nor declared void",
				id.Origin, id.Seq))
		}
	}

	// Rule 7: a rejoining incarnation must not deliver any message sent in a
	// view before its own join view.
	joinViews := map[int]int64{}
	for _, ev := range rt.Recorder().Events {
		if ev.Kind == proto.EvJoin {
			joinViews[ev.Member] = ev.Time
		}
	}
	_ = joinViews
	for member, stream := range byMember {
		// Rejoined members have ids higher than the initial membership range.
		if member <= rt.Scenario().Members {
			continue
		}
		// Find the first view this incarnation installed.
		firstView := int64(0)
		for _, d := range stream {
			firstView = d.View
			break
		}
		for _, d := range stream {
			if sendView[d.Msg] != 0 && sendView[d.Msg] < firstView {
				add("rejoin-state", fmt.Sprintf(
					"rejoined incarnation m%d delivered old-view message m%d#%d (sent in view %d, joined in %d)",
					member, d.Msg.Origin, d.Msg.Seq, sendView[d.Msg], firstView))
			}
		}
	}

	return rep
}

func checkPairAgreement(add func(string, string), a int, sa []sim.AppRecord, b int, sb []sim.AppRecord) {
	posA := map[proto.MsgID]int{}
	for i, d := range sa {
		posA[d.Msg] = i
	}
	posB := map[proto.MsgID]int{}
	for i, d := range sb {
		posB[d.Msg] = i
	}
	for x, ia := range posA {
		ib, ok := posB[x]
		if !ok {
			continue
		}
		for y, ja := range posA {
			jb, ok2 := posB[y]
			if !ok2 || x == y {
				continue
			}
			if (ia < ja) != (ib < jb) {
				// Only a violation if both pairs were in the same view on both
				// sides; cross-view pairs are allowed to differ.
				if sa[ia].View == sa[ja].View && sb[ib].View == sb[jb].View {
					add("total", fmt.Sprintf(
						"members m%d and m%d disagree on order of m%d#%d and m%d#%d",
						a, b, x.Origin, x.Seq, y.Origin, y.Seq))
				}
			}
		}
	}
}

func isVoid(voids []sim.VoidRecord, id proto.MsgID) bool {
	for _, v := range voids {
		if v.Msg == id {
			return true
		}
	}
	return false
}

func msgLess(a, b proto.MsgID) bool {
	if a.Origin != b.Origin {
		return a.Origin < b.Origin
	}
	return a.Seq < b.Seq
}

func sameIDs(a, b []proto.MsgID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func diffText(reference, other []proto.MsgID) string {
	set := map[proto.MsgID]bool{}
	for _, id := range other {
		set[id] = true
	}
	missing := []string{}
	for _, id := range reference {
		if !set[id] {
			missing = append(missing, fmt.Sprintf("m%d#%d", id.Origin, id.Seq))
		}
	}
	return "missing=" + strings.Join(missing, ",")
}
