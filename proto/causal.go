package proto

import (
	"fmt"
	"sort"
)

// causalReceive is the ingress of the causal layer. Messages whose vector
// dependencies have not all been delivered park in causalBuf and are retried
// whenever the local clock advances.
func (m *Member) causalReceive(app AppData) {
	if _, seen := m.appByID[app.ID]; seen {
		return // duplicate (envelope identity already deduped, defensive)
	}
	if !m.causalDeliverable(app) {
		if _, parked := m.causalBuf[app.ID]; !parked {
			m.causalBuf[app.ID] = app
			m.rec.Record(m.now(), m.ID, EvCausalBuffer, &app.ID, m.view,
				"waiting on "+m.missingDepsText(app.VC), app.VC.Clone())
		}
		return
	}
	m.causalDoDeliver(app)
	m.drainCausalBuffer()
}

// causalDeliverable checks that every *dependency* of the message is already
// reflected in the local clock. A message's VC includes its own component
// (origin -> own seq), which the local clock cannot contain until delivery
// itself advances it, so that one component is excluded from the check.
func (m *Member) causalDeliverable(app AppData) bool {
	for k, v := range app.VC {
		if k == app.ID.Origin {
			continue
		}
		if m.clock[k] < v {
			return false
		}
	}
	return true
}

func (m *Member) missingDepsText(need VC) string {
	missing := []int{}
	for k, v := range need {
		if m.clock[k] < v {
			missing = append(missing, k)
		}
	}
	sort.Ints(missing)
	s := ""
	for i, k := range missing {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("m%d:%d<%d", k, m.clock[k], need[k])
	}
	return s
}

// drainCausalBuffer repeatedly releases anything whose dependencies are now
// satisfied. Each release advances the clock, which may unlock further
// messages; loop until a full pass releases nothing.
func (m *Member) drainCausalBuffer() {
	for {
		progressed := false
		ids := make([]MsgID, 0, len(m.causalBuf))
		for id := range m.causalBuf {
			ids = append(ids, id)
		}
		// Deterministic iteration order: origin then seq.
		sort.Slice(ids, func(i, j int) bool {
			if ids[i].Origin != ids[j].Origin {
				return ids[i].Origin < ids[j].Origin
			}
			return ids[i].Seq < ids[j].Seq
		})
		for _, id := range ids {
			app := m.causalBuf[id]
			if m.causalDeliverable(app) {
				delete(m.causalBuf, id)
				m.rec.Record(m.now(), m.ID, EvCausalReady, &id, m.view, "", nil)
				m.causalDoDeliver(app)
				progressed = true
			}
		}
		if !progressed {
			return
		}
	}
}

// causalDoDeliver merges the clock (including the message's own component),
// logs the event, and hands the message to the total-order layer.
func (m *Member) causalDoDeliver(app AppData) {
	app.VC.CopyInto(m.clock)
	m.clock[app.ID.Origin] = app.ID.Seq
	m.appByID[app.ID] = app
	m.rec.Record(m.now(), m.ID, EvCausalDeliver, &app.ID, m.view,
		"vc="+vcText(app.VC), app.VC.Clone())
	m.totalOnCausal(app)
}

func vcText(v VC) string {
	keys := make([]int, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	s := "{"
	for i, k := range keys {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%d:%d", k, v[k])
	}
	return s + "}"
}
