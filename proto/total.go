package proto

import (
	"fmt"
	"sort"
)

// totalOnCausal handles an application message that just became causally
// deliverable. Every member issues a provisional sequence number for it
// (round 1). The provisional numbers travel on the same reliable, causal
// channel so every member sees them in the same causal context.
//
// Important ordering subtlety: issuing our own provisional and *processing*
// it must not be a synchronous nested call. If it were, a sender processing
// its own application message would collect its own ack, possibly finalize,
// and try to pump while the outer causal delivery is still on the stack —
// the head message would look perpetually un-finalized. Envelope loopback
// through HandleEnvelope keeps every state transition at the same nesting
// level.
func (m *Member) totalOnCausal(app AppData) {
	m.provCounter++
	prov := m.provCounter
	m.provIssued[app.ID] = prov
	m.headPending[app.ID] = true
	ack := Provisional{For: app.ID, From: m.ID, Seq: prov, VC: m.clock.Clone()}
	m.rec.Record(m.now(), m.ID, EvProvSent, &app.ID, m.view,
		fmt.Sprintf("prov=%d", prov), nil)
	m.broadcastPayload(&ack)
}

// broadcastPayload wraps a control payload (provisional/final) in an envelope
// and sends it over the reliable channel. The local loopback goes through
// HandleEnvelope like any other arrival.
func (m *Member) broadcastPayload(p Payload) {
	if m.phase != PhaseNormal {
		// Flush started before we could send: the flush re-finalizes state
		// explicitly, so skip the regular channel.
		return
	}
	m.sendSeq++
	env := Envelope{View: m.view, Origin: m.ID, FIFOSeq: m.sendSeq, Payload: p}
	m.broadcastEnv(env)
	m.HandleEnvelope(env)
}

// totalReceiveProvisional collects round-1 replies. Only the original sender
// needs the full set; other members still record what they have seen so the
// UI can explain why a message is waiting.
func (m *Member) totalReceiveProvisional(p Provisional) {
	m.rec.Record(m.now(), m.ID, EvProvRecv, &p.For, m.view,
		fmt.Sprintf("from=%d prov=%d", p.From, p.Seq), nil)
	if p.From != m.ID {
		// Others' provisional numbers advance our local counter so our own
		// numbering keeps moving forward relative to the group.
		if p.Seq > m.provCounter {
			m.provCounter = p.Seq
		}
	}
	if m.ID != p.For.Origin {
		return // only the origin finalizes its own message
	}
	set, ok := m.acks[p.For]
	if !ok {
		set = map[int]Provisional{}
		m.acks[p.For] = set
	}
	if _, dup := set[p.From]; dup {
		return
	}
	set[p.From] = p
	if m.phase != PhaseNormal {
		return // flush protocol takes over completion
	}
	if len(set) == len(m.members) {
		m.finalize(p.For)
	}
}

// finalize picks the maximum provisional number (round 2) and broadcasts it.
func (m *Member) finalize(id MsgID) {
	var maxSeq int64
	for _, p := range m.acks[id] {
		if p.Seq > maxSeq {
			maxSeq = p.Seq
		}
	}
	m.finals[id] = maxSeq
	fin := Final{For: id, FinalNo: maxSeq}
	m.rec.Record(m.now(), m.ID, EvFinalSent, &id, m.view,
		fmt.Sprintf("final=%d", maxSeq), nil)
	m.broadcastPayload(&fin)
}

// totalReceiveFinal records the authoritative final order and tries to pump.
func (m *Member) totalReceiveFinal(f Final) {
	m.rec.Record(m.now(), m.ID, EvFinalRecv, &f.For, m.view,
		fmt.Sprintf("final=%d", f.FinalNo), nil)
	m.finals[f.For] = f.FinalNo
	if m.phase == PhaseNormal {
		m.totalPump()
	}
}

// totalPump delivers everything the ordering rules currently allow.
//
// Two constraints from the problem statement interact here:
//
//   - Head-of-line (item 3): while the message with the smallest provisional
//     number has no final number, nothing is delivered.
//   - Tie-break (item 3): messages sharing a final number are ordered by the
//     sender's member id, identically on every member.
//
// Final numbers are computed independently by each origin, so two different
// messages can legitimately share a final number without their Final packets
// arriving together. Delivering one as soon as its Final shows up would
// release it before the same-numbered peer is known, breaking the tie-break.
// We therefore release a finalized message only when its GLOBAL key is
// strictly below every key an unfinalized pending message could still take.
// The smallest provisional among unfinalized messages is a safe lower bound
// on those future final numbers (a final number is always >= the provisional
// its origin received), giving the release rule:
//
//	release finalized m  iff  finalNo(m) < gateProvisional
//	or there is no unfinalized message (gate = infinity).
//
// All released messages are then emitted in (final number, sender id) order,
// so equal-numbered peers come out in member-id order once both are known.
func (m *Member) totalPump() {
	for len(m.headPending) > 0 {
		var gate int64 = 1<<62
		var blocker MsgID
		for id := range m.headPending {
			if _, finalized := m.finals[id]; !finalized {
				seq := m.provIssued[id]
				if seq < gate {
					gate, blocker = seq, id
				}
			}
		}

		ready := make([]keyed, 0, len(m.headPending))
		for id := range m.headPending {
			no, hasFinal := m.finals[id]
			if !hasFinal {
				continue
			}
			// A finalized message can go only if no unfinalized message can
			// still claim a final number <= its own. Equal numbers are held
			// back together until the provisional gate advances past them.
			if no >= gate {
				continue
			}
			ready = append(ready, keyed{id, no, m.provIssued[id]})
		}

		if len(ready) == 0 {
			// Everything releasable is gated on an outstanding final. Record
			// the blocking relationship once per held message for the UI.
			for id := range m.headPending {
				if !m.waitingLog[id] {
					m.waitingLog[id] = true
					detail := "waiting for outstanding final"
					if gate != 1<<62 {
						detail = fmt.Sprintf(
							"blocked behind head m%d#%d (prov=%d)",
							blocker.Origin, blocker.Seq, gate)
					}
					m.rec.Record(m.now(), m.ID, EvTotalWait, &id, m.view, detail, nil)
				}
			}
			return
		}
		m.deliverReady(ready)
	}
}

type keyed struct {
	id      MsgID
	finalNo int64
	prov    int64
}

func (m *Member) deliverReady(ready []keyed) {
	// Global total-order key: (final number, sender's member id).
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].finalNo != ready[j].finalNo {
			return ready[i].finalNo < ready[j].finalNo
		}
		return ready[i].id.Origin < ready[j].id.Origin
	})
	for _, k := range ready {
		app := m.appByID[k.id]
		delete(m.headPending, k.id)
		delete(m.waitingLog, k.id)
		m.rec.Record(m.now(), m.ID, EvTotalDeliver, &k.id, m.view,
			fmt.Sprintf("final=%d", k.finalNo), nil)
		m.app.OnDeliver(m.ID, m.view, app, m.clock.Clone(), k.finalNo)
	}
}
