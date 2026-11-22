// Package sim contains the deterministic event scheduler used by the
// in-process group communication simulator.
//
// Everything in a simulation run executes on the scheduler's single goroutine.
// Members never run concurrently: a "goroutine member" in the problem statement
// is modelled as an event-driven actor whose timer firings and packet
// receptions are all queued here. That, together with a single seeded RNG in
// the network, makes every run bit-for-bit reproducible from its scenario.
package sim

import "container/heap"

// Event is a single scheduled callback.
type Event struct {
	time  int64
	order int64 // tie-breaker so simultaneous events keep insertion order
	fn    func()
	index int
}

// Scheduler is a min-heap keyed on virtual time.
type Scheduler struct {
	heap    eventHeap
	counter int64
	now     int64
	stopped bool
}

// NewScheduler creates an empty virtual clock.
func NewScheduler() *Scheduler { return &Scheduler{} }

// Now returns the current virtual time in ticks.
func (s *Scheduler) Now() int64 { return s.now }

// Schedule enqueues fn to run after delay ticks (delay >= 0).
func (s *Scheduler) Schedule(delay int64, fn func()) *Event {
	s.counter++
	ev := &Event{time: s.now + delay, order: s.counter, fn: fn}
	heap.Push(&s.heap, ev)
	return ev
}

// Stop terminates Run even if events remain.
func (s *Scheduler) Stop() { s.stopped = true }

// IsStopped reports whether Stop was called.
func (s *Scheduler) IsStopped() bool { return s.stopped }

// Run drains the heap in virtual-time order.
func (s *Scheduler) Run() {
	for s.heap.Len() > 0 && !s.stopped {
		ev := heap.Pop(&s.heap).(*Event)
		s.now = ev.time
		ev.fn()
	}
}

type eventHeap []*Event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].time != h[j].time {
		return h[i].time < h[j].time
	}
	return h[i].order < h[j].order
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i }
func (h *eventHeap) Push(x any) {
	ev := x.(*Event)
	ev.index = len(*h)
	*h = append(*h, ev)
}
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return ev
}
