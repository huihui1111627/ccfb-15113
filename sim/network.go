package sim

import (
	"math/rand"
)

// Packet is anything that can travel between two members over the network.
type Packet any

// Link describes the fault characteristics of one directed link (from -> to).
type Link struct {
	Loss       float64 // probability a packet is dropped
	MinDelay   int64   // minimum delivery delay in ticks
	MaxDelay   int64   // maximum delivery delay in ticks
	ExtraLoss  float64 // extra loss applied while a fault window is active
}

// Network is the shared, fault-injecting packet fabric for one simulation.
//
// All delivery is scheduled on the same Scheduler that drives the members, so
// the network's RNG draws and the timer firings share one deterministic
// timeline. A member that has crashed (or re-joined under a new id) is marked
// dead: packets addressed to its old incarnation are dropped at the door,
// which models "the process is gone" rather than a network loss.
type Network struct {
	sched   *Scheduler
	rng     *rand.Rand
	links   map[[2]int]*Link
	alive   map[int]bool
	blocked map[int]bool // partitioned members: every send/receive is blocked
	netLoss float64      // global extra loss applied to every link
}

// NewNetwork builds a network. linkSeed makes all delay/loss draws repeatable.
func NewNetwork(sched *Scheduler, linkSeed int64) *Network {
	return &Network{
		sched:   sched,
		rng:     rand.New(rand.NewSource(linkSeed)),
		links:   map[[2]int]*Link{},
		alive:   map[int]bool{},
		blocked: map[int]bool{},
	}
}

// SetAlive registers whether an incarnation (member id) exists.
func (n *Network) SetAlive(id int, alive bool) { n.alive[id] = alive }

// IsAlive reports whether the incarnation is currently up.
func (n *Network) IsAlive(id int) bool { return n.alive[id] }

// SetPartitioned marks a member as network-partitioned.
func (n *Network) SetPartitioned(id int, on bool) { n.blocked[id] = on }

// SetGlobalLoss adds (or removes) a cluster-wide extra loss probability.
func (n *Network) SetGlobalLoss(p float64) { n.netLoss = p }

// Link returns the link descriptor for from->to, creating a default one.
func (n *Network) Link(from, to int) *Link {
	key := [2]int{from, to}
	l, ok := n.links[key]
	if !ok {
		l = &Link{MinDelay: 2, MaxDelay: 6}
		n.links[key] = l
	}
	return l
}

// SetLink configures one directed link.
func (n *Network) SetLink(from, to int, loss float64, minD, maxD int64) {
	l := n.Link(from, to)
	l.Loss, l.MinDelay, l.MaxDelay = loss, minD, maxD
}

// Send enqueues a packet for delivery. The delivery callback only fires if the
// packet survives every loss stage and both endpoints are still up.
func (n *Network) Send(from, to int, deliver func(pkt Packet), pkt Packet) {
	if !n.alive[from] || !n.alive[to] || n.blocked[from] || n.blocked[to] {
		return
	}
	link := n.Link(from, to)
	loss := link.Loss + link.ExtraLoss + n.netLoss
	if loss > 0 && n.rng.Float64() < loss {
		return
	}
	delay := link.MinDelay
	if link.MaxDelay > link.MinDelay {
		delay += n.rng.Int63n(link.MaxDelay - link.MinDelay + 1)
	}
	if delay < 1 {
		delay = 1
	}
	n.sched.Schedule(delay, func() {
		// Re-check liveness and partition at actual delivery time.
		if !n.alive[from] || !n.alive[to] || n.blocked[from] || n.blocked[to] {
			return
		}
		deliver(pkt)
	})
}
