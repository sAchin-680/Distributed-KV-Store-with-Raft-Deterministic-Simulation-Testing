package sim

import (
	"container/heap"
	"fmt"

	"github.com/sAchin-680/raftkv/raft"
)

// eventKind is what an event does when it fires.
type eventKind uint8

const (
	evTick eventKind = iota
	evDeliver
	evCrash
	evRestart
	evPartition
	evHeal
	evClientWrite
)

func (k eventKind) String() string {
	switch k {
	case evTick:
		return "tick"
	case evDeliver:
		return "deliver"
	case evCrash:
		return "crash"
	case evRestart:
		return "restart"
	case evPartition:
		return "partition"
	case evHeal:
		return "heal"
	case evClientWrite:
		return "write"
	default:
		return fmt.Sprintf("unknown-event(%d)", uint8(k))
	}
}

// event is one thing that happens at one instant of virtual time.
type event struct {
	at   int64 // virtual milliseconds since the run began
	seq  uint64
	kind eventKind

	node raft.NodeID
	msg  raft.Message

	// loseDisk marks a restart that comes back with no persisted state, which
	// models a replaced machine rather than a reboot.
	loseDisk bool
}

// eventQueue orders events by time, then by insertion sequence.
//
// The sequence tiebreaker is not a detail. Go's heap gives no guarantee about
// the order of equal elements, and events scheduled for the same millisecond are
// routine — every node's first tick, for one. Without a total order the same
// seed would produce different executions on different runs, and seed replay,
// which is the entire point of this package, would silently stop working.
type eventQueue struct {
	items []*event
	next  uint64
}

func newEventQueue() *eventQueue {
	q := &eventQueue{}
	heap.Init(q)
	return q
}

func (q *eventQueue) Len() int { return len(q.items) }

func (q *eventQueue) Less(i, j int) bool {
	if q.items[i].at != q.items[j].at {
		return q.items[i].at < q.items[j].at
	}
	return q.items[i].seq < q.items[j].seq
}

func (q *eventQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

func (q *eventQueue) Push(x any) { q.items = append(q.items, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := q.items
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	q.items = old[:n-1]
	return e
}

// schedule adds an event, assigning it the next sequence number.
func (q *eventQueue) schedule(e *event) {
	e.seq = q.next
	q.next++
	heap.Push(q, e)
}

// pop returns the earliest event, or nil when the queue is empty.
func (q *eventQueue) pop() *event {
	if len(q.items) == 0 {
		return nil
	}
	return heap.Pop(q).(*event)
}
