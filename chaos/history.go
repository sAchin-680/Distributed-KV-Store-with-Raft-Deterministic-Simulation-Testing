// Package chaos records what clients did to a cluster and checks the result is
// linearizable.
//
// This is a different kind of verification from the simulator, and it exists
// because the simulator cannot do it.
//
// The simulator proves the *algorithm* is safe under any interleaving it can
// generate, by reading each node's internal state after every event. It says
// nothing about whether the real implementation is right: it replaces gRPC with
// an in-memory queue, bbolt with a map, and the Go scheduler with a
// single-threaded loop. Bugs in any of those are invisible to it.
//
// What this checks instead is the only thing a user can actually observe — the
// sequence of operations clients issued and the answers they got — against the
// question "could a single machine, executing these one at a time, have produced
// these answers?". If not, something between the client and the state machine is
// wrong, and it does not matter which layer.
//
// The two are complementary, and neither replaces the other. The simulator can
// replay any failure it finds; this cannot, because the faults are real and the
// scheduling is the kernel's. What this can see is everything the simulator's
// stand-ins hide.
package chaos

import (
	"fmt"
	"sync"
	"time"
)

// OpKind is the operation a client performed.
type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpDelete
)

func (o OpKind) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown-op(%d)", uint8(o))
	}
}

// Input is what a client asked for.
type Input struct {
	Op    OpKind
	Key   string
	Value string
}

// Output is what it was told.
type Output struct {
	Value string
	Found bool

	// Unknown marks an operation whose outcome the client never learned: a
	// timeout, a connection failure, a leader change mid-write.
	//
	// These are not failures to be discarded. An operation that timed out may
	// have taken effect anyway — the answer was lost, not necessarily the
	// command — so a checker that dropped them would be checking a history with
	// holes in it and could call an illegal execution legal. They are recorded
	// as operations that were *invoked* and never *returned*, which is exactly
	// what the checker needs in order to consider both possibilities.
	Unknown bool
}

func (o Output) String() string {
	switch {
	case o.Unknown:
		return "<unknown>"
	case !o.Found:
		return "<nil>"
	default:
		return fmt.Sprintf("%q", o.Value)
	}
}

// Event is one end of one operation.
type Event struct {
	ClientID int
	// ID pairs an invocation with its completion.
	ID int

	Input  Input
	Output Output

	// Invoked and Returned are monotonic nanosecond timestamps. Returned is
	// zero for an operation that never completed.
	Invoked  int64
	Returned int64
}

// Completed reports whether the client learned the outcome.
func (e Event) Completed() bool { return e.Returned != 0 && !e.Output.Unknown }

// History is the record of everything clients did.
//
// Times come from a monotonic clock, and that matters more than it looks.
// Linearizability is defined in terms of real-time ordering — an operation that
// completed before another began must appear to happen first — so the history is
// only meaningful if its timestamps are comparable. A wall clock that steps
// backwards mid-run, which NTP will happily do, would invent orderings that
// never existed and produce a violation that is entirely the clock's fault.
type History struct {
	mu     sync.Mutex
	events []Event
	nextID int
	origin time.Time
}

// NewHistory returns an empty history anchored at the current instant.
func NewHistory() *History {
	return &History{origin: time.Now()}
}

// Begin records an invocation and returns a handle to complete it with.
func (h *History) Begin(clientID int, in Input) *Pending {
	h.mu.Lock()
	id := h.nextID
	h.nextID++
	h.mu.Unlock()

	return &Pending{
		history:  h,
		clientID: clientID,
		id:       id,
		input:    in,
		invoked:  time.Since(h.origin).Nanoseconds(),
	}
}

// Pending is an operation in flight.
type Pending struct {
	history  *History
	clientID int
	id       int
	input    Input
	invoked  int64
	done     bool
}

// Complete records the answer the client received.
func (p *Pending) Complete(out Output) {
	if p.done {
		return
	}
	p.done = true

	p.history.mu.Lock()
	defer p.history.mu.Unlock()
	p.history.events = append(p.history.events, Event{
		ClientID: p.clientID,
		ID:       p.id,
		Input:    p.input,
		Output:   out,
		Invoked:  p.invoked,
		Returned: time.Since(p.history.origin).Nanoseconds(),
	})
}

// Unknown records an operation whose outcome the client never learned.
//
// Deliberately not the same as failing it. The operation stays in the history
// as invoked-but-never-returned, so the checker is free to place it anywhere
// after its invocation — including "it took effect" — which is the truth.
func (p *Pending) Unknown() {
	if p.done {
		return
	}
	p.done = true

	p.history.mu.Lock()
	defer p.history.mu.Unlock()
	p.history.events = append(p.history.events, Event{
		ClientID: p.clientID,
		ID:       p.id,
		Input:    p.input,
		Output:   Output{Unknown: true},
		Invoked:  p.invoked,
	})
}

// HistoryFromEvents rebuilds a history that was written out earlier.
//
// The saved history is the evidence a run produced. Being able to decide it
// again later matters because an inconclusive search is not a result: it has to
// be settled with more time, and re-running the experiment instead would
// produce a different history and answer a different question.
func HistoryFromEvents(events []Event) *History {
	h := NewHistory()
	h.events = append(h.events, events...)
	for _, e := range events {
		if e.ID >= h.nextID {
			h.nextID = e.ID + 1
		}
	}
	return h
}

// Events returns a copy of the history, ordered by invocation time.
func (h *History) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]Event, len(h.events))
	copy(out, h.events)
	sortByInvocation(out)
	return out
}

// Len is how many operations were recorded.
func (h *History) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.events)
}

// Stats summarizes a history.
type Stats struct {
	Total     int
	Completed int
	Unknown   int
	Gets      int
	Puts      int
	Deletes   int
	Keys      int
	Duration  time.Duration
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"%d operations over %s (%d get, %d put, %d delete) across %d keys; "+
			"%d completed, %d outcome unknown",
		s.Total, s.Duration.Round(time.Millisecond),
		s.Gets, s.Puts, s.Deletes, s.Keys, s.Completed, s.Unknown)
}

// Stats summarizes what was recorded.
func (h *History) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()

	var s Stats
	keys := map[string]bool{}
	var last int64

	for _, e := range h.events {
		s.Total++
		keys[e.Input.Key] = true
		switch e.Input.Op {
		case OpGet:
			s.Gets++
		case OpPut:
			s.Puts++
		case OpDelete:
			s.Deletes++
		}
		if e.Output.Unknown {
			s.Unknown++
		} else {
			s.Completed++
		}
		if e.Returned > last {
			last = e.Returned
		}
	}
	s.Keys = len(keys)
	s.Duration = time.Duration(last)
	return s
}

func sortByInvocation(events []Event) {
	// Insertion sort: histories arrive very nearly in order already, because
	// they are appended as operations complete.
	for i := 1; i < len(events); i++ {
		for j := i; j > 0 && events[j].Invoked < events[j-1].Invoked; j-- {
			events[j], events[j-1] = events[j-1], events[j]
		}
	}
}
