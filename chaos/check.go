package chaos

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// The model: a single map, one operation at a time.
//
// Linearizability asks whether the answers clients got could have come from one
// machine executing their operations one at a time, in *some* order consistent
// with real time — an operation that finished before another started must come
// first, and concurrent ones may go in either order.
//
// Porcupine searches for such an order. This supplies the rules it searches
// against: what each operation does to the state, and what it should therefore
// have returned. Writing the checker by hand was the alternative and would have
// been worse: a home-grown checker is itself unverified, so a clean result would
// mean nothing.

type modelState struct {
	value string
	found bool
}

// kvModel is the model for a single key.
//
// One key rather than the whole map, because Porcupine's search is exponential
// in the number of concurrent operations and keys are independent: no operation
// on key A can affect the answer to an operation on key B. Partitioning the
// history by key turns one intractable search into many small ones. On a
// ten-thousand-operation history this is the difference between seconds and
// never finishing.
var kvModel = porcupine.Model{
	// Split the history into one independent sub-history per key.
	//
	// Without this the single-key model above would be applied to the whole
	// history, checking every key's operations against one shared value — which
	// would report violations that are not violations, and is wrong rather than
	// merely slow.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(Input).Key
			byKey[key] = append(byKey[key], op)
		}
		// Sorted, so a failure reports the same partition every run.
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},

	Init: func() any { return modelState{} },

	Step: func(state, input, output any) (bool, any) {
		st := state.(modelState)
		in := input.(Input)
		out := output.(Output)

		switch in.Op {
		case OpGet:
			// A read changes nothing and must report what is there.
			return out.Found == st.found && (!st.found || out.Value == st.value), st

		case OpPut:
			return true, modelState{value: in.Value, found: true}

		case OpDelete:
			// Delete reports whether it removed anything, so that answer is
			// part of what has to be consistent — it is the one place a
			// duplicated write becomes visible even though the map looks right.
			if !out.Unknown && out.Found != st.found {
				return false, st
			}
			return true, modelState{}

		default:
			return false, st
		}
	},

	Equal: func(a, b any) bool {
		x, y := a.(modelState), b.(modelState)
		return x.found == y.found && x.value == y.value
	},

	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		out := output.(Output)
		switch in.Op {
		case OpGet:
			return fmt.Sprintf("get(%s) -> %s", in.Key, out)
		case OpPut:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		case OpDelete:
			return fmt.Sprintf("delete(%s) -> existed=%t", in.Key, out.Found)
		default:
			return "unknown"
		}
	},

	DescribeState: func(state any) string {
		st := state.(modelState)
		if !st.found {
			return "<nil>"
		}
		return fmt.Sprintf("%q", st.value)
	},
}

// Result is the outcome of checking a history.
type Result struct {
	Status  porcupine.CheckResult
	Stats   Stats
	Elapsed time.Duration
	Timeout time.Duration
	Info    porcupine.LinearizationInfo
}

// OK reports whether the history is linearizable.
func (r Result) OK() bool { return r.Status == porcupine.Ok }

func (r Result) String() string {
	switch r.Status {
	case porcupine.Ok:
		return fmt.Sprintf("linearizable — %s, checked in %s",
			r.Stats, r.Elapsed.Round(time.Millisecond))
	case porcupine.Illegal:
		return fmt.Sprintf("NOT LINEARIZABLE — %s, found in %s",
			r.Stats, r.Elapsed.Round(time.Millisecond))
	default:
		return fmt.Sprintf("UNKNOWN — the search did not finish within %s. "+
			"That is not a pass: it means the history was too large or too "+
			"concurrent to decide, and it has to be re-run smaller before the "+
			"result means anything", r.Timeout)
	}
}

// Check decides whether a history is linearizable.
//
// The timeout matters, and an exhausted one must never be read as success. A
// history that could not be decided in time has not been shown to be legal, and
// treating "I gave up" as "it passed" is how a checker becomes decorative.
func Check(h *History, timeout time.Duration) Result {
	events := h.Events()
	ops := toPorcupine(events)

	began := time.Now()
	status, info := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)

	return Result{
		Status:  status,
		Stats:   h.Stats(),
		Elapsed: time.Since(began),
		Timeout: timeout,
		Info:    info,
	}
}

// WriteVisualization writes an interactive report of a failing history.
//
// Worth doing for a failure and not much else: it shows every operation on a
// timeline and marks the point at which no legal ordering remained, which is
// the difference between "the check failed" and knowing which write went
// missing.
func (r Result) WriteVisualization(path string) error {
	f, err := os.Create(path) //nolint:gosec // a path the operator chose
	if err != nil {
		return fmt.Errorf("chaos: creating %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	if err := porcupine.Visualize(kvModel, r.Info, f); err != nil {
		return fmt.Errorf("chaos: writing visualization: %w", err)
	}
	return nil
}

// toPorcupine converts a recorded history into Porcupine's event form.
//
// The event form rather than the operation form, because an operation whose
// outcome the client never learned has an invocation and no return, and only
// the event form can express that. Dropping those operations instead — the
// obvious shortcut — removes exactly the writes most likely to be involved in a
// violation, since a timeout usually means a leader change, and would let an
// illegal history check clean.
func toPorcupine(events []Event) []porcupine.Operation {
	// Porcupine's Operation form needs a return time for every operation. An
	// operation that never returned is given one past the end of the history,
	// which expresses the same thing: it could have taken effect at any point
	// from its invocation onwards, including after everything else.
	var end int64
	for _, e := range events {
		if e.Returned > end {
			end = e.Returned
		}
		if e.Invoked > end {
			end = e.Invoked
		}
	}
	end++

	ops := make([]porcupine.Operation, 0, len(events))
	for _, e := range events {
		returned := e.Returned
		if !e.Completed() {
			returned = end
		}
		ops = append(ops, porcupine.Operation{
			ClientId: e.ClientID,
			Input:    e.Input,
			Output:   e.Output,
			Call:     e.Invoked,
			Return:   returned,
		})
	}
	return ops
}

// PartitionByKey is how the model keeps the search tractable. Exposed so a
// caller can see there is no cross-key interaction being assumed away: two
// operations on different keys genuinely cannot affect each other's answers.
func PartitionByKey(events []Event) map[string][]Event {
	out := map[string][]Event{}
	for _, e := range events {
		out[e.Input.Key] = append(out[e.Input.Key], e)
	}
	return out
}
