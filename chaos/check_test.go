package chaos

import (
	"testing"
	"time"
)

// A checker that cannot fail is worse than none: it reports success forever and
// nobody looks again. Before any real history is trusted, the checker has to be
// shown to detect the violations it exists for.
//
// These build histories by hand — legal ones that must pass, and illegal ones
// that must fail — so a clean result on a real run means something.

// record builds a history from a literal sequence of completed operations.
func record(t *testing.T, ops ...Event) *History {
	t.Helper()
	h := NewHistory()
	for i, op := range ops {
		if op.Invoked == 0 {
			op.Invoked = int64(i*2 + 1)
		}
		if op.Returned == 0 && !op.Output.Unknown {
			op.Returned = int64(i*2 + 2)
		}
		op.ID = i
		h.events = append(h.events, op)
	}
	h.nextID = len(ops)
	return h
}

func put(client int, key, value string) Event {
	return Event{ClientID: client, Input: Input{Op: OpPut, Key: key, Value: value}}
}

func get(client int, key, value string) Event {
	return Event{
		ClientID: client,
		Input:    Input{Op: OpGet, Key: key},
		Output:   Output{Value: value, Found: true},
	}
}

func getMissing(client int, key string) Event {
	return Event{ClientID: client, Input: Input{Op: OpGet, Key: key}}
}

func check(t *testing.T, h *History) Result {
	t.Helper()
	return Check(h, 10*time.Second)
}

// ---------------------------------------------------------------------------
// Histories that must pass
// ---------------------------------------------------------------------------

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		get(0, "a", "1"),
		put(0, "a", "2"),
		get(0, "a", "2"),
	)
	if r := check(t, h); !r.OK() {
		t.Errorf("a plainly correct history was rejected: %s", r)
	}
}

// Two writes that overlap may land in either order, and a read afterwards may
// legally see either value.
func TestConcurrentWritesMayLandInEitherOrder(t *testing.T) {
	h := NewHistory()
	h.events = []Event{
		{ID: 0, ClientID: 0, Input: Input{Op: OpPut, Key: "a", Value: "x"}, Invoked: 10, Returned: 50},
		{ID: 1, ClientID: 1, Input: Input{Op: OpPut, Key: "a", Value: "y"}, Invoked: 20, Returned: 60},
		{ID: 2, ClientID: 2, Input: Input{Op: OpGet, Key: "a"},
			Output: Output{Value: "x", Found: true}, Invoked: 70, Returned: 80},
	}
	if r := check(t, h); !r.OK() {
		t.Errorf("concurrent writes ordered legally were rejected: %s", r)
	}
}

// Keys are independent, so a value written to one must never constrain a read
// of another. This is what the model's partitioning claims, checked.
func TestKeysAreIndependent(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		put(0, "b", "2"),
		get(0, "a", "1"),
		get(0, "b", "2"),
	)
	if r := check(t, h); !r.OK() {
		t.Errorf("independent keys were checked against one another: %s", r)
	}
}

// An operation whose outcome the client never learned may or may not have
// happened, and the checker must be free to decide either way.
func TestUnknownWriteMayOrMayNotHaveHappened(t *testing.T) {
	h := NewHistory()
	h.events = []Event{
		// This write timed out. The client does not know.
		{ID: 0, ClientID: 0, Input: Input{Op: OpPut, Key: "a", Value: "x"},
			Output: Output{Unknown: true}, Invoked: 10},
		// And the value turns up afterwards, so it evidently did happen.
		{ID: 1, ClientID: 1, Input: Input{Op: OpGet, Key: "a"},
			Output: Output{Value: "x", Found: true}, Invoked: 100, Returned: 110},
	}
	if r := check(t, h); !r.OK() {
		t.Errorf("a timed-out write that evidently committed was rejected: %s", r)
	}
}

func TestUnknownWriteThatNeverTookEffectIsAlsoFine(t *testing.T) {
	h := NewHistory()
	h.events = []Event{
		{ID: 0, ClientID: 0, Input: Input{Op: OpPut, Key: "a", Value: "x"},
			Output: Output{Unknown: true}, Invoked: 10},
		{ID: 1, ClientID: 1, Input: Input{Op: OpGet, Key: "a"}, Invoked: 100, Returned: 110},
	}
	if r := check(t, h); !r.OK() {
		t.Errorf("a timed-out write that evidently did not commit was rejected: %s", r)
	}
}

// ---------------------------------------------------------------------------
// Histories that must fail
// ---------------------------------------------------------------------------

// The violation this whole layer exists to catch: a read that completed after a
// write completed, returning the value from before it. That is a stale read, and
// it is exactly what a partitioned leader serves if it answers without
// confirming leadership.
func TestStaleReadIsRejected(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		put(0, "a", "2"),
		get(1, "a", "1"), // sees the old value, after the new one was written
	)
	r := check(t, h)
	if r.OK() {
		t.Error("a stale read was accepted: the read began after the write that " +
			"replaced its value had already completed, so no single-machine " +
			"ordering can produce it")
	}
	t.Logf("correctly rejected: %s", r)
}

// A write that completed and then vanished. This is what losing a committed
// entry looks like from outside — the failure the commit rule prevents.
func TestLostWriteIsRejected(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		getMissing(1, "a"),
	)
	if r := check(t, h); r.OK() {
		t.Error("a lost write was accepted: the key was written and then read " +
			"as absent, with nothing in between to remove it")
	}
}

// A value nobody ever wrote.
func TestFabricatedValueIsRejected(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		get(1, "a", "something-else"),
	)
	if r := check(t, h); r.OK() {
		t.Error("a read returned a value no client ever wrote, and it was accepted")
	}
}

// Delete reports whether it removed anything, so that answer has to be
// consistent too. It is the one place a duplicated write becomes visible even
// though the map itself looks correct — which is why client sessions exist.
func TestInconsistentDeleteAnswerIsRejected(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		Event{ClientID: 0, Input: Input{Op: OpDelete, Key: "a"},
			Output: Output{Found: true}},
		Event{ClientID: 0, Input: Input{Op: OpDelete, Key: "a"},
			Output: Output{Found: true}}, // claims to have removed it again
	)
	if r := check(t, h); r.OK() {
		t.Error("a delete claimed to remove a key that was already gone, and it " +
			"was accepted; a retried delete answering differently is how a " +
			"duplicated write shows up in a history")
	}
}

// A read that observes a write which had not yet been invoked.
func TestReadFromTheFutureIsRejected(t *testing.T) {
	h := NewHistory()
	h.events = []Event{
		{ID: 0, ClientID: 0, Input: Input{Op: OpGet, Key: "a"},
			Output: Output{Value: "later", Found: true}, Invoked: 10, Returned: 20},
		{ID: 1, ClientID: 1, Input: Input{Op: OpPut, Key: "a", Value: "later"},
			Invoked: 30, Returned: 40},
	}
	if r := check(t, h); r.OK() {
		t.Error("a read returned a value whose write had not yet been invoked")
	}
}

// ---------------------------------------------------------------------------
// The history recorder itself
// ---------------------------------------------------------------------------

func TestUnknownOperationsStayInTheHistory(t *testing.T) {
	h := NewHistory()
	p := h.Begin(0, Input{Op: OpPut, Key: "a", Value: "x"})
	p.Unknown()

	events := h.Events()
	if len(events) != 1 {
		t.Fatalf("%d events recorded, want 1 — an operation whose outcome was "+
			"never learned must stay in the history, because it may have "+
			"happened", len(events))
	}
	if events[0].Completed() {
		t.Error("an unknown operation was marked completed")
	}
	if events[0].Returned != 0 {
		t.Error("an unknown operation was given a return time")
	}
}

func TestCompletingTwiceIsIgnored(t *testing.T) {
	h := NewHistory()
	p := h.Begin(0, Input{Op: OpGet, Key: "a"})
	p.Complete(Output{Value: "1", Found: true})
	p.Complete(Output{Value: "2", Found: true})
	p.Unknown()

	if got := h.Len(); got != 1 {
		t.Errorf("%d events recorded, want 1", got)
	}
	if v := h.Events()[0].Output.Value; v != "1" {
		t.Errorf("recorded %q, want the first answer %q", v, "1")
	}
}

func TestStatsCountEverything(t *testing.T) {
	h := record(t,
		put(0, "a", "1"),
		get(0, "a", "1"),
		Event{ClientID: 0, Input: Input{Op: OpDelete, Key: "b"}},
	)
	h.events = append(h.events, Event{
		ID: 9, ClientID: 1, Input: Input{Op: OpPut, Key: "c"},
		Output: Output{Unknown: true}, Invoked: 99,
	})

	s := h.Stats()
	if s.Total != 4 || s.Puts != 2 || s.Gets != 1 || s.Deletes != 1 {
		t.Errorf("stats = %+v", s)
	}
	if s.Unknown != 1 || s.Completed != 3 {
		t.Errorf("completed/unknown = %d/%d, want 3/1", s.Completed, s.Unknown)
	}
	if s.Keys != 3 {
		t.Errorf("keys = %d, want 3", s.Keys)
	}
}
