package kvstore_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/sAchin-680/raftkv/kvstore"
	"github.com/sAchin-680/raftkv/raft"
)

func apply(t *testing.T, s *kvstore.Store, index raft.Index, cmd []byte) kvstore.Result {
	t.Helper()
	res, err := s.Apply(raft.LogEntry{Index: index, Term: 1, Type: raft.EntryNormal, Command: cmd})
	if err != nil {
		t.Fatalf("Apply(%d): %v", index, err)
	}
	r, _ := res.(kvstore.Result)
	return r
}

func set(t *testing.T, key, value string) []byte {
	t.Helper()
	cmd, err := kvstore.SetCommand([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("SetCommand: %v", err)
	}
	return cmd
}

func TestSetAndGet(t *testing.T) {
	s := kvstore.New()
	apply(t, s, 1, set(t, "a", "1"))

	v, ok := s.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Errorf("Get(a) = %q, %v; want \"1\", true", v, ok)
	}
	if _, ok := s.Get([]byte("missing")); ok {
		t.Error("Get on an absent key reported it present")
	}
}

func TestSetOverwrites(t *testing.T) {
	s := kvstore.New()
	apply(t, s, 1, set(t, "a", "first"))
	apply(t, s, 2, set(t, "a", "second"))

	if v, _ := s.Get([]byte("a")); string(v) != "second" {
		t.Errorf("Get(a) = %q, want \"second\"", v)
	}
	if s.Len() != 1 {
		t.Errorf("Len = %d, want 1", s.Len())
	}
}

func TestDelete(t *testing.T) {
	s := kvstore.New()
	apply(t, s, 1, set(t, "a", "1"))

	cmd, err := kvstore.DeleteCommand([]byte("a"))
	if err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	apply(t, s, 2, cmd)

	if _, ok := s.Get([]byte("a")); ok {
		t.Error("the key survived a delete")
	}

	// Deleting something absent is not an error: a leader that replicated the
	// delete twice, or a node replaying its log, must both succeed.
	apply(t, s, 3, cmd)
}

func TestAppliedIndexTracksTheLog(t *testing.T) {
	s := kvstore.New()
	if got := s.AppliedIndex(); got != 0 {
		t.Errorf("AppliedIndex = %d on a fresh store, want 0", got)
	}

	apply(t, s, 7, set(t, "a", "1"))
	if got := s.AppliedIndex(); got != 7 {
		t.Errorf("AppliedIndex = %d, want 7 — a linearizable read needs this to "+
			"tell whether the state is new enough", got)
	}
}

// Stored values must not alias the caller's buffer, or a caller reusing a slice
// silently rewrites committed state.
func TestValuesAreCopiedInAndOut(t *testing.T) {
	s := kvstore.New()
	value := []byte("original")
	cmd, err := kvstore.SetCommand([]byte("a"), value)
	if err != nil {
		t.Fatalf("SetCommand: %v", err)
	}
	apply(t, s, 1, cmd)

	copy(value, "mutated!")
	if v, _ := s.Get([]byte("a")); string(v) != "original" {
		t.Errorf("stored value changed to %q when the caller reused its buffer", v)
	}

	got, _ := s.Get([]byte("a"))
	copy(got, "clobber!")
	if v, _ := s.Get([]byte("a")); string(v) != "original" {
		t.Errorf("stored value changed to %q when a reader mutated what Get returned", v)
	}
}

// Keys come back sorted rather than in map order, because callers compare them
// across nodes and Go randomizes map iteration.
func TestKeysAreSorted(t *testing.T) {
	s := kvstore.New()
	for i, k := range []string{"delta", "alpha", "charlie", "bravo"} {
		apply(t, s, raft.Index(i+1), set(t, k, "v"))
	}

	got := s.Keys()
	want := []string{"alpha", "bravo", "charlie", "delta"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys() = %v, want %v", got, want)
		}
	}
}

// An entry this build cannot parse is fatal, not skippable. Skipping it would
// leave this node's state machine different from every other node's, with
// nothing to detect the divergence.
func TestUndecodableCommandIsFatal(t *testing.T) {
	s := kvstore.New()
	_, err := s.Apply(raft.LogEntry{Index: 1, Term: 1, Command: []byte{0xff}})
	if err == nil {
		t.Error("a malformed command was accepted; a node that skips an entry " +
			"other nodes applied has silently diverged")
	}
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

func TestCommandRoundTrips(t *testing.T) {
	for _, want := range []kvstore.Command{
		{Op: kvstore.OpSet, Key: []byte("k"), Value: []byte("v")},
		{Op: kvstore.OpSet, Key: []byte("k"), Value: nil},
		{Op: kvstore.OpSet, Key: []byte("binary\x00key"), Value: []byte{0, 1, 2, 255}},
		{Op: kvstore.OpDelete, Key: []byte("k")},
	} {
		raw, err := kvstore.EncodeCommand(want)
		if err != nil {
			t.Fatalf("EncodeCommand(%v): %v", want, err)
		}
		got, err := kvstore.DecodeCommand(raw)
		if err != nil {
			t.Fatalf("DecodeCommand: %v", err)
		}
		if got.Op != want.Op {
			t.Errorf("op = %s, want %s", got.Op, want.Op)
		}
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("key = %q, want %q", got.Key, want.Key)
		}
		if want.Op == kvstore.OpSet && !bytes.Equal(got.Value, want.Value) {
			t.Errorf("value = %q, want %q", got.Value, want.Value)
		}
	}
}

func TestEmptyKeyIsRejected(t *testing.T) {
	if _, err := kvstore.SetCommand(nil, []byte("v")); err == nil {
		t.Error("an empty key should be refused at encode time, not silently stored")
	}
}

func TestTruncatedCommandIsAnError(t *testing.T) {
	raw := set(t, "key", "value")
	for _, n := range []int{0, 1, 2, len(raw) - 1} {
		if _, err := kvstore.DecodeCommand(raw[:n]); err == nil {
			t.Errorf("DecodeCommand on %d bytes succeeded, want an error", n)
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

func TestSnapshotRoundTrips(t *testing.T) {
	s := kvstore.New()
	for i := range 10 {
		apply(t, s, raft.Index(i+1), set(t, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)))
	}

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := kvstore.New()
	if err := restored.Restore(data, 10); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.Len() != s.Len() {
		t.Fatalf("restored %d keys, want %d", restored.Len(), s.Len())
	}
	if restored.AppliedIndex() != 10 {
		t.Errorf("restored applied index %d, want 10", restored.AppliedIndex())
	}
	for _, k := range s.Keys() {
		want, _ := s.Get([]byte(k))
		got, ok := restored.Get([]byte(k))
		if !ok || !bytes.Equal(got, want) {
			t.Errorf("restored %q = %q, want %q", k, got, want)
		}
	}
}

// Two stores holding the same data must produce byte-identical snapshots.
//
// Without this, identical replicas look different to anything that compares
// them — and an encoder that ranges over a Go map does not give it, because Go
// randomizes that order deliberately.
func TestSnapshotsAreDeterministic(t *testing.T) {
	build := func() *kvstore.Store {
		s := kvstore.New()
		// Inserted in different orders on purpose.
		for i, k := range []string{"z", "a", "m", "b", "q"} {
			apply(t, s, raft.Index(i+1), set(t, k, "value-"+k))
		}
		return s
	}

	first, err := build().Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	for range 20 {
		again, err := build().Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if !bytes.Equal(first, again) {
			t.Fatal("two stores with identical contents produced different snapshot " +
				"bytes; replicas would look divergent when they are not")
		}
	}
}

func TestRestoreReplacesExistingData(t *testing.T) {
	s := kvstore.New()
	apply(t, s, 1, set(t, "stale", "old"))

	source := kvstore.New()
	apply(t, source, 5, set(t, "fresh", "new"))
	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if err := s.Restore(data, 5); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := s.Get([]byte("stale")); ok {
		t.Error("restoring a snapshot left an older key behind; a snapshot is the " +
			"whole state, not an overlay")
	}
	if _, ok := s.Get([]byte("fresh")); !ok {
		t.Error("the restored key is missing")
	}
}
