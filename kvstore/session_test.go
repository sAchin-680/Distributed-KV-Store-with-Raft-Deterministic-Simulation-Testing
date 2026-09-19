package kvstore_test

import (
	"fmt"
	"testing"

	"github.com/sAchin-680/raftkv/kvstore"
	"github.com/sAchin-680/raftkv/raft"
)

func sessionSet(t *testing.T, clientID, seq uint64, key, value string) []byte {
	t.Helper()
	cmd, err := kvstore.SessionSetCommand(clientID, seq, []byte(key), []byte(value))
	if err != nil {
		t.Fatalf("SessionSetCommand: %v", err)
	}
	return cmd
}

func sessionDelete(t *testing.T, clientID, seq uint64, key string) []byte {
	t.Helper()
	cmd, err := kvstore.SessionDeleteCommand(clientID, seq, []byte(key))
	if err != nil {
		t.Fatalf("SessionDeleteCommand: %v", err)
	}
	return cmd
}

// Raft applies a command at least once, not exactly once. A client whose
// request times out cannot tell whether it committed, retries, and the same
// command reaches the log twice.
//
// For Set the map ends up right either way. The *history* does not: a write
// that completed once appears twice, and that history is not linearizable even
// though the data looks fine. Phase three checks exactly that history.
func TestRetriedWriteIsAppliedOnce(t *testing.T) {
	s := kvstore.New()
	const client, seq = 7, 1

	first := apply(t, s, 1, sessionSet(t, client, seq, "k", "v"))
	if first.Duplicate {
		t.Error("the first application was reported as a duplicate")
	}

	// The same command again, at a different log index — which is exactly what
	// a retry produces.
	second := apply(t, s, 2, sessionSet(t, client, seq, "k", "v"))
	if !second.Duplicate {
		t.Error("a retried command was applied again instead of being recognized")
	}
}

// The case where duplication is visibly wrong, not just theoretically.
//
// Delete reports whether it removed anything. Applied twice, the retry would
// answer "no such key" for a delete that did remove one — a different answer to
// the same request.
func TestRetriedDeleteReplaysItsOriginalAnswer(t *testing.T) {
	s := kvstore.New()
	const client = 9

	apply(t, s, 1, sessionSet(t, client, 1, "k", "v"))

	first := apply(t, s, 2, sessionDelete(t, client, 2, "k"))
	if !first.Existed {
		t.Fatal("the first delete should have reported the key existed")
	}

	second := apply(t, s, 3, sessionDelete(t, client, 2, "k"))
	if !second.Existed {
		t.Error("the retry answered that the key did not exist; without " +
			"deduplication a client sees a different answer to the same request")
	}
	if !second.Duplicate {
		t.Error("the retry was not recognized as one")
	}
}

// A stale retry of an older request must not be re-applied either, or a
// long-delayed duplicate overwrites newer data.
func TestOutOfOrderRetryIsAlsoDeduplicated(t *testing.T) {
	s := kvstore.New()
	const client = 3

	apply(t, s, 1, sessionSet(t, client, 1, "k", "first"))
	apply(t, s, 2, sessionSet(t, client, 2, "k", "second"))

	// The original attempt at sequence 1 finally arrives.
	replay := apply(t, s, 3, sessionSet(t, client, 1, "k", "first"))
	if !replay.Duplicate {
		t.Error("an out-of-order retry was applied")
	}
	if v, _ := s.Get([]byte("k")); string(v) != "second" {
		t.Errorf("value is %q, want \"second\" — a late duplicate overwrote newer data", v)
	}
}

func TestDifferentClientsAreIndependent(t *testing.T) {
	s := kvstore.New()

	apply(t, s, 1, sessionSet(t, 1, 1, "a", "from-one"))
	// Same sequence number, different client. Not a duplicate.
	result := apply(t, s, 2, sessionSet(t, 2, 1, "b", "from-two"))

	if result.Duplicate {
		t.Error("a different client's request was treated as a retry")
	}
	if s.Len() != 2 {
		t.Errorf("%d keys, want 2", s.Len())
	}
}

func TestWritesWithoutASessionAreNeverDeduplicated(t *testing.T) {
	s := kvstore.New()

	apply(t, s, 1, set(t, "k", "v"))
	result := apply(t, s, 2, set(t, "k", "v"))

	if result.Duplicate {
		t.Error("a write with no client id was deduplicated; there is nothing " +
			"to deduplicate it against")
	}
	if s.Sessions() != 0 {
		t.Errorf("%d sessions recorded for sessionless writes, want 0", s.Sessions())
	}
}

// Sessions are bounded, or a long-running cluster leaks memory in proportion to
// how many clients have ever connected.
func TestSessionsAreBounded(t *testing.T) {
	const limit = 8
	s := kvstore.NewWithSessionLimit(limit)

	for i := range 100 {
		apply(t, s, raft.Index(i+1), sessionSet(t, uint64(i+1), 1, fmt.Sprintf("k-%d", i), "v"))
	}

	if got := s.Sessions(); got > limit {
		t.Errorf("%d sessions retained, limit is %d", got, limit)
	}
	// The most recent clients are the ones worth keeping.
	if got := s.Sessions(); got != limit {
		t.Errorf("%d sessions retained, want exactly %d", got, limit)
	}
}

// Eviction changes state machine state, so every node must evict exactly the
// same sessions. Choosing by wall-clock time, or by ranging a Go map, would not
// be reproducible — and the divergence would be invisible until two replicas
// were compared.
func TestSessionEvictionIsDeterministic(t *testing.T) {
	build := func() *kvstore.Store {
		s := kvstore.NewWithSessionLimit(4)
		for i := range 40 {
			apply(t, s, raft.Index(i+1),
				sessionSet(t, uint64(i+1), 1, fmt.Sprintf("k-%d", i), "v"))
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
		if string(first) != string(again) {
			t.Fatal("two identical command sequences produced different state; " +
				"session eviction is not deterministic and replicas would diverge")
		}
	}
}

func TestSessionFieldsRoundTrip(t *testing.T) {
	raw := sessionSet(t, 1<<40, 1<<30, "key", "value")
	cmd, err := kvstore.DecodeCommand(raw)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if cmd.ClientID != 1<<40 {
		t.Errorf("client id = %d, want %d", cmd.ClientID, uint64(1<<40))
	}
	if cmd.Sequence != 1<<30 {
		t.Errorf("sequence = %d, want %d", cmd.Sequence, uint64(1<<30))
	}
}
