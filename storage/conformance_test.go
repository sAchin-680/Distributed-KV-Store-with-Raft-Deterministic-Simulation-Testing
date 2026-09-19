package storage_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/storage"
)

// One contract, two implementations.
//
// The simulator runs ten thousand randomized cluster-hours against the in-memory
// store and reports no safety violations. That result is worth exactly as much
// as the claim that the in-memory store behaves like the real one. If it is more
// forgiving anywhere — accepting an append the real store rejects, returning a
// different error for a compacted index — then the simulation is validating
// something the production system does not do.
//
// So both run the same suite, and it is written against the raft.Storage
// interface rather than either type.

type factory struct {
	name string
	open func(t *testing.T) raft.Storage
}

func factories() []factory {
	return []factory{
		{
			name: "memory",
			open: func(*testing.T) raft.Storage { return raft.NewMemoryStorage() },
		},
		{
			name: "bolt",
			open: func(t *testing.T) raft.Storage {
				t.Helper()
				opts := storage.DefaultOptions()
				// Tests would otherwise spend their time in the kernel. The
				// fsync path itself is exercised separately, below.
				opts.NoSync = true
				db, err := storage.Open(filepath.Join(t.TempDir(), "raft.db"), opts)
				if err != nil {
					t.Fatalf("opening bolt store: %v", err)
				}
				t.Cleanup(func() { _ = db.Close() })
				return db
			},
		},
	}
}

// eachStorage runs fn against every implementation.
func eachStorage(t *testing.T, fn func(t *testing.T, s raft.Storage)) {
	t.Helper()
	for _, f := range factories() {
		t.Run(f.name, func(t *testing.T) { fn(t, f.open(t)) })
	}
}

func entries(spec ...[2]uint64) []raft.LogEntry {
	out := make([]raft.LogEntry, len(spec))
	for i, s := range spec {
		out[i] = raft.LogEntry{
			Index:   raft.Index(s[0]),
			Term:    raft.Term(s[1]),
			Command: fmt.Appendf(nil, "cmd-%d", s[0]),
		}
	}
	return out
}

func mustAppend(t *testing.T, s raft.Storage, ents []raft.LogEntry) {
	t.Helper()
	if err := s.AppendEntries(ents); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

func TestEmptyStoreBounds(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		if got := s.FirstIndex(); got != 1 {
			t.Errorf("FirstIndex = %d, want 1", got)
		}
		if got := s.LastIndex(); got != 0 {
			t.Errorf("LastIndex = %d, want 0 — the log is 1-based and empty", got)
		}

		hs, snap, err := s.InitialState()
		if err != nil {
			t.Fatalf("InitialState: %v", err)
		}
		if !hs.IsEmpty() || !snap.IsEmpty() {
			t.Errorf("a store that has never run returned %s and %s", hs, snap)
		}
	})
}

func TestBoundsAfterAppend(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}))

		if got := s.FirstIndex(); got != 1 {
			t.Errorf("FirstIndex = %d, want 1", got)
		}
		if got := s.LastIndex(); got != 3 {
			t.Errorf("LastIndex = %d, want 3", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

func TestGetEntryRoundTrips(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		want := raft.LogEntry{
			Index: 1, Term: 4, Type: raft.EntryConfChange,
			Command: []byte("membership"),
		}
		mustAppend(t, s, []raft.LogEntry{want})

		got, err := s.GetEntry(1)
		if err != nil {
			t.Fatalf("GetEntry: %v", err)
		}
		if got.Index != want.Index || got.Term != want.Term ||
			got.Type != want.Type || string(got.Command) != string(want.Command) {
			t.Errorf("round trip changed the entry:\n got %+v\nwant %+v", got, want)
		}
	})
}

func TestReadingOutOfRange(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries([2]uint64{1, 1}, [2]uint64{2, 1}))

		if _, err := s.GetEntry(9); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("GetEntry(9) = %v, want ErrUnavailable", err)
		}
		if _, err := s.Term(9); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("Term(9) = %v, want ErrUnavailable", err)
		}
		if _, err := s.Entries(1, 9); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("Entries(1,9) = %v, want ErrUnavailable", err)
		}
	})
}

func TestEntriesReturnsHalfOpenRange(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries(
			[2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}, [2]uint64{4, 2}))

		got, err := s.Entries(2, 4)
		if err != nil {
			t.Fatalf("Entries: %v", err)
		}
		if len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
			t.Fatalf("Entries(2,4) returned %v, want indexes 2 and 3", got)
		}

		if got, err := s.Entries(3, 3); err != nil || len(got) != 0 {
			t.Errorf("Entries(3,3) = %v, %v; want an empty range", got, err)
		}
	})
}

// ---------------------------------------------------------------------------
// Appending
// ---------------------------------------------------------------------------

// Overwriting a divergent tail is the normal mechanism for bringing a follower
// back into agreement with a leader, not an error condition.
func TestAppendTruncatesConflictingSuffix(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries(
			[2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}, [2]uint64{4, 1}))

		mustAppend(t, s, entries([2]uint64{3, 5}))

		if got := s.LastIndex(); got != 3 {
			t.Errorf("LastIndex = %d, want 3 — indexes past the overwrite must be gone", got)
		}
		if term, err := s.Term(3); err != nil || term != 5 {
			t.Errorf("Term(3) = %d, %v; want 5, nil", term, err)
		}
		if _, err := s.GetEntry(4); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("GetEntry(4) = %v, want ErrUnavailable", err)
		}
	})
}

func TestAppendRejectsAGap(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries([2]uint64{1, 1}))

		if err := s.AppendEntries(entries([2]uint64{5, 1})); err == nil {
			t.Error("appending at index 5 after index 1 should be refused: a log " +
				"with a hole cannot satisfy the log matching property")
		}
	})
}

func TestAppendRejectsNonContiguousBatch(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		if err := s.AppendEntries(entries([2]uint64{1, 1}, [2]uint64{3, 1})); err == nil {
			t.Error("a batch with a hole in it should be refused")
		}
	})
}

func TestAppendEmptyIsANoOp(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries([2]uint64{1, 1}))
		if err := s.AppendEntries(nil); err != nil {
			t.Errorf("AppendEntries(nil) = %v, want nil", err)
		}
		if got := s.LastIndex(); got != 1 {
			t.Errorf("LastIndex = %d, want 1", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Hard state
// ---------------------------------------------------------------------------

func TestHardStateRoundTrips(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		want := raft.HardState{Term: 7, Vote: 3, Commit: 42}
		if err := s.SetHardState(want); err != nil {
			t.Fatalf("SetHardState: %v", err)
		}

		got, _, err := s.InitialState()
		if err != nil {
			t.Fatalf("InitialState: %v", err)
		}
		if got != want {
			t.Errorf("hard state round trip: got %s, want %s", got, want)
		}
	})
}

// Terms only ever increase. A store that accepted a lower one would let a node
// go back in time, forget it had voted, and vote a second time in one term.
func TestHardStateTermNeverGoesBackwards(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		if err := s.SetHardState(raft.HardState{Term: 5, Vote: 1}); err != nil {
			t.Fatalf("SetHardState: %v", err)
		}
		if err := s.SetHardState(raft.HardState{Term: 3, Vote: 2}); err == nil {
			t.Error("accepting a lower term would let a node forget a vote it cast")
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshots and compaction
// ---------------------------------------------------------------------------

func TestSnapshotRoundTrips(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		want := raft.Snapshot{
			LastIncludedIndex: 10,
			LastIncludedTerm:  3,
			Config:            raft.NewConfiguration([]raft.NodeID{1, 2, 3}, 4),
			Data:              []byte("state machine"),
		}
		if err := s.SaveSnapshot(want); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		got, err := s.LoadSnapshot()
		if err != nil {
			t.Fatalf("LoadSnapshot: %v", err)
		}
		if got.LastIncludedIndex != want.LastIncludedIndex ||
			got.LastIncludedTerm != want.LastIncludedTerm ||
			string(got.Data) != string(want.Data) {
			t.Errorf("snapshot round trip: got %s, want %s", got, want)
		}
		// The configuration has to survive: a node restoring from a snapshot has
		// discarded the entries that established its membership, so without it
		// the node comes back not knowing who its peers are.
		if !got.Config.Equal(want.Config) {
			t.Errorf("configuration lost in round trip: got %s, want %s",
				got.Config, want.Config)
		}
	})
}

func TestSnapshotMovesTheLogFloor(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		if err := s.SaveSnapshot(raft.Snapshot{
			LastIncludedIndex: 10, LastIncludedTerm: 3,
			Config: raft.NewConfiguration([]raft.NodeID{1}),
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}

		if got := s.FirstIndex(); got != 11 {
			t.Errorf("FirstIndex = %d, want 11", got)
		}
		if got := s.LastIndex(); got != 10 {
			t.Errorf("LastIndex = %d, want 10", got)
		}
		// The boundary index must answer even though the entry is gone — an
		// AppendEntries consistency check can reach back to exactly here.
		if term, err := s.Term(10); err != nil || term != 3 {
			t.Errorf("Term(10) = %d, %v; want 3, nil at the snapshot boundary", term, err)
		}
		if _, err := s.Term(9); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("Term(9) = %v, want ErrCompacted", err)
		}
	})
}

func TestStaleSnapshotIsRejected(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		snap := raft.Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 3}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		older := raft.Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 2}
		if err := s.SaveSnapshot(older); !errors.Is(err, raft.ErrSnapshotOutOfDate) {
			t.Errorf("SaveSnapshot(older) = %v, want ErrSnapshotOutOfDate", err)
		}
	})
}

func TestCompactDiscardsThePrefix(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries(
			[2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2},
			[2]uint64{4, 2}, [2]uint64{5, 3}))

		if err := s.Compact(3); err != nil {
			t.Fatalf("Compact: %v", err)
		}

		if got := s.FirstIndex(); got != 4 {
			t.Errorf("FirstIndex = %d, want 4", got)
		}
		if got := s.LastIndex(); got != 5 {
			t.Errorf("LastIndex = %d, want 5 — compaction must not touch the tail", got)
		}
		if term, err := s.Term(3); err != nil || term != 2 {
			t.Errorf("Term(3) = %d, %v; want 2, nil at the compaction boundary", term, err)
		}
		if _, err := s.GetEntry(2); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("GetEntry(2) = %v, want ErrCompacted", err)
		}
		if _, err := s.Entries(1, 5); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("Entries(1,5) = %v, want ErrCompacted", err)
		}
	})
}

func TestCompactPastLastIndexIsRefused(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries([2]uint64{1, 1}, [2]uint64{2, 1}))
		if err := s.Compact(9); err == nil {
			t.Error("compacting past the end of the log should be refused")
		}
	})
}

// A leader may resend entries a follower has already folded into a snapshot.
// Silently ignoring them is correct; erroring would stall replication.
func TestAppendBelowSnapshotBoundaryIsIgnored(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		mustAppend(t, s, entries(
			[2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}, [2]uint64{4, 1}))
		if err := s.Compact(2); err != nil {
			t.Fatalf("Compact: %v", err)
		}

		if err := s.AppendEntries(entries([2]uint64{1, 1}, [2]uint64{2, 1})); err != nil {
			t.Errorf("resending compacted entries = %v, want it to be ignored", err)
		}
		if got := s.FirstIndex(); got != 3 {
			t.Errorf("FirstIndex = %d, want 3 — the resend must change nothing", got)
		}
		if got := s.LastIndex(); got != 4 {
			t.Errorf("LastIndex = %d, want 4", got)
		}
	})
}

// Saving a snapshot must not block a later compaction to a lower index.
//
// A leader keeps a tail of entries past the snapshot point so slightly-behind
// followers can be repaired with entries rather than a whole state machine
// image. That means the log floor sits *below* the snapshot boundary, and a
// store that treats the two as one thing decides Compact has already run and
// silently does nothing — leaving the log growing for ever behind a snapshot
// that claimed to have shortened it.
func TestCompactBelowTheSnapshotBoundaryStillWorks(t *testing.T) {
	eachStorage(t, func(t *testing.T, s raft.Storage) {
		ents := make([]raft.LogEntry, 0, 60)
		for i := 1; i <= 60; i++ {
			ents = append(ents, raft.LogEntry{Index: raft.Index(i), Term: 1})
		}
		mustAppend(t, s, ents)

		if err := s.SaveSnapshot(raft.Snapshot{
			LastIncludedIndex: 60, LastIncludedTerm: 1,
			Config: raft.NewConfiguration([]raft.NodeID{1}),
		}); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		// Saving alone keeps the entries, so a follower one entry behind is
		// still repairable without shipping the whole state machine.
		if got := s.FirstIndex(); got != 1 {
			t.Errorf("FirstIndex = %d after SaveSnapshot alone, want 1", got)
		}

		// Now compact behind the snapshot, keeping a tail of five.
		if err := s.Compact(55); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if got := s.FirstIndex(); got != 56 {
			t.Errorf("FirstIndex = %d, want 56 — compaction below the snapshot "+
				"boundary did nothing", got)
		}
		if got := s.LastIndex(); got != 60 {
			t.Errorf("LastIndex = %d, want 60 — the retained tail was lost", got)
		}
		// The floor itself must still answer, for the consistency check.
		if term, err := s.Term(55); err != nil || term != 1 {
			t.Errorf("Term(55) = %d, %v; want 1, nil at the compaction boundary", term, err)
		}
		if _, err := s.GetEntry(54); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("GetEntry(54) = %v, want ErrCompacted", err)
		}
	})
}
