package storage_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/storage"
)

func openAt(t testing.TB, path string, opts storage.Options) *storage.Bolt {
	t.Helper()
	db, err := storage.Open(path, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	return db
}

// The reason this package exists.
//
// The simulator models a crash by discarding a node's volatile state and keeping
// its storage. This is the real version of that: close the database, open it
// again, and require everything the core promised was durable to still be there.
func TestStateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	db := openAt(t, path, storage.DefaultOptions())
	mustAppend(t, db, entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 4}))
	if err := db.SetHardState(raft.HardState{Term: 4, Vote: 2, Commit: 3}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, path, storage.DefaultOptions())
	defer func() { _ = reopened.Close() }()

	hs, _, err := reopened.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}

	// Term and vote are the ones that matter. A node that forgets it voted can
	// vote a second time in the same term and elect a second leader.
	if hs.Term != 4 || hs.Vote != 2 {
		t.Errorf("recovered %s, want term 4 vote 2", hs)
	}
	if hs.Commit != 3 {
		t.Errorf("recovered commit %d, want 3", hs.Commit)
	}

	if got := reopened.LastIndex(); got != 3 {
		t.Errorf("LastIndex = %d, want 3", got)
	}
	for i, wantTerm := range map[raft.Index]raft.Term{1: 1, 2: 1, 3: 4} {
		if term, err := reopened.Term(i); err != nil || term != wantTerm {
			t.Errorf("Term(%d) = %d, %v; want %d, nil", i, term, err, wantTerm)
		}
	}
	entry, err := reopened.GetEntry(3)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if string(entry.Command) != "cmd-3" {
		t.Errorf("command = %q, want %q", entry.Command, "cmd-3")
	}
}

func TestSnapshotSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	db := openAt(t, path, storage.DefaultOptions())
	want := raft.Snapshot{
		LastIncludedIndex: 20,
		LastIncludedTerm:  6,
		Config:            raft.NewConfiguration([]raft.NodeID{1, 2, 3}, 9),
		Data:              []byte("the state machine at index 20"),
	}
	if err := db.SaveSnapshot(want); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	mustAppend(t, db, entries([2]uint64{21, 6}, [2]uint64{22, 6}))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, path, storage.DefaultOptions())
	defer func() { _ = reopened.Close() }()

	_, snap, err := reopened.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if snap.LastIncludedIndex != 20 || snap.LastIncludedTerm != 6 {
		t.Errorf("recovered snapshot %s, want index 20 term 6", snap)
	}
	if !snap.Config.Equal(want.Config) {
		t.Errorf("recovered configuration %s, want %s — a node that loses this "+
			"comes back not knowing who its peers are", snap.Config, want.Config)
	}
	if got := reopened.FirstIndex(); got != 21 {
		t.Errorf("FirstIndex = %d, want 21", got)
	}
	if got := reopened.LastIndex(); got != 22 {
		t.Errorf("LastIndex = %d, want 22", got)
	}
	if term, err := reopened.Term(20); err != nil || term != 6 {
		t.Errorf("Term(20) = %d, %v; want 6, nil at the snapshot boundary", term, err)
	}
}

func TestTruncationSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	db := openAt(t, path, storage.DefaultOptions())
	mustAppend(t, db, entries(
		[2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}, [2]uint64{4, 1}))
	mustAppend(t, db, entries([2]uint64{2, 7})) // overwrite from index 2
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, path, storage.DefaultOptions())
	defer func() { _ = reopened.Close() }()

	if got := reopened.LastIndex(); got != 2 {
		t.Errorf("LastIndex = %d, want 2 — the truncated tail must not come back", got)
	}
	if term, err := reopened.Term(2); err != nil || term != 7 {
		t.Errorf("Term(2) = %d, %v; want 7, nil", term, err)
	}
}

// A second process opening the same database must fail rather than corrupt it.
// In a Kubernetes rollout this is what stops a terminating pod and its
// replacement from writing the same volume at once.
func TestSecondOpenIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	db := openAt(t, path, storage.DefaultOptions())
	defer func() { _ = db.Close() }()

	if _, err := storage.Open(path, storage.Options{Timeout: 100 * time.Millisecond}); err == nil {
		t.Error("a second open of a locked database should fail, not proceed")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db := openAt(t, filepath.Join(t.TempDir(), "raft.db"), storage.DefaultOptions())
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// What durability actually costs.
//
// This is the number that puts the core-only benchmarks in perspective: the
// consensus algorithm commits a round in about 1.5 µs, and a single durable
// append takes far longer than that. A real cluster is bounded by this and by
// the network, not by the algorithm — which is exactly why the core benchmarks
// must never be quoted as system throughput.
func BenchmarkBoltAppend(b *testing.B) {
	for _, sync := range []bool{true, false} {
		name := "fsync"
		if !sync {
			name = "nosync"
		}
		for _, batch := range []int{1, 16, 128} {
			b.Run(fmt.Sprintf("%s/batch=%d", name, batch), func(b *testing.B) {
				db := openAt(b, filepath.Join(b.TempDir(), "raft.db"),
					storage.Options{NoSync: !sync, Timeout: time.Second})
				defer func() { _ = db.Close() }()

				cmd := make([]byte, 128)
				next := raft.Index(1)

				b.SetBytes(int64(batch * 128))
				b.ResetTimer()
				for range b.N {
					ents := make([]raft.LogEntry, batch)
					for j := range ents {
						ents[j] = raft.LogEntry{
							Index: next + raft.Index(j), Term: 1, Command: cmd,
						}
					}
					if err := db.AppendEntries(ents); err != nil {
						b.Fatalf("AppendEntries: %v", err)
					}
					next += raft.Index(batch)
				}
				b.StopTimer()
				b.ReportMetric(float64(batch), "entries/op")
			})
		}
	}
}

func BenchmarkBoltRead(b *testing.B) {
	db := openAt(b, filepath.Join(b.TempDir(), "raft.db"),
		storage.Options{NoSync: true, Timeout: time.Second})
	defer func() { _ = db.Close() }()

	const total = 10_000
	cmd := make([]byte, 128)
	ents := make([]raft.LogEntry, total)
	for i := range ents {
		ents[i] = raft.LogEntry{Index: raft.Index(i + 1), Term: 1, Command: cmd}
	}
	if err := db.AppendEntries(ents); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.Run("GetEntry", func(b *testing.B) {
		for i := range b.N {
			if _, err := db.GetEntry(raft.Index(i%total) + 1); err != nil {
				b.Fatalf("GetEntry: %v", err)
			}
		}
	})

	b.Run("Entries/64", func(b *testing.B) {
		for i := range b.N {
			lo := raft.Index(i%(total-64)) + 1
			if _, err := db.Entries(lo, lo+64); err != nil {
				b.Fatalf("Entries: %v", err)
			}
		}
	})
}
