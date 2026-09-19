package raft

import (
	"errors"
	"testing"
)

// newTestLog builds a log whose entry at index i+1 has term terms[i].
func newTestLog(t *testing.T, terms ...Term) *raftLog {
	t.Helper()
	st := NewMemoryStorage()
	if len(terms) > 0 {
		ents := make([]LogEntry, len(terms))
		for i, term := range terms {
			ents[i] = LogEntry{Index: Index(i + 1), Term: term}
		}
		if err := st.AppendEntries(ents); err != nil {
			t.Fatalf("seeding log: %v", err)
		}
	}
	l, err := newRaftLog(st)
	if err != nil {
		t.Fatalf("newRaftLog: %v", err)
	}
	return l
}

func entries(spec ...[2]uint64) []LogEntry {
	out := make([]LogEntry, len(spec))
	for i, s := range spec {
		out[i] = LogEntry{Index: Index(s[0]), Term: Term(s[1])}
	}
	return out
}

// ---------------------------------------------------------------------------
// The election restriction (§5.4.1)
// ---------------------------------------------------------------------------

// This is the rule that guarantees a new leader already holds every committed
// entry, which is what makes "committed means permanent" true. The ordering of
// the comparison is the whole point: term first, index only as a tiebreak.
func TestElectionRestriction(t *testing.T) {
	// Local log: index 1,2,3 with terms 1,2,2. Last entry is 3@2.
	local := []Term{1, 2, 2}

	tests := []struct {
		name          string
		candLastIndex Index
		candLastTerm  Term
		wantUpToDate  bool
	}{
		{
			name:          "identical log",
			candLastIndex: 3, candLastTerm: 2,
			wantUpToDate: true,
		},
		{
			name:          "same term, longer log",
			candLastIndex: 5, candLastTerm: 2,
			wantUpToDate: true,
		},
		{
			name:          "same term, shorter log",
			candLastIndex: 2, candLastTerm: 2,
			wantUpToDate: false,
		},
		{
			name:          "higher term, shorter log still wins",
			candLastIndex: 1, candLastTerm: 3,
			wantUpToDate: true,
		},
		{
			// The case that breaks implementations which compare index first.
			// A long log of stale entries loses to a short log with a newer
			// term, because those stale entries provably never committed.
			name:          "lower term, much longer log still loses",
			candLastIndex: 99, candLastTerm: 1,
			wantUpToDate: false,
		},
		{
			name:          "empty candidate log",
			candLastIndex: 0, candLastTerm: 0,
			wantUpToDate: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newTestLog(t, local...)
			got := l.isUpToDate(tc.candLastIndex, tc.candLastTerm)
			if got != tc.wantUpToDate {
				t.Errorf("isUpToDate(%d@%d) = %t, want %t (local last = %d@%d)",
					tc.candLastIndex, tc.candLastTerm, got, tc.wantUpToDate,
					l.lastIndex(), l.lastTerm())
			}
		})
	}
}

func TestEmptyLogAcceptsAnyCandidate(t *testing.T) {
	l := newTestLog(t)
	if !l.isUpToDate(0, 0) {
		t.Error("an empty log must consider another empty log up to date")
	}
	if !l.isUpToDate(1, 1) {
		t.Error("an empty log must consider any non-empty log up to date")
	}
}

// ---------------------------------------------------------------------------
// The log matching property
// ---------------------------------------------------------------------------

func TestMaybeAppendRejectsOnConsistencyCheckFailure(t *testing.T) {
	l := newTestLog(t, 1, 2, 2)

	tests := []struct {
		name     string
		prevIdx  Index
		prevTerm Term
	}{
		{"term mismatch at a held index", 2, 5},
		{"index past the end of the log", 9, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, err := l.maybeAppend(tc.prevIdx, tc.prevTerm, 0, entries([2]uint64{4, 3}))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Error("append should have been rejected; the leader must back up and retry")
			}
			if l.lastIndex() != 3 {
				t.Errorf("a rejected append must not modify the log; last index = %d", l.lastIndex())
			}
		})
	}
}

func TestMaybeAppendTruncatesConflictingSuffix(t *testing.T) {
	// Local: 1@1 2@2 3@2 4@2. The leader disagrees from index 3 onward.
	l := newTestLog(t, 1, 2, 2, 2)

	last, ok, err := l.maybeAppend(2, 2, 0, entries([2]uint64{3, 3}, [2]uint64{4, 3}))
	if err != nil || !ok {
		t.Fatalf("maybeAppend: ok=%t err=%v", ok, err)
	}
	if last != 4 {
		t.Errorf("last new index = %d, want 4", last)
	}

	for _, want := range []struct {
		idx  Index
		term Term
	}{{1, 1}, {2, 2}, {3, 3}, {4, 3}} {
		got, err := l.term(want.idx)
		if err != nil {
			t.Fatalf("term(%d): %v", want.idx, err)
		}
		if got != want.term {
			t.Errorf("term at %d = %d, want %d", want.idx, got, want.term)
		}
	}
}

// A duplicated or delayed AppendEntries carrying entries the follower already
// has must be a no-op. If it truncated and re-appended, it would discard entries
// beyond its own range that a later message already delivered — and duplicates
// are not hypothetical, they are one of the faults the simulator injects.
func TestMaybeAppendIgnoresEntriesAlreadyHeld(t *testing.T) {
	l := newTestLog(t, 1, 1, 1, 1)

	last, ok, err := l.maybeAppend(0, 0, 0, entries([2]uint64{1, 1}, [2]uint64{2, 1}))
	if err != nil || !ok {
		t.Fatalf("maybeAppend: ok=%t err=%v", ok, err)
	}
	if last != 2 {
		t.Errorf("last new index = %d, want 2", last)
	}
	if l.lastIndex() != 4 {
		t.Errorf("last index = %d, want 4 — a duplicate must not truncate the tail", l.lastIndex())
	}
}

// If this ever fires in a real run, the safety argument has already been broken
// somewhere upstream. The log refuses rather than quietly corrupting itself.
func TestMaybeAppendRefusesToOverwriteCommittedEntries(t *testing.T) {
	l := newTestLog(t, 1, 1, 1)
	l.commitTo(3)

	_, ok, err := l.maybeAppend(1, 1, 0, entries([2]uint64{2, 9}))
	if !errors.Is(err, ErrSafetyViolation) {
		t.Fatalf("err = %v, want ErrSafetyViolation", err)
	}
	if ok {
		t.Error("append should not have succeeded")
	}
}

func TestMaybeAppendCommitsOnlyWhatItHolds(t *testing.T) {
	l := newTestLog(t, 1)

	// The leader's commit index is 10, but it only sent us up to index 2. We
	// may not commit past what we actually hold from this message.
	_, ok, err := l.maybeAppend(1, 1, 10, entries([2]uint64{2, 1}))
	if err != nil || !ok {
		t.Fatalf("maybeAppend: ok=%t err=%v", ok, err)
	}
	if l.committed != 2 {
		t.Errorf("committed = %d, want 2 (clamped to the last entry received)", l.committed)
	}
}

func TestCommitIndexNeverMovesBackwards(t *testing.T) {
	l := newTestLog(t, 1, 1, 1)
	l.commitTo(3)
	l.commitTo(1)
	if l.committed != 3 {
		t.Errorf("committed = %d, want 3 — a stale message must not retract a commit", l.committed)
	}
}

// ---------------------------------------------------------------------------
// Snapshot boundary
// ---------------------------------------------------------------------------

func TestTermIsAnswerableAtTheSnapshotBoundary(t *testing.T) {
	l := newTestLog(t, 1, 1, 2, 2, 3)
	l.commitTo(5)
	l.appliedTo(5)

	snap := Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
		Config:            NewConfiguration(ids(1, 2, 3)),
	}
	if err := l.storage.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// The boundary index itself must still answer — an AppendEntries
	// consistency check can legitimately reach back to exactly here.
	got, err := l.term(3)
	if err != nil {
		t.Fatalf("term at the snapshot boundary must be answerable, got %v", err)
	}
	if got != 2 {
		t.Errorf("term(3) = %d, want 2", got)
	}

	// Anything below it is gone, and the leader is expected to notice and send
	// a snapshot instead of entries.
	if _, err := l.term(2); !errors.Is(err, ErrCompacted) {
		t.Errorf("term(2) = %v, want ErrCompacted", err)
	}

	// Entries after the boundary survived compaction.
	if l.lastIndex() != 5 {
		t.Errorf("last index = %d, want 5", l.lastIndex())
	}
	if got, err := l.term(5); err != nil || got != 3 {
		t.Errorf("term(5) = %d, %v; want 3, nil", got, err)
	}
}

func TestRestoreFromSnapshotReplacesTheLog(t *testing.T) {
	l := newTestLog(t, 1, 1, 2, 2, 2)
	l.commitTo(3)
	l.appliedTo(3)

	// A snapshot from a leader that is far ahead of anything we hold. Our
	// entries at 4 and 5 are discarded: they were never committed, and the
	// snapshot is authoritative for everything up to its boundary.
	snap := Snapshot{
		LastIncludedIndex: 8,
		LastIncludedTerm:  4,
		Config:            NewConfiguration(ids(1, 2, 3)),
		Data:              []byte("state"),
	}
	if err := l.restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if l.committed != 8 || l.applied != 8 {
		t.Errorf("committed=%d applied=%d, want 8 and 8 — a snapshot is by definition applied",
			l.committed, l.applied)
	}
	if got, want := l.firstIndex(), Index(9); got != want {
		t.Errorf("firstIndex = %d, want %d", got, want)
	}
	if got, want := l.lastIndex(), Index(8); got != want {
		t.Errorf("lastIndex = %d, want %d", got, want)
	}
	if got, err := l.term(8); err != nil || got != 4 {
		t.Errorf("term(8) = %d, %v; want 4, nil", got, err)
	}
}

func TestRestoreRejectsAStaleSnapshot(t *testing.T) {
	l := newTestLog(t, 1, 1, 1, 1, 1)
	l.commitTo(5)

	// We already hold everything this snapshot covers, through the log itself.
	// Installing it would discard entries past its boundary for nothing.
	err := l.restore(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1})
	if !errors.Is(err, ErrSnapshotOutOfDate) {
		t.Fatalf("restore = %v, want ErrSnapshotOutOfDate", err)
	}
	if l.lastIndex() != 5 {
		t.Errorf("lastIndex = %d, want 5 — a rejected snapshot must change nothing", l.lastIndex())
	}
}

func TestCompactRefusesToDiscardUnappliedEntries(t *testing.T) {
	l := newTestLog(t, 1, 1, 1, 1, 1)
	l.commitTo(5)
	l.appliedTo(3)

	// Compacting past applied would throw away entries the state machine has
	// not consumed, leaving it permanently unable to reach the committed state.
	if err := l.compact(4); err == nil {
		t.Error("compact past the applied index should be refused")
	}

	if err := l.compact(3); err != nil {
		t.Fatalf("compact(3): %v", err)
	}
	if got, want := l.firstIndex(), Index(4); got != want {
		t.Errorf("firstIndex = %d, want %d", got, want)
	}
	if _, err := l.term(2); !errors.Is(err, ErrCompacted) {
		t.Errorf("term(2) = %v, want ErrCompacted", err)
	}
	// The boundary itself must still answer, for the consistency check.
	if got, err := l.term(3); err != nil || got != 1 {
		t.Errorf("term(3) = %d, %v; want 1, nil", got, err)
	}
}

func TestIndexZeroHasTermZero(t *testing.T) {
	// Not a curiosity: the first AppendEntries a leader ever sends carries
	// prevLogIndex 0, and the consistency check has to be answerable there.
	l := newTestLog(t, 1, 1)
	got, err := l.term(0)
	if err != nil {
		t.Fatalf("term(0): %v", err)
	}
	if got != 0 {
		t.Errorf("term(0) = %d, want 0", got)
	}
	if !l.matchTerm(0, 0) {
		t.Error("matchTerm(0, 0) must hold so a leader can append to an empty follower")
	}
}

func TestEntriesToApplyIsBounded(t *testing.T) {
	l := newTestLog(t, 1, 1, 1, 1, 1, 1)
	l.commitTo(6)

	got, err := l.entriesToApply(2)
	if err != nil {
		t.Fatalf("entriesToApply: %v", err)
	}
	if len(got) != 2 || got[0].Index != 1 || got[1].Index != 2 {
		t.Fatalf("got %v, want entries 1 and 2", got)
	}

	l.appliedTo(2)
	got, err = l.entriesToApply(0) // 0 means unbounded
	if err != nil {
		t.Fatalf("entriesToApply: %v", err)
	}
	if len(got) != 4 || got[0].Index != 3 {
		t.Fatalf("got %d entries starting at %d, want 4 starting at 3", len(got), got[0].Index)
	}
}
