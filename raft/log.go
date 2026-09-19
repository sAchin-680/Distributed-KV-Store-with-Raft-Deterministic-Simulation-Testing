package raft

import (
	"errors"
	"fmt"
)

// raftLog is the core's view of the replicated log.
//
// It owns the two volatile indexes that Raft tracks alongside the durable log —
// committed and applied — and it enforces the invariants that relate them to the
// entries in Storage. Everything here is a thin, testable layer over Storage;
// no message handling, no roles, no terms of its own.
type raftLog struct {
	storage Storage

	// committed is the highest index known to be replicated to a quorum. Once
	// an index is committed it is permanent: no future leader may overwrite it,
	// and every node will eventually hold exactly that entry at that index.
	committed Index

	// applied is the highest index handed to the state machine. Always
	// applied <= committed; the gap is entries that are safe to apply but that
	// the driver has not drained yet.
	applied Index
}

func newRaftLog(storage Storage) (*raftLog, error) {
	hs, snap, err := storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: reading initial state: %w", err)
	}

	l := &raftLog{storage: storage}

	// A snapshot means everything up to its last included index is, by
	// definition, both committed and applied — the state machine image in the
	// snapshot already reflects them.
	l.committed = snap.LastIncludedIndex
	l.applied = snap.LastIncludedIndex

	// A persisted commit index can only move us forward, and only as far as the
	// log actually reaches. It is an optimization, not a source of truth, so it
	// is clamped rather than trusted.
	if hs.Commit > l.committed {
		l.committed = min(hs.Commit, storage.LastIndex())
	}

	return l, nil
}

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

func (l *raftLog) firstIndex() Index { return l.storage.FirstIndex() }
func (l *raftLog) lastIndex() Index  { return l.storage.LastIndex() }

// lastTerm returns the term of the last entry, or 0 for an empty log.
func (l *raftLog) lastTerm() Term {
	t, err := l.term(l.lastIndex())
	if err != nil {
		// The last index is always answerable: it is either a stored entry or
		// the snapshot boundary. Reaching here means Storage is inconsistent
		// with itself, which is not a condition we can sensibly continue from.
		panic(fmt.Sprintf("raft: term of last index %d unavailable: %v", l.lastIndex(), err))
	}
	return t
}

// term returns the term of the entry at index i.
//
// Index 0 is the position before the first entry and has term 0. It is not a
// special case for its own sake: the very first AppendEntries a leader sends
// carries prevLogIndex 0, and the consistency check has to be answerable there.
func (l *raftLog) term(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	return l.storage.Term(i)
}

// matchTerm reports whether the log holds an entry at index i with term t.
//
// A missing or compacted entry is reported as "no match" rather than an error.
// Callers are asking a question about agreement, and "I do not have it" is a
// perfectly good negative answer to that question.
func (l *raftLog) matchTerm(i Index, t Term) bool {
	lt, err := l.term(i)
	if err != nil {
		return false
	}
	return lt == t
}

// entries returns the entries in [lo, hi).
func (l *raftLog) entries(lo, hi Index) ([]LogEntry, error) {
	if lo >= hi {
		return nil, nil
	}
	if hi > l.lastIndex()+1 {
		return nil, fmt.Errorf("raft: entries(%d,%d) past last index %d: %w",
			lo, hi, l.lastIndex(), ErrUnavailable)
	}
	return l.storage.Entries(lo, hi)
}

// ---------------------------------------------------------------------------
// The election restriction
// ---------------------------------------------------------------------------

// isUpToDate reports whether a candidate's log, whose last entry is
// lastIdx@lastTerm, is at least as up to date as this node's.
//
// This is the election restriction (§5.4.1), and it is the single detail most
// often missed in from-scratch implementations. It is what guarantees that a
// newly elected leader already holds every committed entry, which is in turn
// what makes "committed means permanent" true.
//
// The comparison is lexicographic on (term, index), and the order matters: a
// *later term* always wins regardless of length. A node with a long log full of
// entries from term 3 is less up to date than a node with a short log whose last
// entry is from term 5, because the term-5 entry could only exist if a term-5
// leader was elected by a quorum, which means the long log's tail was never
// committed. Comparing index first — the intuitive "longer log wins" — is wrong
// and will silently lose committed data.
func (l *raftLog) isUpToDate(lastIdx Index, lastTerm Term) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= l.lastIndex()
}

// ---------------------------------------------------------------------------
// Appending
// ---------------------------------------------------------------------------

// append writes entries to storage, overwriting any conflicting suffix.
func (l *raftLog) append(ents []LogEntry) error {
	if len(ents) == 0 {
		return nil
	}
	if first := ents[0].Index; first <= l.committed {
		return fmt.Errorf("raft: append at index %d would overwrite committed index %d",
			first, l.committed)
	}
	return l.storage.AppendEntries(ents)
}

// findConflict returns the index of the first entry in ents that this log does
// not already hold with the same term, or 0 if the log already holds all of them.
//
// An entry past the end of the log counts as a conflict. That is not a
// distinction worth making: "I disagree with you here" and "I have nothing here"
// both mean the same thing to the caller — start writing at this index.
func (l *raftLog) findConflict(ents []LogEntry) Index {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// maybeAppend implements the receiving half of AppendEntries.
//
// It returns the index of the last new entry and true on success. It returns
// false when the consistency check at prevIdx@prevTerm fails, which is the
// signal for the leader to back up and retry — that retry loop is what enforces
// the log matching property: if two logs agree on an entry, they agree on every
// entry before it.
func (l *raftLog) maybeAppend(prevIdx Index, prevTerm Term, leaderCommit Index, ents []LogEntry) (Index, bool, error) {
	if !l.matchTerm(prevIdx, prevTerm) {
		return 0, false, nil
	}

	lastNewIndex := prevIdx + Index(len(ents))

	switch conflict := l.findConflict(ents); {
	case conflict == 0:
		// Every entry is already present with a matching term. Writing them
		// again would be harmless but wasteful — and worse, truncating first
		// would discard entries beyond lastNewIndex that a later AppendEntries
		// has already delivered. Duplicate and reordered messages make this a
		// real case, not a theoretical one.

	case conflict <= l.committed:
		// A leader is asking us to overwrite something we have already
		// committed. If this is ever reachable, Raft's safety argument has been
		// broken somewhere upstream — most likely the election restriction or
		// the commit rule. Refuse loudly rather than corrupting the log; the
		// simulator's safety checker exists to catch exactly this.
		return 0, false, fmt.Errorf(
			"raft: leader entry at index %d conflicts with committed index %d: %w",
			conflict, l.committed, ErrSafetyViolation)

	default:
		offset := conflict - prevIdx - 1
		if err := l.append(ents[offset:]); err != nil {
			return 0, false, err
		}
	}

	// The leader's commit index may run ahead of what it just sent us; we may
	// only commit as far as entries we actually hold from this message.
	l.commitTo(min(leaderCommit, lastNewIndex))
	return lastNewIndex, true, nil
}

// ErrSafetyViolation marks a condition that Raft's correctness argument says is
// unreachable. It is never an expected outcome — seeing it means there is a bug
// in this implementation, and the right response is to stop, not to recover.
var ErrSafetyViolation = errors.New("raft: safety violation")

// ---------------------------------------------------------------------------
// Commit and apply
// ---------------------------------------------------------------------------

// commitTo advances the commit index. It never moves backwards: a commit index
// is a promise, and a stale or duplicated message must not be able to retract it.
func (l *raftLog) commitTo(i Index) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit to %d beyond last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

// appliedTo records that the state machine has consumed entries through i.
func (l *raftLog) appliedTo(i Index) {
	if i == 0 || i <= l.applied {
		return
	}
	if i > l.committed {
		panic(fmt.Sprintf("raft: applied %d beyond committed %d", i, l.committed))
	}
	l.applied = i
}

// hasEntriesToApply reports whether any committed entry is waiting for the
// state machine.
func (l *raftLog) hasEntriesToApply() bool {
	return l.committed > l.applied
}

// entriesToApply returns the committed entries the state machine has not seen,
// capped at maxEntries so a large backlog is drained in bounded chunks rather
// than in one allocation the size of the whole log.
func (l *raftLog) entriesToApply(maxEntries int) ([]LogEntry, error) {
	if !l.hasEntriesToApply() {
		return nil, nil
	}
	lo := max(l.applied+1, l.firstIndex())
	hi := l.committed + 1
	if maxEntries > 0 && hi-lo > Index(maxEntries) {
		hi = lo + Index(maxEntries)
	}
	return l.entries(lo, hi)
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// restore replaces the log with a snapshot, discarding everything it covers.
func (l *raftLog) restore(snap Snapshot) error {
	if snap.LastIncludedIndex <= l.committed {
		// We already have everything this snapshot contains, through the log.
		// Installing it would be a no-op at best and would throw away entries
		// past its boundary at worst.
		return ErrSnapshotOutOfDate
	}
	if err := l.storage.SaveSnapshot(snap); err != nil {
		return err
	}
	l.committed = snap.LastIncludedIndex
	l.applied = snap.LastIncludedIndex
	return nil
}

// compact discards log entries through upto, which must already be covered by a
// saved snapshot and must not exceed what the state machine has applied.
func (l *raftLog) compact(upto Index) error {
	if upto > l.applied {
		return fmt.Errorf("raft: cannot compact to %d past applied index %d", upto, l.applied)
	}
	return l.storage.Compact(upto)
}

func (l *raftLog) String() string {
	return fmt.Sprintf("log{first=%d last=%d committed=%d applied=%d}",
		l.firstIndex(), l.lastIndex(), l.committed, l.applied)
}
