package raft

import (
	"fmt"
	"slices"
)

// MemoryStorage is an in-memory Storage.
//
// "Durability" here means surviving the loss of a node's volatile state, which
// is exactly what a crash is from Raft's point of view. The simulator crashes a
// node by discarding its RawNode and keeping its MemoryStorage, then restarting from
// it — the persisted log outlives the process. To simulate losing the disk too,
// it simply starts the node with a fresh MemoryStorage instead.
//
// Not safe for concurrent use; see the note on Storage.
type MemoryStorage struct {
	hardState HardState
	snapshot  Snapshot

	// ents holds the log, with ents[0] a sentinel rather than a real entry: it
	// carries the index and term of the last entry covered by the snapshot.
	//
	// The sentinel earns its place by making the snapshot boundary answerable.
	// An AppendEntries consistency check can legitimately reach back to exactly
	// that index, so Term() has to answer for an entry that no longer exists.
	// Keeping it as element zero means every index calculation is one uniform
	// piece of arithmetic instead of a special case at the boundary.
	ents []LogEntry
}

var _ Storage = (*MemoryStorage)(nil)

// NewMemoryStorage returns an empty store representing a node that has never run.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents: []LogEntry{{Index: 0, Term: 0}},
	}
}

func (m *MemoryStorage) InitialState() (HardState, Snapshot, error) {
	return m.hardState, m.snapshot.Clone(), nil
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	if hs.Term < m.hardState.Term {
		return fmt.Errorf("storage: hard state term %d is below persisted term %d",
			hs.Term, m.hardState.Term)
	}
	m.hardState = hs
	return nil
}

// offset is the index of the sentinel: the last index covered by the snapshot.
func (m *MemoryStorage) offset() Index { return m.ents[0].Index }

func (m *MemoryStorage) FirstIndex() Index { return m.offset() + 1 }

func (m *MemoryStorage) LastIndex() Index { return m.offset() + Index(len(m.ents)) - 1 }

func (m *MemoryStorage) Term(i Index) (Term, error) {
	switch {
	case i < m.offset():
		return 0, fmt.Errorf("storage: term at %d, first available %d: %w",
			i, m.FirstIndex(), ErrCompacted)
	case i > m.LastIndex():
		return 0, fmt.Errorf("storage: term at %d, last index %d: %w",
			i, m.LastIndex(), ErrUnavailable)
	default:
		return m.ents[i-m.offset()].Term, nil
	}
}

func (m *MemoryStorage) GetEntry(i Index) (LogEntry, error) {
	switch {
	case i < m.FirstIndex():
		return LogEntry{}, fmt.Errorf("storage: entry at %d, first available %d: %w",
			i, m.FirstIndex(), ErrCompacted)
	case i > m.LastIndex():
		return LogEntry{}, fmt.Errorf("storage: entry at %d, last index %d: %w",
			i, m.LastIndex(), ErrUnavailable)
	default:
		return cloneEntry(m.ents[i-m.offset()]), nil
	}
}

func (m *MemoryStorage) Entries(lo, hi Index) ([]LogEntry, error) {
	if lo >= hi {
		return nil, nil
	}
	if lo < m.FirstIndex() {
		return nil, fmt.Errorf("storage: entries from %d, first available %d: %w",
			lo, m.FirstIndex(), ErrCompacted)
	}
	if hi > m.LastIndex()+1 {
		return nil, fmt.Errorf("storage: entries to %d, last index %d: %w",
			hi-1, m.LastIndex(), ErrUnavailable)
	}
	out := make([]LogEntry, 0, hi-lo)
	for _, e := range m.ents[lo-m.offset() : hi-m.offset()] {
		out = append(out, cloneEntry(e))
	}
	return out, nil
}

func (m *MemoryStorage) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := checkContiguous(entries); err != nil {
		return err
	}

	first, last := entries[0].Index, entries[len(entries)-1].Index

	// Entirely covered by the snapshot: nothing left to store. Not an error —
	// a leader can legitimately resend entries a follower has since compacted.
	if last <= m.offset() {
		return nil
	}

	// Trim any prefix the snapshot already covers.
	if first <= m.offset() {
		entries = entries[m.offset()+1-first:]
		first = entries[0].Index
	}

	if first > m.LastIndex()+1 {
		return fmt.Errorf("storage: append at %d leaves a gap after last index %d",
			first, m.LastIndex())
	}

	// Truncate the conflicting suffix, then append. Overwriting a divergent
	// tail is the normal mechanism by which a follower is brought back into
	// agreement with a leader — the core has already checked that nothing
	// committed is being discarded.
	m.ents = m.ents[:first-m.offset()]
	for _, e := range entries {
		m.ents = append(m.ents, cloneEntry(e))
	}
	return nil
}

func checkContiguous(entries []LogEntry) error {
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("storage: entries are not contiguous at %d -> %d",
				entries[i-1].Index, entries[i].Index)
		}
		if entries[i].Term < entries[i-1].Term {
			return fmt.Errorf("storage: entry terms decrease at index %d (%d -> %d)",
				entries[i].Index, entries[i-1].Term, entries[i].Term)
		}
	}
	return nil
}

func (m *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	if snap.LastIncludedIndex <= m.snapshot.LastIncludedIndex {
		return fmt.Errorf("storage: snapshot at %d, already have %d: %w",
			snap.LastIncludedIndex, m.snapshot.LastIncludedIndex, ErrSnapshotOutOfDate)
	}
	m.snapshot = snap.Clone()

	// A snapshot that covers entries we hold leaves the log's tail intact; one
	// that runs past our log replaces it entirely, which is the catch-up case
	// where a follower was too far behind to be repaired with entries.
	if snap.LastIncludedIndex <= m.LastIndex() &&
		m.ents[snap.LastIncludedIndex-m.offset()].Term == snap.LastIncludedTerm {
		m.ents = append([]LogEntry{{
			Index: snap.LastIncludedIndex,
			Term:  snap.LastIncludedTerm,
		}}, m.ents[snap.LastIncludedIndex-m.offset()+1:]...)
	} else {
		m.ents = []LogEntry{{
			Index: snap.LastIncludedIndex,
			Term:  snap.LastIncludedTerm,
		}}
	}
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (Snapshot, error) {
	return m.snapshot.Clone(), nil
}

func (m *MemoryStorage) Compact(upto Index) error {
	switch {
	case upto <= m.offset():
		return nil // already compacted this far
	case upto > m.LastIndex():
		return fmt.Errorf("storage: compact to %d past last index %d", upto, m.LastIndex())
	}
	m.ents = slices.Clone(m.ents[upto-m.offset():])
	m.ents[0].Command = nil // the sentinel carries only index and term
	m.ents[0].Type = EntryNormal
	return nil
}

func (m *MemoryStorage) Close() error { return nil }

func cloneEntry(e LogEntry) LogEntry {
	e.Command = slices.Clone(e.Command)
	return e
}
