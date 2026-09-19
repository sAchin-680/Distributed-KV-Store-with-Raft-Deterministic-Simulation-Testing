package raft

import "errors"

// Errors returned by Storage implementations. Callers distinguish them with
// errors.Is; implementations must wrap rather than replace them.
var (
	// ErrCompacted means the requested index is older than the log's first
	// available entry — it has been discarded into a snapshot. For a leader
	// this is the signal to send InstallSnapshot instead of AppendEntries.
	ErrCompacted = errors.New("raft: requested index is compacted")

	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("raft: requested index is unavailable")

	// ErrSnapshotOutOfDate means a snapshot older than the current one was
	// offered. Applying it would move the node backwards.
	ErrSnapshotOutOfDate = errors.New("raft: snapshot is out of date")
)

// Storage is the durable state Raft depends on to survive a crash.
//
// This interface is the seam that makes the whole verification strategy work.
// Production runs on bbolt with real fsyncs; the simulator swaps in an in-memory
// implementation and gets the same semantics with none of the I/O, so a
// simulated hour of cluster time costs milliseconds. A crash in simulation is
// just dropping the volatile state and keeping the Storage — which is precisely
// what a real crash is.
//
// Implementations must be crash-safe in one specific sense: whatever a call
// returns from, must be durable. The core calls Storage synchronously and only
// emits the resulting messages afterwards, because Raft's correctness depends on
// a vote or an appended entry being on disk *before* the response admitting to
// it leaves the node.
//
// Implementations are not required to be safe for concurrent use. The core is
// single-threaded by construction, and the production driver gives each node's
// Storage to exactly one goroutine.
type Storage interface {
	// InitialState returns the persisted hard state and snapshot recorded at
	// startup. A node that has never run returns zero values for both.
	InitialState() (HardState, Snapshot, error)

	// SetHardState durably records term, vote and commit index. It must not
	// return until the write is durable: a node that forgets it voted in a term
	// can vote a second time in that term and elect a second leader.
	SetHardState(hs HardState) error

	// AppendEntries durably appends entries to the log.
	//
	// Entries must be contiguous and ascending. If the first entry's index is
	// one the log already holds, that entry and everything after it is
	// discarded first — overwriting a conflicting suffix is the normal way a
	// follower is brought back into agreement with a leader, not an error.
	AppendEntries(entries []LogEntry) error

	// GetEntry returns a single entry, or ErrCompacted/ErrUnavailable.
	GetEntry(index Index) (LogEntry, error)

	// Entries returns the entries in [lo, hi). It returns ErrCompacted if lo
	// precedes the first available entry.
	Entries(lo, hi Index) ([]LogEntry, error)

	// FirstIndex returns the lowest index still available in the log, which is
	// one past the snapshot's last included index.
	//
	// FirstIndex and LastIndex are infallible by contract: implementations must
	// serve them from memory. They are consulted on nearly every operation, and
	// a fallible bounds check would spread error handling through every call
	// site in the core for no benefit.
	FirstIndex() Index

	// LastIndex returns the highest index in the log, or the snapshot's last
	// included index if the log is empty.
	LastIndex() Index

	// Term returns the term of the entry at index. It answers for the
	// snapshot's last included index too, even though that entry itself is no
	// longer stored — AppendEntries consistency checks reach back to exactly
	// that boundary, so it has to be answerable.
	Term(index Index) (Term, error)

	// SaveSnapshot durably records a snapshot. It returns ErrSnapshotOutOfDate
	// if one at least as recent is already stored.
	SaveSnapshot(snap Snapshot) error

	// LoadSnapshot returns the most recent snapshot, or an empty one.
	LoadSnapshot() (Snapshot, error)

	// Compact discards every log entry at or below upto. The caller is
	// responsible for having saved a snapshot that covers them first; Compact
	// itself only enforces that it never discards past LastIndex.
	Compact(upto Index) error

	// Close releases resources. Safe to call more than once.
	Close() error
}
