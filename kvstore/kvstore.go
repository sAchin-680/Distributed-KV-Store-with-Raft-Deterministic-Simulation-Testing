// Package kvstore is the replicated state machine: a key/value map fed by
// committed log entries.
//
// Nothing here knows about Raft beyond the shape of a log entry. That is the
// point of a replicated state machine — the consensus layer decides *what order*
// commands run in, and the state machine decides what they mean. Keeping them
// apart is why the same core can replicate a KV map, a configuration store, or
// anything else deterministic.
//
// Determinism is the one hard requirement. Two nodes applying the same command
// must reach the same state; anything that consults the wall clock, a random
// source, or iteration order of a Go map would break that, and the divergence
// would not show up until two replicas were compared.
package kvstore

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/sAchin-680/raftkv/raft"
)

// Op is what a command does.
type Op uint8

const (
	OpSet Op = iota
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpSet:
		return "set"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown-op(%d)", uint8(o))
	}
}

// Command is one mutation, as it travels through the log.
type Command struct {
	Op    Op
	Key   []byte
	Value []byte

	// ClientID and Sequence identify the client and the request, so a retry of
	// a command that already committed replays its stored answer instead of
	// being applied a second time. A zero ClientID opts out.
	ClientID uint64
	Sequence uint64
}

// Store is an in-memory key/value map driven by the log.
//
// Apply runs only on the node's single run goroutine; reads can arrive from any
// gRPC handler goroutine. The mutex guards that boundary and nothing else — it
// is not protecting Raft state, which is never shared.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte

	// sessions remembers the last request applied per client, so a retry can be
	// recognized. See session.go.
	sessions    map[uint64]session
	maxSessions int

	// applied is the highest log index folded into this map. A read needs it to
	// answer "is this state at least as new as the index I was promised?",
	// which is the question a linearizable read turns into.
	applied raft.Index
}

// New returns an empty store.
func New() *Store { return NewWithSessionLimit(DefaultMaxSessions) }

// NewWithSessionLimit returns an empty store remembering at most n clients.
func NewWithSessionLimit(n int) *Store {
	if n < 1 {
		n = 1
	}
	return &Store{
		data:        make(map[string][]byte),
		sessions:    make(map[uint64]session),
		maxSessions: n,
	}
}

var _ interface {
	Apply(raft.LogEntry) (any, error)
} = (*Store)(nil)

// Apply folds one committed entry into the map and returns what the write
// should answer.
//
// Called in log order, once per entry, never concurrently.
func (s *Store) Apply(entry raft.LogEntry) (any, error) {
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		// A command this build cannot parse is a fatal condition, not something
		// to skip. Skipping it would leave this node's state machine different
		// from every other node's, with nothing to detect the divergence.
		return nil, fmt.Errorf("kvstore: entry %d: %w", entry.Index, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A retry of something already applied replays its answer. The applied
	// index still advances — the entry is in the log either way — but the map
	// is left alone.
	if result, duplicate := s.checkSession(cmd.ClientID, cmd.Sequence); duplicate {
		s.applied = entry.Index
		return result, nil
	}

	var result Result
	switch cmd.Op {
	case OpSet:
		s.data[string(cmd.Key)] = slices.Clone(cmd.Value)
	case OpDelete:
		_, result.Existed = s.data[string(cmd.Key)]
		delete(s.data, string(cmd.Key))
	default:
		return nil, fmt.Errorf("kvstore: entry %d has unknown operation %d", entry.Index, cmd.Op)
	}

	s.recordSession(cmd.ClientID, cmd.Sequence, entry.Index, result)
	s.applied = entry.Index
	return result, nil
}

// Get reads a key, reporting whether it was present.
//
// This is a local read of whatever has been applied here. It is *not*
// linearizable on its own: a partitioned former leader will happily serve stale
// values from a map it stopped updating. Serving a linearizable read means
// confirming leadership first and then waiting for AppliedIndex to reach the
// index that was committed at that moment.
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	return slices.Clone(v), true
}

// AppliedIndex is the highest log index reflected in this map.
func (s *Store) AppliedIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Len is how many keys are present.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Keys returns every key, sorted.
//
// Sorted rather than in map order, because callers compare these across nodes
// and Go randomizes map iteration. An unsorted answer would make two identical
// state machines look different.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Sorted(maps.Keys(s.data))
}

// Snapshot returns a deterministic serialization of the whole map, for the
// snapshotting work that folds a log prefix into a single image.
//
// The error is always nil today. It is in the signature because a snapshot of a
// larger state machine will eventually be able to fail, and changing the shape
// of this call later would touch every caller.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return encodeMap(s.data), nil
}

// Restore replaces the map with the contents of a snapshot.
func (s *Store) Restore(data []byte, appliedIndex raft.Index) error {
	m, err := decodeMap(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = m
	s.applied = appliedIndex
	return nil
}
