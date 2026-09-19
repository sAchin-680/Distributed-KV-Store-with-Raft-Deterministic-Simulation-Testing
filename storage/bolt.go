// Package storage holds the production implementation of raft.Storage.
//
// The in-memory implementation that tests and the simulator run on lives in the
// raft package, because the core's own tests need it. Both are held to the same
// conformance suite: a simulation is only as trustworthy as its stand-ins, and
// an in-memory store that is more forgiving than the real one would let bugs
// through ten thousand clean runs.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/sAchin-680/raftkv/codec"
	"github.com/sAchin-680/raftkv/raft"
)

// Bucket names. Kept short because bbolt stores them in every page header.
var (
	bucketLog  = []byte("log")
	bucketMeta = []byte("meta")

	keyHardState = []byte("hard")
	keySnapshot  = []byte("snap")
)

// Options configures a Bolt store.
type Options struct {
	// NoSync disables fsync on commit.
	//
	// Dangerous, and named to say so. Raft's safety argument assumes a vote is
	// durable before the response admitting to it leaves the node; without
	// fsync a crash can lose it, the node can vote twice in one term, and two
	// leaders can be elected. It exists for benchmarks and for tests that would
	// otherwise spend all their time in the kernel, never for production.
	NoSync bool

	// Timeout bounds how long Open waits for the file lock. Zero means wait
	// indefinitely, which turns a second process started by mistake into a hang
	// rather than an error.
	Timeout time.Duration
}

// DefaultOptions syncs every commit and fails fast if another process holds the
// database.
func DefaultOptions() Options {
	return Options{Timeout: 5 * time.Second}
}

// Bolt is a raft.Storage backed by an embedded bbolt database.
//
// bbolt rather than an external database on purpose: Raft already provides the
// replication, so the storage layer only has to be durable on one machine.
// Adding a network hop and a second distributed system underneath a consensus
// protocol buys nothing and adds a failure domain.
type Bolt struct {
	db   *bolt.DB
	path string

	// first and last are cached because raft.Storage declares them infallible.
	// They are consulted on nearly every operation, and a fallible bounds check
	// would spread error handling through every call site in the core for no
	// benefit.
	first raft.Index
	last  raft.Index

	// snapIndex and snapTerm answer for the entry at the snapshot boundary,
	// which no longer exists in the log. An AppendEntries consistency check can
	// legitimately reference exactly that index, so it has to be answerable.
	snapIndex raft.Index
	snapTerm  raft.Term

	hardState raft.HardState
}

var _ raft.Storage = (*Bolt)(nil)

// Open opens or creates the database at path, recovering any state in it.
func Open(path string, opts Options) (*Bolt, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("storage: creating %s: %w", dir, err)
		}
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:      opts.Timeout,
		NoSync:       opts.NoSync,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: opening %s: %w", path, err)
	}

	s := &Bolt{db: db, path: path}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// init creates the buckets and loads the cached state.
func (s *Bolt) init() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		log, err := tx.CreateBucketIfNotExists(bucketLog)
		if err != nil {
			return fmt.Errorf("storage: creating log bucket: %w", err)
		}
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return fmt.Errorf("storage: creating meta bucket: %w", err)
		}

		if raw := meta.Get(keyHardState); raw != nil {
			hs, err := codec.UnmarshalHardState(raw)
			if err != nil {
				return err
			}
			s.hardState = hs
		}
		if raw := meta.Get(keySnapshot); raw != nil {
			snap, err := codec.UnmarshalSnapshot(raw)
			if err != nil {
				return err
			}
			s.snapIndex, s.snapTerm = snap.LastIncludedIndex, snap.LastIncludedTerm
		}

		s.first = s.snapIndex + 1
		s.last = s.snapIndex
		if k, _ := log.Cursor().Last(); k != nil {
			s.last = decodeIndex(k)
		}
		if k, _ := log.Cursor().First(); k != nil {
			s.first = decodeIndex(k)
		}
		return nil
	})
}

// Path is where this store lives on disk.
func (s *Bolt) Path() string { return s.path }

func (s *Bolt) InitialState() (raft.HardState, raft.Snapshot, error) {
	snap, err := s.LoadSnapshot()
	if err != nil {
		return raft.HardState{}, raft.Snapshot{}, err
	}
	return s.hardState, snap, nil
}

func (s *Bolt) SetHardState(hs raft.HardState) error {
	if hs.Term < s.hardState.Term {
		return fmt.Errorf("storage: hard state term %d is below persisted term %d",
			hs.Term, s.hardState.Term)
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(keyHardState, codec.MarshalHardState(hs))
	})
	if err != nil {
		return fmt.Errorf("storage: persisting hard state: %w", err)
	}
	s.hardState = hs
	return nil
}

func (s *Bolt) FirstIndex() raft.Index { return s.first }
func (s *Bolt) LastIndex() raft.Index  { return s.last }

func (s *Bolt) Term(i raft.Index) (raft.Term, error) {
	if i == s.snapIndex && s.snapIndex > 0 {
		return s.snapTerm, nil
	}
	entry, err := s.GetEntry(i)
	if err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (s *Bolt) GetEntry(i raft.Index) (raft.LogEntry, error) {
	switch {
	case i < s.first:
		return raft.LogEntry{}, fmt.Errorf("storage: entry at %d, first available %d: %w",
			i, s.first, raft.ErrCompacted)
	case i > s.last:
		return raft.LogEntry{}, fmt.Errorf("storage: entry at %d, last index %d: %w",
			i, s.last, raft.ErrUnavailable)
	}

	var entry raft.LogEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketLog).Get(encodeIndex(i))
		if raw == nil {
			return fmt.Errorf("storage: index %d is within [%d,%d] but absent: %w",
				i, s.first, s.last, raft.ErrUnavailable)
		}
		var err error
		entry, err = codec.UnmarshalEntry(raw)
		return err
	})
	return entry, err
}

func (s *Bolt) Entries(lo, hi raft.Index) ([]raft.LogEntry, error) {
	if lo >= hi {
		return nil, nil
	}
	if lo < s.first {
		return nil, fmt.Errorf("storage: entries from %d, first available %d: %w",
			lo, s.first, raft.ErrCompacted)
	}
	if hi > s.last+1 {
		return nil, fmt.Errorf("storage: entries to %d, last index %d: %w",
			hi-1, s.last, raft.ErrUnavailable)
	}

	out := make([]raft.LogEntry, 0, hi-lo)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketLog).Cursor()
		// Big-endian keys make bbolt's byte order the same as numeric order, so
		// a range scan is a cursor walk rather than a lookup per index.
		for k, v := c.Seek(encodeIndex(lo)); k != nil && decodeIndex(k) < hi; k, v = c.Next() {
			entry, err := codec.UnmarshalEntry(v)
			if err != nil {
				return err
			}
			out = append(out, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Bolt) AppendEntries(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := checkContiguous(entries); err != nil {
		return err
	}

	firstNew, lastNew := entries[0].Index, entries[len(entries)-1].Index

	// Entirely covered by the snapshot: nothing left to store. Not an error — a
	// leader can legitimately resend entries a follower has since compacted.
	if lastNew <= s.snapIndex {
		return nil
	}
	if firstNew <= s.snapIndex {
		entries = entries[s.snapIndex+1-firstNew:]
		firstNew = entries[0].Index
	}
	if firstNew > s.last+1 {
		return fmt.Errorf("storage: append at %d leaves a gap after last index %d",
			firstNew, s.last)
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLog)

		// Truncate any conflicting suffix first. Overwriting a divergent tail is
		// the normal way a follower is brought back into agreement with a
		// leader; the core has already checked nothing committed is discarded.
		c := b.Cursor()
		for k, _ := c.Seek(encodeIndex(firstNew)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("storage: truncating at %d: %w", decodeIndex(k), err)
			}
		}

		for _, e := range entries {
			raw, err := codec.MarshalEntry(e)
			if err != nil {
				return err
			}
			if err := b.Put(encodeIndex(e.Index), raw); err != nil {
				return fmt.Errorf("storage: writing entry %d: %w", e.Index, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.last = lastNew
	if s.first > s.last {
		s.first = s.snapIndex + 1
	}
	return nil
}

func checkContiguous(entries []raft.LogEntry) error {
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

func (s *Bolt) SaveSnapshot(snap raft.Snapshot) error {
	if snap.LastIncludedIndex <= s.snapIndex {
		return fmt.Errorf("storage: snapshot at %d, already have %d: %w",
			snap.LastIncludedIndex, s.snapIndex, raft.ErrSnapshotOutOfDate)
	}

	raw, err := codec.MarshalSnapshot(snap)
	if err != nil {
		return err
	}

	// A snapshot that runs past our log replaces it entirely — the catch-up case
	// where a follower was too far behind to repair with entries. One that lands
	// inside our log leaves the tail intact.
	replaceLog := snap.LastIncludedIndex > s.last
	if !replaceLog {
		if term, terr := s.Term(snap.LastIncludedIndex); terr != nil || term != snap.LastIncludedTerm {
			replaceLog = true
		}
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Put(keySnapshot, raw); err != nil {
			return fmt.Errorf("storage: writing snapshot: %w", err)
		}
		if !replaceLog {
			return nil
		}
		if err := tx.DeleteBucket(bucketLog); err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("storage: clearing log: %w", err)
		}
		_, err := tx.CreateBucketIfNotExists(bucketLog)
		return err
	})
	if err != nil {
		return err
	}

	s.snapIndex, s.snapTerm = snap.LastIncludedIndex, snap.LastIncludedTerm
	if replaceLog {
		s.last = snap.LastIncludedIndex
	}
	s.first = s.snapIndex + 1
	return nil
}

func (s *Bolt) LoadSnapshot() (raft.Snapshot, error) {
	var snap raft.Snapshot
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketMeta).Get(keySnapshot)
		if raw == nil {
			return nil
		}
		var err error
		snap, err = codec.UnmarshalSnapshot(raw)
		return err
	})
	return snap, err
}

func (s *Bolt) Compact(upto raft.Index) error {
	switch {
	case upto <= s.snapIndex:
		return nil
	case upto > s.last:
		return fmt.Errorf("storage: compact to %d past last index %d", upto, s.last)
	}

	// Remember the boundary entry's term before discarding it: the consistency
	// check can still reference exactly that index.
	term, err := s.Term(upto)
	if err != nil {
		return fmt.Errorf("storage: reading term at compaction boundary %d: %w", upto, err)
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketLog).Cursor()
		for k, _ := c.First(); k != nil && decodeIndex(k) <= upto; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("storage: compacting index %d: %w", decodeIndex(k), err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.snapIndex, s.snapTerm = upto, term
	s.first = upto + 1
	return nil
}

func (s *Bolt) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	if err != nil {
		return fmt.Errorf("storage: closing %s: %w", s.path, err)
	}
	return nil
}

// Sync forces a flush, for the case where NoSync is set and a caller wants a
// durability point anyway.
func (s *Bolt) Sync() error { return s.db.Sync() }

// encodeIndex renders an index as a big-endian key so that bbolt's byte
// ordering matches numeric ordering. Little-endian would sort 10 before 9 and
// turn every range scan into a full walk.
func encodeIndex(i raft.Index) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i))
	return k[:]
}

func decodeIndex(k []byte) raft.Index {
	return raft.Index(binary.BigEndian.Uint64(k))
}
