package kvstore

import (
	"cmp"
	"slices"

	"github.com/sAchin-680/raftkv/raft"
)

// Raft guarantees a command is applied *at least* once, not exactly once.
//
// A client whose request times out cannot tell whether the entry committed —
// the answer was lost, not the command — so it retries, and the same command
// can reach the log twice. For a plain Set that is harmless to the map, since
// setting the same key to the same value twice leaves the same state.
//
// It is not harmless to the *history*. A linearizability checker sees a write
// that completed once and appears twice, or a delete that reports "existed" on
// the retry when the original already removed the key. Phase three's whole job
// is checking that history, and without deduplication it would be checking one
// this layer corrupted.
//
// So every write carries a client id and a sequence number, and the state
// machine remembers the last sequence it saw from each client along with the
// answer it gave. A repeat replays the stored answer instead of applying again.

// DefaultMaxSessions bounds how many clients are remembered.
//
// Sessions have to be bounded or a long-running cluster leaks memory in
// proportion to how many clients have ever connected. The paper suggests
// expiring them; a cap with deterministic eviction achieves the same thing
// without needing a replicated clock.
const DefaultMaxSessions = 4096

// session is what the state machine remembers about one client.
type session struct {
	// lastSeq is the highest sequence number applied from this client.
	lastSeq uint64

	// lastIndex is the log index of that command, used to choose an eviction
	// victim. It comes from the log, so every node computes the same value.
	lastIndex raft.Index

	// result is the answer given, replayed verbatim on a duplicate.
	result Result
}

// Result is what a write returns.
type Result struct {
	// Existed reports whether a Delete removed anything.
	Existed bool

	// Duplicate reports that this command had already been applied and the
	// stored answer was replayed. Surfaced so a client can tell the difference,
	// and so tests can assert deduplication actually happened.
	Duplicate bool
}

// checkSession decides whether a command is new, and returns the stored answer
// if it is not.
//
// A zero client id means no session, which is used for internal writes and for
// clients that have opted out of deduplication. Those are never deduplicated.
func (s *Store) checkSession(clientID, seq uint64) (Result, bool) {
	if clientID == 0 {
		return Result{}, false
	}
	prev, ok := s.sessions[clientID]
	if !ok || seq > prev.lastSeq {
		return Result{}, false
	}

	// A sequence number at or below the last one we applied. Replay the answer
	// rather than applying the command again.
	//
	// Strictly below, rather than only equal, because a client that retried an
	// older request out of order deserves the same treatment — and because
	// treating it as new would apply a command the client already considers
	// finished.
	result := prev.result
	result.Duplicate = true
	return result, true
}

// recordSession remembers what was applied for a client.
func (s *Store) recordSession(clientID, seq uint64, index raft.Index, result Result) {
	if clientID == 0 {
		return
	}
	s.sessions[clientID] = session{lastSeq: seq, lastIndex: index, result: result}
	s.evictSessions()
}

// evictSessions drops the least recently used sessions once over the cap.
//
// Determinism is the whole difficulty here. Eviction changes state machine
// state, so every node must evict exactly the same sessions or the replicas
// diverge — silently, and only visibly much later. The victim is chosen by last
// log index, with the client id breaking ties, and both of those come from the
// log itself. Choosing by wall-clock time, or by ranging a Go map, would not be
// reproducible across nodes.
func (s *Store) evictSessions() {
	if len(s.sessions) <= s.maxSessions {
		return
	}

	type candidate struct {
		id    uint64
		index raft.Index
	}
	candidates := make([]candidate, 0, len(s.sessions))
	for id, sess := range s.sessions {
		candidates = append(candidates, candidate{id: id, index: sess.lastIndex})
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		if c := cmp.Compare(a.index, b.index); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})

	for _, c := range candidates[:len(s.sessions)-s.maxSessions] {
		delete(s.sessions, c.id)
	}
}

// Sessions is how many clients are currently remembered.
func (s *Store) Sessions() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}
