package raft

import (
	"errors"
	"fmt"
)

// A log that only grows is a log that eventually fills the disk and makes every
// restart replay the whole history of the cluster. Snapshotting folds a prefix
// of it into a single image of the state machine, and discards the entries that
// image already accounts for.
//
// The consequence is that a follower can fall behind the point where the
// leader's log begins. There are no entries left to send it, so the leader sends
// the image instead.

// ErrNothingToSnapshot means no entry has been applied yet, so there is no state
// worth capturing.
var ErrNothingToSnapshot = errors.New("raft: nothing applied to snapshot")

// defaultSnapshotRetryHeartbeats is how many heartbeats pass before a snapshot
// whose reply never arrived is sent again.
//
// Deliberately slow: the failure it recovers from is rare, and the thing it
// retries is the most expensive message the cluster sends.
const defaultSnapshotRetryHeartbeats = 10

// ShouldSnapshot reports whether the log has grown past the configured
// threshold and is worth compacting.
//
// The decision is the driver's, not the core's, because the driver owns the
// state machine and is the only thing that can serialize it.
func (r *RawNode) ShouldSnapshot() bool {
	if r.cfg.SnapshotThreshold == 0 || r.log.applied == 0 {
		return false
	}
	first := r.log.firstIndex()
	if r.log.applied < first {
		return false
	}
	return uint64(r.log.applied-first+1) >= r.cfg.SnapshotThreshold
}

// CreateSnapshot folds everything applied so far into a snapshot and compacts
// the log behind it.
//
// data is the serialized state machine, which the caller produces. The boundary
// is the applied index and not the commit index: a snapshot claims the state
// machine reflects every entry up to that point, and entries that are committed
// but not yet applied are not reflected in anything.
func (r *RawNode) CreateSnapshot(data []byte) (Snapshot, error) {
	applied := r.log.applied
	if applied == 0 {
		return Snapshot{}, ErrNothingToSnapshot
	}

	term, err := r.log.term(applied)
	if err != nil {
		return Snapshot{}, fmt.Errorf("raft: term at applied index %d: %w", applied, err)
	}

	snap := Snapshot{
		LastIncludedIndex: applied,
		LastIncludedTerm:  term,
		// The configuration has to travel with the image. Restoring from a
		// snapshot discards the entries that established the cluster's
		// membership, so without it a node comes back not knowing who its peers
		// are: unable to campaign, count a quorum, or tell a legitimate leader
		// from a stranger.
		Config: r.conf.Clone(),
		Data:   data,
	}

	if err := r.cfg.Storage.SaveSnapshot(snap); err != nil {
		if errors.Is(err, ErrSnapshotOutOfDate) {
			return Snapshot{}, err
		}
		return Snapshot{}, fmt.Errorf("raft: saving snapshot at %d: %w", applied, err)
	}

	// Keep a tail of entries past the snapshot point.
	//
	// Compacting all the way to the boundary means any follower even one entry
	// behind needs the entire state machine sent to it. Retaining a tail lets
	// those followers be repaired with entries, which is enormously cheaper —
	// and a follower being briefly behind is the normal case, not an exception.
	compactTo := applied
	if keep := Index(r.cfg.SnapshotCatchUpEntries); keep > 0 {
		if applied <= keep {
			return snap, nil
		}
		compactTo = applied - keep
	}
	if compactTo <= r.log.firstIndex()-1 {
		return snap, nil
	}
	if err := r.log.compact(compactTo); err != nil {
		return Snapshot{}, fmt.Errorf("raft: compacting to %d: %w", compactTo, err)
	}
	return snap, nil
}

// SnapshotToApply returns a snapshot the state machine must restore from, and
// clears it.
//
// The core can install a snapshot into the log but cannot install one into the
// application; only the driver knows how to rebuild the state machine. This is
// how the driver finds out it has to.
func (r *RawNode) SnapshotToApply() (Snapshot, bool) {
	if r.appliedSnapshot == nil {
		return Snapshot{}, false
	}
	snap := *r.appliedSnapshot
	r.appliedSnapshot = nil
	return snap, true
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

// sendSnapshot ships the whole state machine to a follower that has fallen
// behind the start of our log.
func (r *RawNode) sendSnapshot(to NodeID) {
	pr, ok := r.progress[to]
	if !ok {
		return
	}

	// One at a time. A snapshot can be large, and re-sending it on every
	// heartbeat while the first is still in flight would saturate the link to
	// exactly the follower that is already struggling.
	if pr.PendingSnapshot != 0 {
		return
	}

	snap, err := r.cfg.Storage.LoadSnapshot()
	if err != nil || snap.IsEmpty() {
		// Nothing to send. The follower stays behind until a snapshot exists,
		// which is correct: there is no way to repair it and inventing one
		// would be worse.
		return
	}

	pr.PendingSnapshot = snap.LastIncludedIndex
	pr.SnapshotTicks = 0
	r.send(Message{Type: MsgSnapshotReq, To: to, Snapshot: &snap})
}

// expirePendingSnapshots lets a snapshot be retried when its reply never came.
//
// Without this a single lost response strands a follower permanently: it is too
// far behind for entries, and the leader believes a snapshot is already on its
// way. The retry is deliberately slow, because the failure it recovers from is
// rare and the thing it retries is expensive.
func (r *RawNode) expirePendingSnapshots() {
	retryAfter := r.cfg.SnapshotRetryHeartbeats
	if retryAfter <= 0 {
		retryAfter = defaultSnapshotRetryHeartbeats
	}

	for _, id := range r.conf.Members() {
		pr, ok := r.progress[id]
		if !ok || pr.PendingSnapshot == 0 {
			continue
		}
		pr.SnapshotTicks++
		if pr.SnapshotTicks >= retryAfter {
			pr.PendingSnapshot = 0
			pr.SnapshotTicks = 0
		}
	}
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------

func (r *RawNode) handleSnapshotRequest(m Message) error {
	r.electionElapsed = 0
	if r.state != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.lead = m.From

	if m.Snapshot == nil {
		return fmt.Errorf("raft: snapshot message from %d carries none: %w",
			m.From, ErrIgnoredMessage)
	}
	snap := *m.Snapshot

	// Already covered by our own log. Accepting it would discard entries past
	// the snapshot boundary that we legitimately hold.
	if snap.LastIncludedIndex <= r.log.committed {
		r.send(Message{
			Type: MsgSnapshotResp, To: m.From,
			Success: true, MatchIndex: r.log.committed,
		})
		return nil
	}

	if err := r.log.restore(snap); err != nil {
		if errors.Is(err, ErrSnapshotOutOfDate) {
			r.send(Message{
				Type: MsgSnapshotResp, To: m.From,
				Success: true, MatchIndex: r.log.committed,
			})
			return nil
		}
		return fmt.Errorf("raft: restoring snapshot at %d: %w", snap.LastIncludedIndex, err)
	}

	// Adopt the configuration the snapshot carries. The entries that would have
	// told us the membership are exactly what the snapshot replaced.
	if !snap.Config.IsEmpty() {
		r.conf = snap.Config.Clone()
	}

	// Hand it to the driver, which is the only thing that can rebuild the
	// application state.
	stored := snap
	r.appliedSnapshot = &stored

	if err := r.persistHardState(); err != nil {
		return err
	}
	r.send(Message{
		Type: MsgSnapshotResp, To: m.From,
		Success: true, MatchIndex: snap.LastIncludedIndex,
	})
	return nil
}

func (r *RawNode) handleSnapshotResponse(m Message) error {
	if r.state != Leader {
		return fmt.Errorf("raft: snapshot response to non-leader: %w", ErrIgnoredMessage)
	}
	pr, ok := r.progress[m.From]
	if !ok {
		return fmt.Errorf("raft: snapshot response from untracked peer %d: %w",
			m.From, ErrIgnoredMessage)
	}

	pr.PendingSnapshot = 0
	pr.SnapshotTicks = 0

	if !m.Success {
		// The follower refused it. Fall back to entries; if it is still behind
		// our log's start, the next attempt sends a snapshot again.
		return nil
	}

	if m.MatchIndex > pr.Match {
		pr.Match = m.MatchIndex
		pr.Next = pr.Match + 1

		advanced, err := r.maybeAdvanceCommit()
		if err != nil {
			return err
		}
		if advanced {
			r.broadcastAppend()
			return nil
		}
	}
	if pr.Next <= r.log.lastIndex() {
		r.sendAppend(m.From)
	}
	return nil
}
