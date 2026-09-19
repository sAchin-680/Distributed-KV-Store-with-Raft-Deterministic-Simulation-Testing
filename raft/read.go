package raft

import "errors"

// ErrReadIndexUnavailable means a linearizable read cannot be served yet
// because this leader has not committed an entry from its own term.
//
// It is transient and short-lived: a leader appends a no-op the instant it is
// elected precisely so this clears as soon as that entry commits. The caller
// should retry rather than fall back to a weaker read.
var ErrReadIndexUnavailable = errors.New("raft: read index not yet available")

// ReadState is a confirmed read barrier: at the moment this read was requested,
// everything up to Index was committed, and a quorum has since confirmed this
// node was still the leader.
//
// A read served once the state machine has applied Index is linearizable. It
// sees every write that completed before the read began, and none that began
// after it finished.
type ReadState struct {
	ID    uint64
	Index Index
}

// readRequest is a read barrier waiting for a quorum to confirm leadership.
type readRequest struct {
	id    uint64
	index Index
	acks  map[NodeID]bool
}

// ReadIndex begins a linearizable read.
//
// # Why a read needs any of this
//
// The obvious implementation — "I am the leader, so my state machine is
// current, so I will just read it" — is wrong, and wrong in the way that is
// hardest to notice: it returns plausible data.
//
// A leader that has been partitioned away does not know it. It has not heard
// from anyone, but nothing tells it that; from the inside, a partition and an
// idle cluster look identical. Meanwhile the other side elected a new leader and
// has been committing writes for some time. The old leader will happily serve
// reads from a state machine that stopped advancing, with no error and no
// warning, until its own election timeout eventually fires.
//
// # What this does instead
//
//  1. Record the current commit index. That is the read index: the point the
//     read must observe in order to be linearizable.
//  2. Confirm with a quorum that this node is still the leader, by exchanging
//     heartbeats. A quorum cannot simultaneously be acknowledging a different
//     leader in a later term, so this is proof, not a guess.
//  3. Once confirmed, wait for the state machine to apply the read index, and
//     only then read.
//
// The partitioned leader fails at step 2 and returns an error, which is the
// correct answer: it genuinely does not know.
//
// # The precondition
//
// The commit index only means what step 1 needs it to mean once this leader has
// committed an entry from its own term. Before that, entries committed by a
// previous leader may exist that this one has not yet learned about, so its
// commit index understates what the cluster has actually committed. The no-op a
// leader appends on election exists to clear this, and until it commits, reads
// return ErrReadIndexUnavailable.
//
// Callers pass an id to match the eventual ReadState to this request.
func (r *RawNode) ReadIndex(id uint64) error {
	if r.state != Leader {
		return ErrNotLeader
	}
	if !r.committedInCurrentTerm() {
		return ErrReadIndexUnavailable
	}

	req := &readRequest{
		id:    id,
		index: r.log.committed,
		acks:  map[NodeID]bool{r.id: true},
	}

	// A single-node cluster is its own quorum, so the confirmation is already
	// complete and no message is needed.
	if r.conf.HasQuorum(func(n NodeID) bool { return req.acks[n] }) {
		r.readStates = append(r.readStates, ReadState{ID: req.id, Index: req.index})
		return nil
	}

	r.pendingReads = append(r.pendingReads, req)

	// One heartbeat round per read.
	//
	// A read-heavy workload makes this expensive, and the standard fix is to
	// batch: several reads arriving close together share one confirmation
	// round, because a single quorum acknowledgement confirms leadership for
	// every read index recorded before it. Worth doing when a measurement asks
	// for it; not worth the extra state before then.
	for _, peer := range r.conf.Peers(r.id) {
		r.sendAppendWithRead(peer, id)
	}
	return nil
}

// committedInCurrentTerm reports whether this leader has committed an entry
// from its own term.
func (r *RawNode) committedInCurrentTerm() bool {
	if r.log.committed == 0 {
		return false
	}
	term, err := r.log.term(r.log.committed)
	return err == nil && term == r.term
}

// ackRead records that a peer confirmed our leadership for a read.
//
// Any response carrying our own term counts, whether or not the append itself
// succeeded. A follower that rejects entries on a log mismatch has still
// accepted that we are the leader of this term — which is the only thing the
// read barrier is asking. A response from a node that had moved on to a later
// term never reaches here; it makes us step down first.
func (r *RawNode) ackRead(from NodeID, readID uint64) {
	for i, req := range r.pendingReads {
		if req.id != readID {
			continue
		}
		req.acks[from] = true
		if r.conf.HasQuorum(func(n NodeID) bool { return req.acks[n] }) {
			r.readStates = append(r.readStates, ReadState{ID: req.id, Index: req.index})
			r.pendingReads = append(r.pendingReads[:i], r.pendingReads[i+1:]...)
		}
		return
	}
}

// ReadStates drains the read barriers confirmed since the last call, in the
// order they were confirmed.
//
// A slice rather than a map, so the order is the same on every run. Anything
// the caller derives from this ordering would otherwise vary between replays of
// the same seed, which is the one property the simulation framework cannot
// afford to lose.
func (r *RawNode) ReadStates() []ReadState {
	states := r.readStates
	r.readStates = nil
	return states
}

// PendingReads is how many read barriers are waiting for a quorum.
func (r *RawNode) PendingReads() int { return len(r.pendingReads) }

// dropPendingReads abandons every unconfirmed read.
//
// Called whenever this node stops being the leader of the term the reads were
// recorded in. The reads cannot be completed and must not be answered: their
// recorded index is only meaningful under the leadership that recorded it.
// Failing them makes the caller retry against whoever leads now, which is the
// honest outcome.
func (r *RawNode) dropPendingReads() {
	r.pendingReads = nil
	r.readStates = nil
}
