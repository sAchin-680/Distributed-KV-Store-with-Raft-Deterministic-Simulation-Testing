package node

import (
	"context"
	"fmt"

	"github.com/sAchin-680/raftkv/raft"
)

// readRequest is one linearizable read waiting on the run loop.
type readRequest struct {
	id   uint64
	done chan readResult
}

type readResult struct {
	index raft.Index
	err   error
}

// ReadIndex returns an index the caller must wait for the state machine to
// reach before reading, having first confirmed with a quorum that this node is
// still the leader.
//
// It is the safe half of a linearizable read. The unsafe half — reading the
// local state machine because this node believes it is the leader — returns
// plausible, stale data from a partitioned leader with no error at all. See
// docs/adr/0003-read-index.md.
//
// A read that cannot be confirmed returns an error rather than a best guess.
// "I do not know" is a correct answer; stale data presented as current is not.
func (n *Node) ReadIndex(ctx context.Context) (raft.Index, error) {
	req := &readRequest{done: make(chan readResult, 1)}

	select {
	case n.readCh <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.doneCh:
		return 0, ErrStopped
	}

	select {
	case res := <-req.done:
		return res.index, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.doneCh:
		return 0, ErrStopped
	}
}

// AwaitApplied blocks until the state machine has applied through index.
//
// Paired with ReadIndex this completes a linearizable read: the barrier says
// which index the read must observe, and this waits for the state machine to
// get there. Polling the run loop rather than subscribing keeps the loop free
// of per-waiter bookkeeping; the wait is bounded by replication latency, which
// is milliseconds.
func (n *Node) AwaitApplied(ctx context.Context, index raft.Index) error {
	for {
		status, err := n.Status(ctx)
		if err != nil {
			return err
		}
		if status.AppliedIndex >= index {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("node: waiting for index %d, applied %d: %w",
				index, status.AppliedIndex, ctx.Err())
		case <-n.doneCh:
			return ErrStopped
		case <-n.cfg.Clock.NewTimer(n.cfg.TickInterval / 4).C():
		}
	}
}

// LinearizableRead runs read once it is safe to do so.
//
// The sequence is: confirm leadership with a quorum, wait for the state machine
// to reach the index that was committed at that moment, then read. Every step is
// necessary; dropping any of them gives a read that is usually right.
func (n *Node) LinearizableRead(ctx context.Context, read func() error) error {
	index, err := n.ReadIndex(ctx)
	if err != nil {
		return err
	}
	if err := n.AwaitApplied(ctx, index); err != nil {
		return err
	}
	return read()
}

// handleRead runs on the run loop.
func (n *Node) handleRead(req *readRequest) {
	n.readSeq++
	req.id = n.readSeq

	if err := n.rn.ReadIndex(req.id); err != nil {
		req.done <- readResult{err: err}
		return
	}
	n.pendingReads[req.id] = req
}

// resolveReads answers the barriers the core has confirmed.
func (n *Node) resolveReads() {
	for _, state := range n.rn.ReadStates() {
		req, ok := n.pendingReads[state.ID]
		if !ok {
			continue
		}
		delete(n.pendingReads, state.ID)
		req.done <- readResult{index: state.Index}
	}
}

// failReads abandons every waiting read.
//
// Called when leadership is lost or the node stops. A barrier's index is only
// meaningful under the leadership that recorded it, so the reads cannot be
// answered — and answering them anyway is precisely the stale read this whole
// mechanism exists to prevent.
func (n *Node) failReads(err error) {
	for id, req := range n.pendingReads {
		delete(n.pendingReads, id)
		req.done <- readResult{err: err}
	}
}
