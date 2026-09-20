package node

import (
	"context"
	"fmt"
	"time"

	"github.com/sAchin-680/raftkv/raft"
)

// confChangeRequest is one membership change waiting on the run loop.
type confChangeRequest struct {
	target raft.Configuration
	done   chan confChangeResult
}

type confChangeResult struct {
	index raft.Index
	err   error
}

// ChangeMembership moves the cluster to a new set of voters and returns once
// the whole transition has committed.
//
// The transition is two entries — entering the joint configuration and leaving
// it — and this waits for both. Returning after the first would hand back a
// cluster that still needs majorities of two configurations for everything,
// which is less available than either one alone.
//
// Leaving is driven by the leader rather than by this call, so a client that
// disappears halfway cannot strand the cluster joint. This only watches.
func (n *Node) ChangeMembership(ctx context.Context, voters []raft.NodeID) (raft.Configuration, error) {
	target := raft.NewConfiguration(voters)
	if err := target.Validate(); err != nil {
		return raft.Configuration{}, err
	}

	req := &confChangeRequest{target: target, done: make(chan confChangeResult, 1)}
	select {
	case n.confCh <- req:
	case <-ctx.Done():
		return raft.Configuration{}, ctx.Err()
	case <-n.doneCh:
		return raft.Configuration{}, ErrStopped
	}

	select {
	case res := <-req.done:
		if res.err != nil {
			return raft.Configuration{}, res.err
		}
	case <-ctx.Done():
		return raft.Configuration{}, ctx.Err()
	case <-n.doneCh:
		return raft.Configuration{}, ErrStopped
	}

	return n.awaitSettled(ctx, target)
}

// awaitSettled waits for the cluster to leave the joint configuration.
func (n *Node) awaitSettled(ctx context.Context, target raft.Configuration) (raft.Configuration, error) {
	for {
		status, err := n.Status(ctx)
		if err != nil {
			return raft.Configuration{}, err
		}
		if !status.Config.IsJoint() {
			if !status.Config.Equal(target) {
				// Leadership changed and someone else's change won, or ours was
				// truncated. Either way the caller asked for something that did
				// not happen and should be told so rather than left guessing.
				return status.Config, fmt.Errorf(
					"node: the cluster settled on %s rather than the requested %s",
					status.Config, target)
			}
			return status.Config, nil
		}

		select {
		case <-ctx.Done():
			return raft.Configuration{}, fmt.Errorf(
				"node: membership change did not complete, cluster is still %s: %w",
				status.Config, ctx.Err())
		case <-n.doneCh:
			return raft.Configuration{}, ErrStopped
		case <-n.cfg.Clock.NewTimer(n.cfg.TickInterval).C():
		}
	}
}

// handleConfChange runs on the run loop.
func (n *Node) handleConfChange(req *confChangeRequest) {
	index, err := n.rn.ProposeConfChange(req.target)
	req.done <- confChangeResult{index: index, err: err}
}

// AwaitConfiguration blocks until this node's configuration matches want, which
// is how a newly added node's operator can tell the change has reached it.
func (n *Node) AwaitConfiguration(ctx context.Context, want raft.Configuration, timeout time.Duration) error {
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		status, err := n.Status(deadline)
		if err != nil {
			return err
		}
		if status.Config.Equal(want) {
			return nil
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("node: configuration is %s, want %s: %w",
				status.Config, want, deadline.Err())
		case <-n.doneCh:
			return ErrStopped
		case <-n.cfg.Clock.NewTimer(n.cfg.TickInterval).C():
		}
	}
}
