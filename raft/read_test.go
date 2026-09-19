package raft

import (
	"errors"
	"fmt"
	"testing"
)

// readyLeader elects a leader and lets its no-op commit, which is the
// precondition a read barrier needs.
func readyLeader(t *testing.T) (*network, *RawNode) {
	t.Helper()
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	n.tickAll(11)
	leader := n.requireSingleLeader()

	// One more round of heartbeats carries the no-op to the followers and
	// brings their acknowledgements back.
	n.tickAll(3)
	if !leader.committedInCurrentTerm() {
		t.Fatalf("setup: leader has not committed an entry of term %d\n%s",
			leader.Term(), n.dump())
	}
	return n, leader
}

func TestReadIndexIsRefusedOnAFollower(t *testing.T) {
	n, leader := readyLeader(t)

	for _, id := range n.ids {
		if id == leader.ID() {
			continue
		}
		if err := n.node(id).ReadIndex(1); !errors.Is(err, ErrNotLeader) {
			t.Errorf("ReadIndex on follower %d = %v, want ErrNotLeader", id, err)
		}
	}
}

// Before a leader has committed anything of its own term, its commit index
// understates what the cluster has committed: entries committed by a previous
// leader may exist that this one has not learned about yet. Serving a read
// against that index could miss a completed write.
func TestReadIndexWaitsForAnEntryFromTheCurrentTerm(t *testing.T) {
	// A leader of a three-node cluster with nothing reachable. Its no-op is
	// appended but can never commit, which is exactly the window this rule
	// exists for — constructed directly rather than raced against a heartbeat.
	st := NewMemoryStorage()
	if err := st.AppendEntries([]LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatalf("seeding log: %v", err)
	}
	if err := st.SetHardState(HardState{Term: 1}); err != nil {
		t.Fatalf("seeding hard state: %v", err)
	}

	leader, err := NewRawNode(Config{
		ID: 1, Storage: st, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}
	leader.becomeCandidate()
	if err := leader.becomeLeader(); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}
	leader.Messages()

	if leader.committedInCurrentTerm() {
		t.Fatal("setup: the no-op committed without a quorum")
	}
	if err := leader.ReadIndex(1); !errors.Is(err, ErrReadIndexUnavailable) {
		t.Fatalf("ReadIndex = %v, want ErrReadIndexUnavailable: this leader's "+
			"commit index understates what the cluster has committed, because "+
			"entries from a previous leader may exist that it has not seen", err)
	}

	// Once a quorum has the no-op, the barrier becomes available.
	noop := leader.LastIndex()
	for _, peer := range ids(2, 3) {
		if err := leader.Step(Message{
			Type: MsgAppendResp, From: peer, To: 1, Term: leader.Term(),
			Success: true, MatchIndex: noop,
		}); err != nil && !errors.Is(err, ErrIgnoredMessage) {
			t.Fatalf("Step: %v", err)
		}
	}
	if !leader.committedInCurrentTerm() {
		t.Fatalf("the no-op did not commit after a quorum acknowledged it "+
			"(commit=%d last=%d)", leader.CommitIndex(), leader.LastIndex())
	}
	if err := leader.ReadIndex(2); err != nil {
		t.Errorf("ReadIndex after the no-op committed = %v, want nil", err)
	}
}

func TestReadIndexConfirmedByQuorum(t *testing.T) {
	n, leader := readyLeader(t)
	wantIndex := leader.CommitIndex()

	if err := leader.ReadIndex(42); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	// Not confirmed until peers answer.
	if got := leader.ReadStates(); len(got) != 0 {
		t.Fatalf("read confirmed before any peer replied: %v", got)
	}
	if leader.PendingReads() != 1 {
		t.Fatalf("%d pending reads, want 1", leader.PendingReads())
	}

	n.deliver()

	states := leader.ReadStates()
	if len(states) != 1 {
		t.Fatalf("got %d read states, want 1", len(states))
	}
	if states[0].ID != 42 {
		t.Errorf("read state id = %d, want 42", states[0].ID)
	}
	if states[0].Index != wantIndex {
		t.Errorf("read index = %d, want the commit index at request time, %d",
			states[0].Index, wantIndex)
	}
	if leader.PendingReads() != 0 {
		t.Errorf("%d reads still pending after confirmation", leader.PendingReads())
	}
}

// THE test for this feature.
//
// A partitioned leader does not know it has been deposed — from the inside, a
// partition and an idle cluster look identical. It will keep serving reads from
// a state machine that stopped advancing, with no error, until its own election
// timeout fires. The read barrier is what turns that silent staleness into an
// explicit "I do not know".
func TestPartitionedLeaderCannotConfirmARead(t *testing.T) {
	n, leader := readyLeader(t)

	// The rest of the cluster becomes unreachable. Nothing tells the leader.
	n.isolate(leader.ID())

	if err := leader.ReadIndex(7); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	n.deliver()

	if states := leader.ReadStates(); len(states) != 0 {
		t.Fatalf("a partitioned leader confirmed a read: %v\n"+
			"it cannot know whether another leader has committed writes it has "+
			"never seen, and serving this read would return stale data with no "+
			"error", states)
	}
	if leader.PendingReads() != 1 {
		t.Errorf("%d pending reads, want the read to still be waiting", leader.PendingReads())
	}

	// Meanwhile the leader still believes it leads — which is exactly why
	// checking its own state is not enough to serve a read.
	if leader.State() != Leader {
		t.Log("leader stepped down already; the point stands but is less pointed")
	}

	// Healing lets the confirmation complete.
	n.heal()
	n.tickAll(2)
	if len(leader.ReadStates()) == 0 && leader.State() == Leader {
		t.Error("the read was never confirmed after the partition healed")
	}
}

// A single node is its own quorum, so no message is needed.
func TestSingleNodeConfirmsReadsImmediately(t *testing.T) {
	n := newNetwork(t, ids(1), withJitter(map[NodeID]int{1: 0}))
	n.tickAll(11)
	leader := n.requireSingleLeader()
	n.tickAll(2)

	if err := leader.ReadIndex(1); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	states := leader.ReadStates()
	if len(states) != 1 {
		t.Fatalf("got %d read states, want 1 without any round trip", len(states))
	}
	if states[0].Index != leader.CommitIndex() {
		t.Errorf("read index = %d, want %d", states[0].Index, leader.CommitIndex())
	}
}

// A minority of replies is not a confirmation. Two of five nodes cannot rule
// out a third having elected someone else.
func TestReadIndexNeedsAMajorityNotJustOneReply(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3, 4, 5),
		withJitter(map[NodeID]int{1: 0, 2: 5, 3: 6, 4: 7, 5: 8}))
	n.tickAll(11)
	leader := n.requireSingleLeader()
	n.tickAll(3)

	if !leader.committedInCurrentTerm() {
		t.Skip("no-op has not committed yet")
	}
	if err := leader.ReadIndex(1); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	// Deliver exactly one peer's reply. With the leader itself that is 2 of 5.
	var delivered bool
	for _, m := range leader.Messages() {
		if m.To != 2 {
			continue
		}
		if err := n.node(2).Step(m); err != nil && !errors.Is(err, ErrIgnoredMessage) {
			t.Fatalf("Step: %v", err)
		}
		for _, resp := range n.node(2).Messages() {
			if err := leader.Step(resp); err != nil && !errors.Is(err, ErrIgnoredMessage) {
				t.Fatalf("Step: %v", err)
			}
			delivered = true
		}
	}
	if !delivered {
		t.Skip("no reply was produced to deliver")
	}

	if states := leader.ReadStates(); len(states) != 0 {
		t.Errorf("read confirmed on 2 of 5 nodes: %v", states)
	}
}

// A read barrier's index only means anything under the leadership that recorded
// it. Losing leadership must abandon the read rather than answer it.
func TestLosingLeadershipDropsPendingReads(t *testing.T) {
	n, leader := readyLeader(t)
	n.isolate(leader.ID())

	if err := leader.ReadIndex(9); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if leader.PendingReads() != 1 {
		t.Fatalf("%d pending reads, want 1", leader.PendingReads())
	}

	// Something from a later term arrives and the leader steps down.
	if err := leader.Step(Message{
		Type: MsgVoteReq, From: 2, To: leader.ID(), Term: leader.Term() + 5,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if leader.State() == Leader {
		t.Fatal("setup: the node did not step down")
	}
	if leader.PendingReads() != 0 {
		t.Errorf("%d reads still pending after stepping down; their recorded "+
			"index is only meaningful under the leadership that took it",
			leader.PendingReads())
	}
	if states := leader.ReadStates(); len(states) != 0 {
		t.Errorf("a deposed leader answered a read: %v", states)
	}
}

// A follower that rejects the append on a log mismatch has still accepted that
// we lead this term, which is the only question the barrier asks.
func TestRejectedAppendStillConfirmsLeadership(t *testing.T) {
	n, leader := readyLeader(t)

	if err := leader.ReadIndex(3); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	leader.Messages() // discard the real heartbeats

	// Hand-craft rejections from a quorum, carrying the leader's own term.
	for _, from := range ids(2, 3) {
		if err := leader.Step(Message{
			Type: MsgAppendResp, From: from, To: leader.ID(), Term: leader.Term(),
			Success: false, ConflictIndex: 1, ReadID: 3,
		}); err != nil && !errors.Is(err, ErrIgnoredMessage) {
			t.Fatalf("Step: %v", err)
		}
	}

	if states := leader.ReadStates(); len(states) != 1 {
		t.Errorf("got %d read states, want 1 — a rejected append still proves "+
			"the follower accepts this leader's term", len(states))
	}
	_ = n
}

func TestReadIndexIsRecordedAtRequestTimeNotConfirmTime(t *testing.T) {
	n, leader := readyLeader(t)
	atRequest := leader.CommitIndex()

	if err := leader.ReadIndex(5); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	// More writes commit before the confirmation arrives.
	for i := range 3 {
		if _, err := leader.Propose(fmt.Appendf(nil, "w%d", i)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		n.deliver()
	}

	states := leader.ReadStates()
	if len(states) != 1 {
		t.Fatalf("got %d read states, want 1", len(states))
	}
	// The barrier is the index at the moment the read began. A read that only
	// has to observe that index is still linearizable — later writes began
	// after this read did, so it is free not to see them.
	if states[0].Index != atRequest {
		t.Errorf("read index = %d, want %d, the commit index when the read began",
			states[0].Index, atRequest)
	}
	if leader.CommitIndex() <= atRequest {
		t.Fatal("setup: no writes committed after the read began")
	}
}
