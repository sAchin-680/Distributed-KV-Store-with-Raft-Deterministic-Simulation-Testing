package raft

import (
	"errors"
	"fmt"
	"testing"
)

// electLeader brings up a cluster and returns its leader.
func electLeader(t *testing.T, n *network) *RawNode {
	t.Helper()
	n.tickAll(11)
	return n.requireSingleLeader()
}

func threeNode(t *testing.T) *network {
	t.Helper()
	return newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
}

// ---------------------------------------------------------------------------
// Proposing and replicating
// ---------------------------------------------------------------------------

func TestProposeReplicatesAndCommits(t *testing.T) {
	n := threeNode(t)
	leader := electLeader(t, n)

	idx := n.propose(leader.ID(), "x=1")
	if idx != 2 {
		t.Fatalf("index = %d, want 2 (1 is the leader's no-op)", idx)
	}

	if leader.CommitIndex() != idx {
		t.Errorf("leader commit = %d, want %d", leader.CommitIndex(), idx)
	}
	for _, id := range ids(2, 3) {
		f := n.node(id)
		if f.LastIndex() != idx {
			t.Errorf("node %d last index = %d, want %d", id, f.LastIndex(), idx)
		}
		if f.CommitIndex() != idx {
			t.Errorf("node %d commit = %d, want %d", id, f.CommitIndex(), idx)
		}
	}
	n.requireLogMatching()
	n.requireCommittedPrefixesAgree()
}

func TestProposeOnFollowerIsRefused(t *testing.T) {
	n := threeNode(t)
	leader := electLeader(t, n)

	for _, id := range n.ids {
		if id == leader.ID() {
			continue
		}
		if _, err := n.node(id).Propose([]byte("nope")); !errors.Is(err, ErrNotLeader) {
			t.Errorf("Propose on follower %d = %v, want ErrNotLeader", id, err)
		}
	}
}

func TestSingleNodeCommitsWithoutSendingAnything(t *testing.T) {
	n := newNetwork(t, ids(1), withJitter(map[NodeID]int{1: 0}))
	leader := electLeader(t, n)

	idx := n.propose(1, "x=1")
	if leader.CommitIndex() != idx {
		t.Errorf("commit = %d, want %d — one node is its own quorum",
			leader.CommitIndex(), idx)
	}
}

func TestCommitRequiresAQuorum(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3, 4, 5),
		withJitter(map[NodeID]int{1: 0, 2: 5, 3: 6, 4: 7, 5: 8}))
	leader := electLeader(t, n)

	// Cut the leader off from three of its four followers: it keeps only node 2,
	// which is two nodes out of five.
	n.isolate(3, 4, 5)

	idx, err := leader.Propose([]byte("x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.deliver()

	if leader.CommitIndex() >= idx {
		t.Errorf("commit = %d, but only 2 of 5 nodes hold index %d",
			leader.CommitIndex(), idx)
	}

	// Reconnect one node: three of five is a quorum, so it commits.
	n.partitioned[3] = false
	n.tick(leader.ID(), 2)

	if leader.CommitIndex() != idx {
		t.Errorf("commit = %d, want %d once a quorum has it\n%s",
			leader.CommitIndex(), idx, n.dump())
	}
}

// ---------------------------------------------------------------------------
// The commit rule (Figure 8)
// ---------------------------------------------------------------------------

// figure8Leader builds the state of S1 at Figure 8(c): leader in term 4, log
// holding an entry from term 1, a stranded entry from term 2, and its own
// term-4 no-op.
func figure8Leader(t *testing.T) *RawNode {
	t.Helper()

	st := NewMemoryStorage()
	if err := st.AppendEntries([]LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2}, // replicated to a minority back in term 2
	}); err != nil {
		t.Fatalf("seeding log: %v", err)
	}
	if err := st.SetHardState(HardState{Term: 3}); err != nil {
		t.Fatalf("seeding hard state: %v", err)
	}

	node, err := NewRawNode(Config{
		ID: 1, Storage: st, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3, 4, 5)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}

	node.becomeCandidate() // term 4
	if err := node.becomeLeader(); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}
	node.Messages() // discard the initial broadcast

	if node.Term() != 4 || node.LastIndex() != 3 {
		t.Fatalf("setup wrong: term=%d last=%d, want 4 and 3", node.Term(), node.LastIndex())
	}
	return node
}

// The case the rule exists for.
//
// A majority now holds the term-2 entry at index 2. Quorum arithmetic alone says
// commit it. Doing so loses data: S5 can still win an election at term 5 — its
// log ends at term 3, which beats term 2 — and overwrite index 2 everywhere. A
// client would have been told a write succeeded that then vanished.
func TestCommitRuleRefusesEntriesFromEarlierTerms(t *testing.T) {
	leader := figure8Leader(t)

	// Nodes 2 and 3 now match index 2. With the leader itself that is 3 of 5.
	leader.progress[2].Match = 2
	leader.progress[3].Match = 2

	advanced, err := leader.maybeAdvanceCommit()
	if err != nil {
		t.Fatalf("maybeAdvanceCommit: %v", err)
	}

	if advanced || leader.CommitIndex() != 0 {
		t.Errorf("committed index %d: a majority holds index 2, but it is from term 2 "+
			"and the leader is in term 4. Committing here is Figure 8 — the entry can "+
			"still be overwritten by a later leader.", leader.CommitIndex())
	}
}

// The other half of the rule: once an entry from the leader's own term reaches a
// majority, it commits — and everything beneath it commits with it.
func TestCommitRuleCommitsEarlierTermsIndirectly(t *testing.T) {
	leader := figure8Leader(t)

	// Nodes 2 and 3 have caught up all the way to the term-4 no-op at index 3.
	leader.progress[2].Match = 3
	leader.progress[3].Match = 3

	advanced, err := leader.maybeAdvanceCommit()
	if err != nil {
		t.Fatalf("maybeAdvanceCommit: %v", err)
	}

	if !advanced || leader.CommitIndex() != 3 {
		t.Fatalf("commit = %d, want 3: index 3 is from the leader's own term and is on "+
			"a majority", leader.CommitIndex())
	}

	// Index 2 is now committed too, indirectly. It was never committed on its
	// own merits — it rode along beneath a same-term entry, which is the only
	// safe way for it to commit at all.
	term, err := leader.log.term(2)
	if err != nil {
		t.Fatalf("term(2): %v", err)
	}
	if term != 2 {
		t.Errorf("term at index 2 = %d, want 2", term)
	}
}

// A leader must not commit an index no quorum has reached, even one from its own
// term.
func TestCommitRuleStillRequiresAQuorum(t *testing.T) {
	leader := figure8Leader(t)

	leader.progress[2].Match = 3 // only one follower, plus the leader = 2 of 5

	advanced, err := leader.maybeAdvanceCommit()
	if err != nil {
		t.Fatalf("maybeAdvanceCommit: %v", err)
	}
	if advanced || leader.CommitIndex() != 0 {
		t.Errorf("commit = %d, want 0: 2 of 5 is not a quorum", leader.CommitIndex())
	}
}

// ---------------------------------------------------------------------------
// Log matching and repair
// ---------------------------------------------------------------------------

func TestLeaderRepairsDivergentFollowerLog(t *testing.T) {
	// Node 3 carries a tail from a leader that was partitioned away: entries at
	// terms 4 and 5 that never committed. Nodes 1 and 2 have the real history.
	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(false),
		withJitter(map[NodeID]int{1: 0, 2: 5, 3: 9}),
		withLog(1, 1, 6, 6),
		withLog(2, 1, 6, 6),
		withLog(3, 1, 4, 4, 5, 5),
	)

	leader := electLeader(t, n)
	if leader.ID() != 1 {
		t.Fatalf("leader = %d, want 1", leader.ID())
	}
	n.tickAll(5)

	want := n.logTerms(1)
	if got := n.logTerms(3); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("node 3 log = %v, want %v — the divergent suffix should have been "+
			"truncated and replaced", got, want)
	}
	n.requireLogMatching()
}

// The conflict hint should let a leader skip a whole term per round trip.
//
// The saving only shows up when the leader has a long log of its own: the cost
// of backing up one index at a time is proportional to how far the leader must
// walk back from its own last index, not to where the divergence began.
func TestConflictHintSkipsWholeTermsNotSingleIndexes(t *testing.T) {
	// Nodes 1 and 2: one entry from term 1, then 29 from term 6.
	good := []Term{1}
	for i := 0; i < 29; i++ {
		good = append(good, 6)
	}
	// Node 3 diverges from index 2 onward — 30 entries, but only two terms.
	stale := []Term{1}
	for i := 0; i < 15; i++ {
		stale = append(stale, 4)
	}
	for i := 0; i < 15; i++ {
		stale = append(stale, 5)
	}

	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(false),
		withJitter(map[NodeID]int{1: 0, 2: 5, 3: 9}),
		withLog(1, good...),
		withLog(2, good...),
		withLog(3, stale...),
	)

	// Elect with node 3 cut off, so the repair has not already happened by the
	// time we start counting. (An earlier version of this test measured nothing
	// for exactly that reason.)
	n.isolate(3)
	leader := electLeader(t, n)
	n.heal()

	// Drive the leader and node 3 by hand, counting round trips.
	if err := leader.Tick(); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	rejections := 0
	for round := 0; round < 200; round++ {
		var toFollower []Message
		for _, m := range leader.Messages() {
			if m.To == 3 {
				toFollower = append(toFollower, m)
			}
		}
		if len(toFollower) == 0 {
			break
		}
		for _, m := range toFollower {
			if err := n.node(3).Step(m); err != nil && !errors.Is(err, ErrIgnoredMessage) {
				t.Fatalf("Step on node 3: %v", err)
			}
			for _, resp := range n.node(3).Messages() {
				if !resp.Success {
					rejections++
				}
				if err := leader.Step(resp); err != nil && !errors.Is(err, ErrIgnoredMessage) {
					t.Fatalf("Step on leader: %v", err)
				}
			}
		}
	}

	// With the hint: one round trip per divergent term, so two or three. Without
	// it: one per index the leader walks back, around thirty.
	if rejections > 5 {
		t.Errorf("took %d rejections to repair a 30-entry divergence spanning 2 terms; "+
			"the conflict hint should make this a handful, not one per entry", rejections)
	}
	if got, want := fmt.Sprint(n.logTerms(3)), fmt.Sprint(n.logTerms(1)); got != want {
		t.Errorf("node 3 log = %v, want %v", got, want)
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	n := threeNode(t)
	leader := electLeader(t, n)

	// It already holds the leader's no-op from the election; it must receive
	// nothing further while cut off.
	before := n.node(3).LastIndex()

	n.isolate(3)
	for i := 0; i < 20; i++ {
		n.propose(leader.ID(), fmt.Sprintf("x=%d", i))
	}
	if got := n.node(3).LastIndex(); got != before {
		t.Fatalf("isolated node advanced from %d to %d", before, got)
	}

	n.heal()
	n.tickAll(10)

	if got, want := n.node(3).LastIndex(), leader.LastIndex(); got != want {
		t.Errorf("node 3 last index = %d, want %d\n%s", got, want, n.dump())
	}
	if got, want := n.node(3).CommitIndex(), leader.CommitIndex(); got != want {
		t.Errorf("node 3 commit = %d, want %d", got, want)
	}
	n.requireLogMatching()
	n.requireCommittedPrefixesAgree()
}

// Entries are capped per message so a far-behind follower does not provoke one
// message the size of the whole log.
func TestAppendEntriesIsBounded(t *testing.T) {
	n := threeNode(t)
	for _, id := range n.ids {
		n.node(id).cfg.MaxEntriesPerMessage = 4
	}
	leader := electLeader(t, n)

	n.isolate(3)
	for i := 0; i < 20; i++ {
		n.propose(leader.ID(), fmt.Sprintf("x=%d", i))
	}
	n.heal()

	if err := leader.Tick(); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	for _, m := range leader.Messages() {
		if len(m.Entries) > 4 {
			t.Errorf("message carried %d entries, cap is 4", len(m.Entries))
		}
	}
}

// ---------------------------------------------------------------------------
// State Machine Safety under leader churn
// ---------------------------------------------------------------------------

// Committed entries must survive repeated leader changes. This is the property
// the commit rule protects; the simulator will hammer it much harder, but it
// should already hold under ordinary failover.
func TestCommittedEntriesSurviveLeaderChurn(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3, 4, 5))
	n.tickAll(30)
	n.requireSingleLeader()

	for round := 0; round < 5; round++ {
		leader := n.requireSingleLeader()
		for i := 0; i < 3; i++ {
			n.propose(leader.ID(), fmt.Sprintf("r%d-%d", round, i))
		}
		n.requireCommittedPrefixesAgree()

		// Kill the leader and let the rest elect a new one.
		n.isolate(leader.ID())
		n.tickAll(60)
		n.heal()
		n.tickAll(30)

		n.requireNoTwoLeadersPerTerm()
		n.requireLogMatching()
		n.requireCommittedPrefixesAgree()
	}
}

// ---------------------------------------------------------------------------
// Applying
// ---------------------------------------------------------------------------

func TestCommittedEntriesAreDrainedInOrderAndOnlyOnce(t *testing.T) {
	n := threeNode(t)
	leader := electLeader(t, n)

	for i := 0; i < 5; i++ {
		n.propose(leader.ID(), fmt.Sprintf("x=%d", i))
	}

	var applied []Index
	for leader.HasCommittedEntries() {
		ents, err := leader.CommittedEntries()
		if err != nil {
			t.Fatalf("CommittedEntries: %v", err)
		}
		if len(ents) == 0 {
			t.Fatal("HasCommittedEntries said yes but none were returned")
		}
		for _, e := range ents {
			applied = append(applied, e.Index)
		}
		leader.ApplyTo(ents[len(ents)-1].Index)
	}

	if len(applied) != int(leader.CommitIndex()) {
		t.Fatalf("applied %d entries, commit index is %d", len(applied), leader.CommitIndex())
	}
	for i, idx := range applied {
		if idx != Index(i+1) {
			t.Fatalf("applied out of order at position %d: %v", i, applied)
		}
	}
	if leader.AppliedIndex() != leader.CommitIndex() {
		t.Errorf("applied = %d, commit = %d", leader.AppliedIndex(), leader.CommitIndex())
	}
}

func TestApplyIsBoundedByMaxApplyEntries(t *testing.T) {
	n := threeNode(t)
	leader := electLeader(t, n)
	leader.cfg.MaxApplyEntries = 2

	for i := 0; i < 5; i++ {
		n.propose(leader.ID(), fmt.Sprintf("x=%d", i))
	}

	ents, err := leader.CommittedEntries()
	if err != nil {
		t.Fatalf("CommittedEntries: %v", err)
	}
	if len(ents) != 2 {
		t.Errorf("got %d entries, want 2 — a large backlog must drain in bounded chunks",
			len(ents))
	}
}

func TestEntriesAreOnlyAppliedAfterTheyCommit(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3, 4, 5),
		withJitter(map[NodeID]int{1: 0, 2: 5, 3: 6, 4: 7, 5: 8}))
	leader := electLeader(t, n)

	// Drain what the election itself committed.
	for leader.HasCommittedEntries() {
		ents, err := leader.CommittedEntries()
		if err != nil {
			t.Fatalf("CommittedEntries: %v", err)
		}
		leader.ApplyTo(ents[len(ents)-1].Index)
	}

	n.isolate(2, 3, 4, 5)
	if _, err := leader.Propose([]byte("x=1")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.deliver()

	if leader.HasCommittedEntries() {
		t.Error("an uncommitted entry was offered to the state machine; a command " +
			"applied before it commits can still be overwritten")
	}
}
