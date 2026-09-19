package raft

import (
	"math/rand"
	"testing"
)

// ---------------------------------------------------------------------------
// Electing a leader
// ---------------------------------------------------------------------------

func TestSingleNodeElectsItself(t *testing.T) {
	n := newNetwork(t, ids(1))
	n.tick(1, 20)

	leader := n.requireSingleLeader()
	if leader.ID() != 1 {
		t.Fatalf("leader = %d, want 1", leader.ID())
	}
	// A single node is its own quorum, so no round trip is needed at all.
	if leader.Term() != 1 {
		t.Errorf("term = %d, want 1", leader.Term())
	}
}

func TestThreeNodeClusterElectsExactlyOneLeader(t *testing.T) {
	// Node 1 has the shortest timeout, so it campaigns first.
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))

	n.tickAll(11)

	leader := n.requireSingleLeader()
	if leader.ID() != 1 {
		t.Fatalf("leader = %d, want 1 (shortest election timeout)", leader.ID())
	}
	for _, id := range ids(2, 3) {
		if got := n.node(id).State(); got != Follower {
			t.Errorf("node %d is %s, want follower", id, got)
		}
		if got := n.node(id).Lead(); got != 1 {
			t.Errorf("node %d follows %d, want 1", id, got)
		}
	}
}

func TestFiveNodeClusterElectsExactlyOneLeader(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3, 4, 5),
		withJitter(map[NodeID]int{1: 9, 2: 9, 3: 0, 4: 9, 5: 9}))

	n.tickAll(11)

	if leader := n.requireSingleLeader(); leader.ID() != 3 {
		t.Fatalf("leader = %d, want 3", leader.ID())
	}
}

// A leader appends an empty entry of its own term the instant it is elected.
// Without one, the commit rule leaves it permanently unable to commit the
// backlog it inherited from earlier terms.
func TestLeaderAppendsNoOpOnElection(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	n.tickAll(11)

	leader := n.requireSingleLeader()
	last := leader.LastIndex()
	if last != 1 {
		t.Fatalf("last index = %d, want 1", last)
	}

	entry, err := leader.log.storage.GetEntry(last)
	if err != nil {
		t.Fatalf("GetEntry(%d): %v", last, err)
	}
	if entry.Type != EntryNoOp {
		t.Errorf("entry type = %s, want %s", entry.Type, EntryNoOp)
	}
	if entry.Term != leader.Term() {
		t.Errorf("entry term = %d, want the leader's own term %d", entry.Term, leader.Term())
	}
}

// ---------------------------------------------------------------------------
// The election restriction, end to end
// ---------------------------------------------------------------------------

// A node whose log is behind cannot win, no matter how eagerly it campaigns.
// This is Leader Completeness in practice: a leader missing a committed entry
// could overwrite it, so the restriction refuses to elect one.
func TestCandidateWithStaleLogCannotWin(t *testing.T) {
	// Nodes 2 and 3 hold three entries ending in term 5. Node 1 holds one stale
	// entry from term 1 — and campaigns first.
	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(false),
		withJitter(map[NodeID]int{1: 0, 2: 8, 3: 9}),
		withLog(1, 1),
		withLog(2, 1, 5, 5),
		withLog(3, 1, 5, 5),
	)

	n.tick(1, 11) // node 1 alone times out and campaigns

	if got := n.node(1).State(); got == Leader {
		t.Fatalf("node 1 became leader with a stale log\n%s", n.dump())
	}
	for _, id := range ids(2, 3) {
		if v := n.node(id).Vote(); v == 1 {
			t.Errorf("node %d voted for the stale candidate", id)
		}
	}
}

// The converse: the node with the most up-to-date log wins even when it
// campaigns last.
func TestUpToDateCandidateWins(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(false),
		withJitter(map[NodeID]int{1: 9, 2: 9, 3: 0}),
		withLog(1, 1, 1),
		withLog(2, 1, 1),
		withLog(3, 1, 1, 7),
	)

	n.tickAll(11)

	if leader := n.requireSingleLeader(); leader.ID() != 3 {
		t.Fatalf("leader = %d, want 3 (the only up-to-date log)\n%s", leader.ID(), n.dump())
	}
}

// ---------------------------------------------------------------------------
// Election Safety
// ---------------------------------------------------------------------------

func TestNodeVotesAtMostOncePerTerm(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withPreVote(false))
	voter := n.node(3)

	ask := func(candidate NodeID) bool {
		if err := voter.Step(Message{
			Type: MsgVoteReq, From: candidate, To: 3, Term: 5,
			LastLogIndex: 0, LastLogTerm: 0,
		}); err != nil {
			t.Fatalf("Step: %v", err)
		}
		msgs := voter.Messages()
		if len(msgs) != 1 {
			t.Fatalf("want one reply, got %d", len(msgs))
		}
		return msgs[0].Granted
	}

	if !ask(1) {
		t.Fatal("first candidate should have been granted the vote")
	}
	if ask(2) {
		t.Fatal("second candidate in the same term must be refused")
	}
	// Asking again from the same candidate is safe and must stay granted, so a
	// lost reply cannot stall an election.
	if !ask(1) {
		t.Error("re-request from the same candidate must still be granted")
	}
}

func TestVoteIsDurableBeforeItIsGranted(t *testing.T) {
	st := NewMemoryStorage()
	node, err := NewRawNode(Config{
		ID: 1, Storage: st, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}

	if err := node.Step(Message{Type: MsgVoteReq, From: 2, To: 1, Term: 4}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	// The grant is only safe if it is already on disk. Simulate a crash by
	// reading the durable state back.
	hs, _, err := st.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs.Term != 4 || hs.Vote != 2 {
		t.Errorf("persisted %s, want term 4 vote 2 — a forgotten vote lets a node "+
			"vote twice in one term and elect two leaders", hs)
	}
}

// A split vote must not deadlock. Randomized timeouts are what break the tie,
// so this runs many seeds and requires every one of them to converge.
func TestSplitVotesAlwaysResolve(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic by design
		n := newNetwork(t, ids(1, 2, 3, 4, 5))
		for _, id := range n.ids {
			n.nodes[id].cfg.Rand = seededRand{rand.New(rand.NewSource(rng.Int63()))} //nolint:gosec // deterministic by design
		}

		// Long enough for several rounds of elections if the first ones tie.
		n.tickAll(200)

		n.requireNoTwoLeadersPerTerm()
		if got := n.leaders(); len(got) != 1 {
			t.Fatalf("seed %d: leaders = %v, want exactly one\n%s", seed, got, n.dump())
		}
	}
}

// ---------------------------------------------------------------------------
// Terms
// ---------------------------------------------------------------------------

func TestHigherTermCausesImmediateStepDown(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	n.tickAll(11)
	leader := n.requireSingleLeader()

	// Any message from a later term ends this leader's authority at once, even
	// a vote request it is about to refuse.
	if err := leader.Step(Message{
		Type: MsgVoteReq, From: 3, To: leader.ID(), Term: leader.Term() + 5,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if leader.State() != Follower {
		t.Errorf("state = %s, want follower after seeing a higher term", leader.State())
	}
	if leader.Lead() != None {
		t.Errorf("lead = %d, want None", leader.Lead())
	}
}

func TestStaleLeaderStepsDownOnRejection(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	n.tickAll(11)
	stale := n.requireSingleLeader()

	// Node 3 moves on to a much later term without the leader noticing.
	follower := n.node(3)
	follower.becomeFollower(stale.Term()+3, None)

	// The stale leader's heartbeat is refused, and the refusal carries the real
	// term. Silence here would leave it broadcasting to a cluster that has
	// moved on.
	n.tick(stale.ID(), 2)

	if stale.State() == Leader {
		t.Errorf("stale leader did not step down\n%s", n.dump())
	}
}

// ---------------------------------------------------------------------------
// Pre-vote
// ---------------------------------------------------------------------------

// The scenario pre-vote exists for. A node is partitioned away and campaigns
// repeatedly; when it returns, the healthy leader must be undisturbed.
func TestPreVotePreventsDisruptionByRejoiningNode(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(true),
		withJitter(map[NodeID]int{1: 0, 2: 8, 3: 9}))

	n.tickAll(11)
	leader := n.requireSingleLeader()
	termBefore := leader.Term()

	// Node 3 loses contact and campaigns into the void for a long time.
	n.isolate(3)
	n.tick(3, 300)

	if got := n.node(3).Term(); got != termBefore {
		t.Errorf("isolated node's term = %d, want %d unchanged — a pre-vote must not "+
			"advance the term", got, termBefore)
	}

	// It rejoins and is allowed to catch up first, so that when it campaigns
	// its log is current and the election restriction has nothing to object to.
	// Otherwise the refusal would be over-determined and this test would pass
	// even with the lease check removed.
	n.heal()
	n.tickAll(5)
	if got, want := n.node(3).LastIndex(), leader.LastIndex(); got != want {
		t.Fatalf("rejoining node did not catch up: last index %d, want %d", got, want)
	}

	// Now it tries again, rather than waiting for a heartbeat to put it back in
	// its place — otherwise this would only test that heartbeats arrive.
	if err := n.node(3).Campaign(); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	n.deliver()
	n.tickAll(5)

	if leader.State() != Leader {
		t.Errorf("leader was disrupted by the rejoining node\n%s", n.dump())
	}
	if leader.Term() != termBefore {
		t.Errorf("term rose to %d, want %d — nothing happened that needed a new term",
			leader.Term(), termBefore)
	}
	n.requireNoTwoLeadersPerTerm()
}

// The same scenario with pre-vote off, showing what it is buying. This is a
// characterization test: it documents the disruption rather than approving of it.
func TestWithoutPreVoteARejoiningNodeDisruptsTheLeader(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3),
		withPreVote(false),
		withJitter(map[NodeID]int{1: 0, 2: 8, 3: 9}))

	n.tickAll(11)
	leader := n.requireSingleLeader()
	termBefore := leader.Term()

	n.isolate(3)
	n.tick(3, 300)

	isolatedTerm := n.node(3).Term()
	if isolatedTerm <= termBefore {
		t.Fatalf("isolated node's term = %d, expected it to run ahead of %d",
			isolatedTerm, termBefore)
	}

	n.heal()
	n.tickAll(2)

	if leader.State() == Leader && leader.Term() == termBefore {
		t.Errorf("expected the inflated term to force the leader down; "+
			"it stayed in office at term %d\n%s", termBefore, n.dump())
	}
	if maxTerm(n.terms()) < isolatedTerm {
		t.Errorf("cluster terms %v did not absorb the inflated term %d",
			n.terms(), isolatedTerm)
	}
}

func TestPreVoteDoesNotPersistAnything(t *testing.T) {
	st := NewMemoryStorage()
	node, err := NewRawNode(Config{
		ID: 1, Storage: st, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}

	if err := node.Step(Message{
		Type: MsgVoteReq, From: 2, To: 1, Term: 9, PreVote: true,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	msgs := node.Messages()
	if len(msgs) != 1 || !msgs[0].Granted {
		t.Fatalf("want one granted pre-vote reply, got %v", msgs)
	}
	if msgs[0].Term != 9 {
		t.Errorf("reply term = %d, want 9 — a pre-vote reply answers the term that "+
			"was asked about", msgs[0].Term)
	}

	hs, _, err := st.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs.Term != 0 || hs.Vote != None {
		t.Errorf("persisted %s, want nothing — a straw poll commits to nothing", hs)
	}
	if node.Term() != 0 {
		t.Errorf("term = %d, want 0 — answering a pre-vote must not advance our term",
			node.Term())
	}
}

// A node that is hearing from a healthy leader refuses pre-votes. That refusal
// is the entire mechanism: it is what makes the previous tests differ.
func TestPreVoteIsRefusedWhileALeaderIsAlive(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 8, 3: 9}))
	n.tickAll(11)
	n.requireSingleLeader()

	follower := n.node(2)

	// The candidate's log is deliberately as up to date as the follower's, so
	// that the election restriction has no reason to refuse. The only thing
	// left that can say no is the lease check — which is the point of the test.
	if err := follower.Step(Message{
		Type: MsgVoteReq, From: 3, To: 2, Term: follower.Term() + 1, PreVote: true,
		LastLogIndex: follower.LastIndex(), LastLogTerm: follower.log.lastTerm(),
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	msgs := follower.Messages()
	if len(msgs) != 1 {
		t.Fatalf("want one reply, got %d", len(msgs))
	}
	if msgs[0].Granted {
		t.Error("pre-vote granted while a leader is alive; the rejoining-node " +
			"protection depends on this being refused")
	}
}

// Regression: a refused pre-vote must reply with the responder's own term, not
// the hypothetical term it was asked about.
//
// Echoing the asked-about term hands the pre-candidate a term one greater than
// its own. It adopts it, and then forces a healthy leader to step down — so the
// straw poll ends up causing exactly the disruption it was added to prevent.
func TestRejectedPreVoteRepliesWithTheRespondersOwnTerm(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 8, 3: 9}))
	n.tickAll(11)
	n.requireSingleLeader()

	follower := n.node(2)
	askedAbout := follower.Term() + 1

	if err := follower.Step(Message{
		Type: MsgVoteReq, From: 3, To: 2, Term: askedAbout, PreVote: true,
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	msgs := follower.Messages()
	if len(msgs) != 1 || msgs[0].Granted {
		t.Fatalf("want one refusal, got %v", msgs)
	}
	if msgs[0].Term != follower.Term() {
		t.Errorf("refusal carries term %d, want the responder's own term %d; "+
			"echoing %d would let the refused candidate adopt it and depose the leader",
			msgs[0].Term, follower.Term(), askedAbout)
	}
}

// ---------------------------------------------------------------------------
// Eligibility
// ---------------------------------------------------------------------------

func TestLearnerNeverCampaigns(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withLearners(4),
		withJitter(map[NodeID]int{1: 5, 2: 7, 3: 9, 4: 0}))

	// Node 4 has the shortest timeout, so if learners could campaign it would
	// go first. The voters get distinct timeouts so that a split vote, which a
	// fixed random source could never break, is not what decides this test.
	n.tickAll(30)

	if got := n.node(4).State(); got != Follower {
		t.Errorf("learner is %s, want follower — learners have no vote to win with", got)
	}
	if leader := n.requireSingleLeader(); leader.ID() == 4 {
		t.Fatal("a learner became leader")
	}
}

func TestLearnerIsNotAskedToVote(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withLearners(4), withPreVote(false),
		withJitter(map[NodeID]int{1: 0, 2: 9, 3: 9, 4: 9}))

	candidate := n.node(1)
	if err := candidate.Campaign(); err != nil {
		t.Fatalf("Campaign: %v", err)
	}

	for _, m := range candidate.Messages() {
		if m.Type == MsgVoteReq && m.To == 4 {
			t.Error("vote solicited from a learner")
		}
	}
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

func TestElectionTimeoutIsRandomizedWithinRange(t *testing.T) {
	const electionTick = 10
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic by design

	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		node, err := NewRawNode(Config{
			ID: 1, Storage: NewMemoryStorage(),
			ElectionTick: electionTick, HeartbeatTick: 1,
			Rand:      seededRand{rng},
			Bootstrap: NewConfiguration(ids(1, 2, 3)),
		})
		if err != nil {
			t.Fatalf("NewRawNode: %v", err)
		}
		got := node.randomizedElectionTimeout
		if got < electionTick || got >= 2*electionTick {
			t.Fatalf("timeout %d outside [%d, %d)", got, electionTick, 2*electionTick)
		}
		seen[got] = true
	}

	// A fixed timeout would make every node campaign in lockstep and split the
	// vote indefinitely.
	if len(seen) < 2 {
		t.Errorf("election timeout is not actually randomized: only saw %v", seen)
	}
}

func TestHeartbeatTickMustBeBelowElectionTick(t *testing.T) {
	_, err := NewRawNode(Config{
		ID: 1, Storage: NewMemoryStorage(),
		ElectionTick: 5, HeartbeatTick: 5,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1)),
	})
	if err == nil {
		t.Fatal("expected a configuration error: a leader that cannot heartbeat " +
			"within one election timeout is campaigned against by its own followers")
	}
}

func TestHeartbeatsSuppressElections(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	n.tickAll(11)
	leader := n.requireSingleLeader()
	term := leader.Term()

	// Far longer than any election timeout. The leader's heartbeats must keep
	// the followers from ever timing out.
	n.tickAll(500)

	if got := n.requireSingleLeader(); got.ID() != leader.ID() {
		t.Errorf("leadership moved to %d; heartbeats should have held it", got.ID())
	}
	if leader.Term() != term {
		t.Errorf("term drifted from %d to %d under a healthy leader", term, leader.Term())
	}
}
