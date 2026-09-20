package raft

import (
	"errors"
	"testing"
)

// settledLeader returns a leader whose no-op has committed, which every
// membership change needs before it can begin.
func settledLeader(t *testing.T, n *network) *RawNode {
	t.Helper()
	n.tickAll(11)
	leader := n.requireSingleLeader()
	n.tickAll(3)
	if !leader.committedInCurrentTerm() {
		t.Fatalf("setup: leader has not committed an entry of its own term\n%s", n.dump())
	}
	return leader
}

// ---------------------------------------------------------------------------
// The transition
// ---------------------------------------------------------------------------

func TestMembershipChangePassesThroughAJointConfiguration(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	// Only the leader is stepped, so the transition can be observed halfway.
	if _, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	// The configuration takes effect on append, before the entry has committed.
	conf := leader.Configuration()
	if !conf.IsJoint() {
		t.Fatalf("configuration is %s, want a joint one — a change must take "+
			"effect when appended, or it could never commit itself", conf)
	}
	if !equalIDs(conf.Voters, ids(1, 2, 3, 4)) {
		t.Errorf("new half = %v, want {1,2,3,4}", conf.Voters)
	}
	if !equalIDs(conf.OldVoters, ids(1, 2, 3)) {
		t.Errorf("old half = %v, want {1,2,3}", conf.OldVoters)
	}

	// Let it commit; leaving the joint configuration is automatic.
	n.deliver()
	n.tickAll(4)

	if conf := leader.Configuration(); conf.IsJoint() {
		t.Errorf("still joint after the change committed: %s — a cluster must "+
			"not be able to get stuck in a configuration that needs two "+
			"majorities for everything", conf)
	}
	if got := leader.Configuration(); !equalIDs(got.Voters, ids(1, 2, 3, 4)) {
		t.Errorf("final configuration = %s, want {1,2,3,4}", got)
	}
}

// The property joint consensus exists for.
//
// Moving from {1,2,3} to {3,4,5}, the two halves overlap only at node 3. While
// joint, no group can decide anything without reaching across the change — which
// is exactly what stops the two configurations electing separate leaders.
func TestJointConfigurationForcesTheTwoHalvesToOverlap(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	if _, err := leader.ProposeConfChange(NewConfiguration(ids(3, 4, 5))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	joint := leader.Configuration()
	if !joint.IsJoint() {
		t.Fatalf("configuration is %s, want joint", joint)
	}

	for _, tc := range []struct {
		name  string
		group []NodeID
		want  bool
	}{
		{"the whole old configuration", ids(1, 2, 3), false},
		{"the whole new configuration", ids(3, 4, 5), false},
		{"a majority of each", ids(2, 3, 4), true},
		{"the old majority without the overlap", ids(1, 2), false},
		{"the new majority without the overlap", ids(4, 5), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := joint.HasQuorum(inSet(tc.group))
			if got != tc.want {
				t.Errorf("HasQuorum(%v) = %t, want %t", tc.group, got, tc.want)
			}
		})
	}
}

func TestOnlyOneChangeAtATime(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	if _, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	// A second change while the first is still joint would reintroduce exactly
	// the ambiguity joint consensus removes.
	_, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4, 5)))
	if !errors.Is(err, ErrConfChangeInProgress) {
		t.Errorf("second change = %v, want ErrConfChangeInProgress", err)
	}
}

func TestConfChangeIsRefusedOnAFollower(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	for _, id := range n.ids {
		if id == leader.ID() {
			continue
		}
		if _, err := n.node(id).ProposeConfChange(NewConfiguration(ids(1, 2))); !errors.Is(err, ErrNotLeader) {
			t.Errorf("change on follower %d = %v, want ErrNotLeader", id, err)
		}
	}
}

func TestEmptyConfigurationIsRefused(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	_, err := leader.ProposeConfChange(Configuration{})
	if !errors.Is(err, ErrConfChangeInvalid) {
		t.Errorf("empty configuration = %v, want ErrConfChangeInvalid — a cluster "+
			"with no voters cannot commit anything, including the change that "+
			"would fix it", err)
	}
}

// ---------------------------------------------------------------------------
// Applied on append, and therefore revertible
// ---------------------------------------------------------------------------

// A configuration change takes effect before it commits, so it can be truncated
// away by a different leader — and the configuration has to go back with it.
//
// This is the awkward consequence of applying on append, and the reason the
// configuration is recomputed from the log rather than tracked incrementally.
func TestTruncatingAConfChangeRevertsTheConfiguration(t *testing.T) {
	// A follower that will receive a configuration change and then lose it.
	follower := newTestLogNode(t, 3)

	before := follower.Configuration()
	if before.IsJoint() {
		t.Fatalf("setup: configuration is already joint")
	}

	cc, err := before.EnterJoint(NewConfiguration(ids(1, 2, 3, 4, 5)))
	if err != nil {
		t.Fatalf("EnterJoint: %v", err)
	}

	// A leader of term 1 appends the change at index 1.
	if err := follower.Step(Message{
		Type: MsgAppendReq, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 1, Type: EntryConfChange, Command: cc.Encode()}},
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	follower.Messages()

	if got := follower.Configuration(); !got.IsJoint() {
		t.Fatalf("configuration = %s, want it to have taken effect on append", got)
	}

	// A different leader in term 2 overwrites that index with an ordinary entry.
	if err := follower.Step(Message{
		Type: MsgAppendReq, From: 3, To: 2, Term: 2,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 2, Type: EntryNormal, Command: []byte("x")}},
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	got := follower.Configuration()
	if got.IsJoint() {
		t.Errorf("configuration is still %s after the change was truncated away; "+
			"the node now believes in a membership no log contains", got)
	}
	if !got.Equal(before) {
		t.Errorf("configuration = %s, want it back at %s", got, before)
	}
}

// A node that restarts must recover its membership from durable state, not from
// whatever it was told at startup.
func TestConfigurationSurvivesRestart(t *testing.T) {
	storage := NewMemoryStorage()

	cc, err := NewConfiguration(ids(1, 2, 3)).EnterJoint(NewConfiguration(ids(1, 2, 3, 4)))
	if err != nil {
		t.Fatalf("EnterJoint: %v", err)
	}
	if err := storage.AppendEntries([]LogEntry{
		{Index: 1, Term: 1, Type: EntryNoOp},
		{Index: 2, Term: 1, Type: EntryConfChange, Command: cc.Encode()},
	}); err != nil {
		t.Fatalf("seeding log: %v", err)
	}

	// Started with a *stale* bootstrap list, as a node restarted from an old
	// command line would be. The log has to win.
	node, err := NewRawNode(Config{
		ID: 1, Storage: storage, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}

	got := node.Configuration()
	if !got.IsJoint() {
		t.Fatalf("configuration = %s, want the joint one from the log — a stale "+
			"startup flag must not silently rewrite the cluster", got)
	}
	if !equalIDs(got.Voters, ids(1, 2, 3, 4)) {
		t.Errorf("voters = %v, want {1,2,3,4}", got.Voters)
	}
}

func TestConfigurationComesBackFromASnapshot(t *testing.T) {
	storage := NewMemoryStorage()
	want := NewConfiguration(ids(4, 5, 6), 9)
	if err := storage.SaveSnapshot(Snapshot{
		LastIncludedIndex: 50, LastIncludedTerm: 3, Config: want,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	node, err := NewRawNode(Config{
		ID: 4, Storage: storage, ElectionTick: 10, HeartbeatTick: 1,
		Rand: fixedRand{}, Bootstrap: NewConfiguration(ids(1, 2, 3)),
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}
	if got := node.Configuration(); !got.Equal(want) {
		t.Errorf("configuration = %s, want %s — the snapshot replaced the entries "+
			"that established membership, so it has to carry it", got, want)
	}
}

// ---------------------------------------------------------------------------
// Leadership
// ---------------------------------------------------------------------------

// A leader removed from the cluster keeps leading until the change leaves the
// joint configuration, because while joint its vote still counts toward C_old —
// one of the two majorities the transition needs.
func TestRemovedLeaderStepsDownOnlyAfterLeavingJoint(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	if _, err := leader.ProposeConfChange(NewConfiguration(ids(2, 3))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	if leader.State() != Leader {
		t.Fatal("the leader stood down while still a member of the old half; " +
			"its votes are needed to complete the very change removing it")
	}

	n.deliver()
	n.tickAll(5)

	if leader.State() == Leader && !leader.Configuration().IsVoter(leader.ID()) {
		t.Errorf("node %d still leads a cluster it is not a member of: %s",
			leader.ID(), leader.Configuration())
	}
}

func TestLeaderTracksNewMembersImmediately(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	if _, ok := leader.Progress(4); ok {
		t.Fatal("setup: node 4 is already tracked")
	}
	if _, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	pr, ok := leader.Progress(4)
	if !ok {
		t.Fatal("the new member is not tracked; the leader would never send it " +
			"anything and the change could not commit")
	}
	if pr.Match != 0 {
		t.Errorf("new member starts at match %d, want 0 — nothing has been "+
			"confirmed by a node that just joined", pr.Match)
	}
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

func TestConfChangeEncodingRoundTrips(t *testing.T) {
	for _, want := range []ConfChange{
		{Kind: ConfChangeEnterJoint, Config: Configuration{
			Voters: ids(2, 3, 4), OldVoters: ids(1, 2, 3), Learners: ids(9),
		}},
		{Kind: ConfChangeLeaveJoint, Config: NewConfiguration(ids(2, 3, 4))},
		{Kind: ConfChangeEnterJoint, Config: NewConfiguration(ids(1))},
	} {
		got, err := DecodeConfChange(want.Encode())
		if err != nil {
			t.Fatalf("DecodeConfChange(%s): %v", want, err)
		}
		if got.Kind != want.Kind {
			t.Errorf("kind = %s, want %s", got.Kind, want.Kind)
		}
		if !got.Config.Equal(want.Config) {
			t.Errorf("config = %s, want %s", got.Config, want.Config)
		}
	}
}

func TestTruncatedConfChangeIsAnError(t *testing.T) {
	raw := ConfChange{
		Kind:   ConfChangeEnterJoint,
		Config: Configuration{Voters: ids(1, 2, 3), OldVoters: ids(1, 2)},
	}.Encode()

	for n := range len(raw) {
		if _, err := DecodeConfChange(raw[:n]); err == nil {
			t.Errorf("decoding %d of %d bytes succeeded; a half-read membership "+
				"would silently shrink the cluster", n, len(raw))
		}
	}
}

func TestDecodedConfigurationIsNormalized(t *testing.T) {
	raw := ConfChange{
		Kind:   ConfChangeEnterJoint,
		Config: Configuration{Voters: []NodeID{3, 1, 2, 1}},
	}.Encode()

	got, err := DecodeConfChange(raw)
	if err != nil {
		t.Fatalf("DecodeConfChange: %v", err)
	}
	if !equalIDs(got.Config.Voters, ids(1, 2, 3)) {
		t.Errorf("voters = %v, want them sorted and deduplicated: the core derives "+
			"ordered decisions from this slice", got.Config.Voters)
	}
}

func TestMembershipChangeCommitsThroughBothHalves(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	before := leader.CommitIndex()
	if _, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4))); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	n.deliver()
	n.tickAll(5)

	// Both halves of the transition are entries, so both have to commit.
	if got := leader.CommitIndex(); got < before+2 {
		t.Errorf("commit index advanced from %d to %d; entering and leaving the "+
			"joint configuration are two entries and both must commit",
			before, got)
	}
	if conf := leader.Configuration(); conf.IsJoint() {
		t.Errorf("configuration is still %s", conf)
	}
}

// The window the joint check alone does not cover.
//
// Once the entry entering the joint configuration commits, the leader appends
// the one leaving it — and the configuration stops looking joint immediately,
// because changes apply on append. But that second entry has not committed yet
// and a different leader could still truncate it away, which would put the
// cluster back in the joint configuration.
//
// Accepting a new change in that window means two overlapping transitions, which
// is exactly what joint consensus exists to rule out.
func TestChangeIsRefusedWhileLeavingJointIsUncommitted(t *testing.T) {
	n := newNetwork(t, ids(1, 2, 3), withJitter(map[NodeID]int{1: 0, 2: 5, 3: 7}))
	leader := settledLeader(t, n)

	enter, err := leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4)))
	if err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	leader.Messages()

	// Acknowledge only the entry that entered the joint configuration. That
	// commits it, which makes the leader append the one that leaves.
	for _, peer := range ids(2, 3) {
		if err := leader.Step(Message{
			Type: MsgAppendResp, From: peer, To: leader.ID(), Term: leader.Term(),
			Success: true, MatchIndex: enter,
		}); err != nil && !errors.Is(err, ErrIgnoredMessage) {
			t.Fatalf("Step: %v", err)
		}
	}
	leader.Messages()

	if leader.Configuration().IsJoint() {
		t.Fatalf("setup: still joint, so this test would only be re-checking "+
			"the joint guard\n%s", n.dump())
	}
	if leader.confIndex <= leader.CommitIndex() {
		t.Fatalf("setup: the leave entry at %d has already committed (commit=%d)",
			leader.confIndex, leader.CommitIndex())
	}

	_, err = leader.ProposeConfChange(NewConfiguration(ids(1, 2, 3, 4, 5)))
	if !errors.Is(err, ErrConfChangeInProgress) {
		t.Errorf("change = %v, want ErrConfChangeInProgress: the previous "+
			"transition is appended but not committed, and a different leader "+
			"could still truncate it back into the joint configuration", err)
	}
}
