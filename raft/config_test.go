package raft

import (
	"errors"
	"testing"
)

func ids(v ...uint64) []NodeID {
	out := make([]NodeID, len(v))
	for i, x := range v {
		out[i] = NodeID(x)
	}
	return out
}

func inSet(members []NodeID) func(NodeID) bool {
	set := make(map[NodeID]bool, len(members))
	for _, m := range members {
		set[m] = true
	}
	return func(id NodeID) bool { return set[id] }
}

func TestConfigurationNormalizes(t *testing.T) {
	c := NewConfiguration(ids(3, 1, 2, 1), ids(4, 2)...)

	if got, want := c.Voters, ids(1, 2, 3); !equalIDs(got, want) {
		t.Errorf("voters = %v, want %v (must be sorted and deduplicated)", got, want)
	}
	// 2 was listed as both a voter and a learner. Voter wins: treating it as a
	// learner would quietly drop it out of the quorum arithmetic.
	if got, want := c.Learners, ids(4); !equalIDs(got, want) {
		t.Errorf("learners = %v, want %v", got, want)
	}
}

func TestQuorumSimpleMajority(t *testing.T) {
	c := NewConfiguration(ids(1, 2, 3))

	tests := []struct {
		name    string
		granted []NodeID
		want    bool
	}{
		{"none", nil, false},
		{"one of three", ids(1), false},
		{"two of three", ids(1, 3), true},
		{"all three", ids(1, 2, 3), true},
		{"two, one of them a stranger", ids(1, 99), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.HasQuorum(inSet(tc.granted)); got != tc.want {
				t.Errorf("HasQuorum(%v) = %t, want %t", tc.granted, got, tc.want)
			}
		})
	}
}

// The property that makes joint consensus safe: during the transition a
// decision needs a majority of BOTH configurations. A set that is a majority of
// only one half decides nothing, which is why two disjoint majorities can never
// elect two leaders at the same instant.
func TestQuorumJointRequiresBothHalves(t *testing.T) {
	// Moving from {1,2,3} to {3,4,5} — the halves overlap only at node 3.
	old := NewConfiguration(ids(1, 2, 3))
	cc, err := old.EnterJoint(NewConfiguration(ids(3, 4, 5)))
	if err != nil {
		t.Fatalf("EnterJoint: %v", err)
	}
	joint := cc.Config

	if !joint.IsJoint() {
		t.Fatal("configuration should be joint")
	}

	tests := []struct {
		name    string
		granted []NodeID
		want    bool
	}{
		{"majority of C_old only", ids(1, 2), false},
		{"majority of C_new only", ids(4, 5), false},
		{"majority of both", ids(1, 2, 3, 4), true},
		{"the minimal overlapping set", ids(2, 3, 4), true},
		{"everyone", ids(1, 2, 3, 4, 5), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := joint.HasQuorum(inSet(tc.granted)); got != tc.want {
				t.Errorf("HasQuorum(%v) = %t, want %t", tc.granted, got, tc.want)
			}
		})
	}
}

func TestQuorumEmptyConfigurationDecidesNothing(t *testing.T) {
	var c Configuration
	if c.HasQuorum(func(NodeID) bool { return true }) {
		t.Error("an empty configuration must never report a quorum")
	}
	if err := c.Validate(); !errors.Is(err, ErrEmptyConfiguration) {
		t.Errorf("Validate() = %v, want ErrEmptyConfiguration", err)
	}
}

func TestCommittedIndexIsTheQuorumMedian(t *testing.T) {
	match := func(m map[NodeID]Index) func(NodeID) Index {
		return func(id NodeID) Index { return m[id] }
	}

	tests := []struct {
		name  string
		conf  Configuration
		match map[NodeID]Index
		want  Index
	}{
		{
			name:  "three nodes, two agree at 5",
			conf:  NewConfiguration(ids(1, 2, 3)),
			match: map[NodeID]Index{1: 5, 2: 5, 3: 2},
			want:  5,
		},
		{
			name:  "three nodes, only the leader has it",
			conf:  NewConfiguration(ids(1, 2, 3)),
			match: map[NodeID]Index{1: 9, 2: 3, 3: 3},
			want:  3,
		},
		{
			name:  "five nodes need three",
			conf:  NewConfiguration(ids(1, 2, 3, 4, 5)),
			match: map[NodeID]Index{1: 10, 2: 10, 3: 7, 4: 1, 5: 0},
			want:  7,
		},
		{
			name:  "four nodes still need three",
			conf:  NewConfiguration(ids(1, 2, 3, 4)),
			match: map[NodeID]Index{1: 8, 2: 8, 3: 4, 4: 4},
			want:  4,
		},
		{
			name:  "learners never count",
			conf:  NewConfiguration(ids(1, 2, 3), 4, 5),
			match: map[NodeID]Index{1: 9, 2: 2, 3: 2, 4: 9, 5: 9},
			want:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.conf.CommittedIndex(match(tc.match)); got != tc.want {
				t.Errorf("CommittedIndex() = %d, want %d", got, tc.want)
			}
		})
	}
}

// A joint configuration commits only as fast as its slower half.
func TestCommittedIndexJointTakesTheMinimum(t *testing.T) {
	old := NewConfiguration(ids(1, 2, 3))
	cc, err := old.EnterJoint(NewConfiguration(ids(4, 5, 6)))
	if err != nil {
		t.Fatalf("EnterJoint: %v", err)
	}

	// C_old is fully caught up at 10; C_new has only one node past 3.
	m := map[NodeID]Index{1: 10, 2: 10, 3: 10, 4: 10, 5: 3, 6: 3}
	got := cc.Config.CommittedIndex(func(id NodeID) Index { return m[id] })

	if want := Index(3); got != want {
		t.Errorf("CommittedIndex() = %d, want %d — the new half has not caught up", got, want)
	}
}

func TestEnterAndLeaveJoint(t *testing.T) {
	start := NewConfiguration(ids(1, 2, 3))

	cc, err := start.EnterJoint(NewConfiguration(ids(2, 3, 4)))
	if err != nil {
		t.Fatalf("EnterJoint: %v", err)
	}
	if !equalIDs(cc.Config.Voters, ids(2, 3, 4)) || !equalIDs(cc.Config.OldVoters, ids(1, 2, 3)) {
		t.Fatalf("joint config = %s, want voters {2,3,4} over old {1,2,3}", cc.Config)
	}

	// A second change cannot start while one is in flight.
	if _, err := cc.Config.EnterJoint(NewConfiguration(ids(5))); err == nil {
		t.Error("EnterJoint during a joint configuration should be refused")
	}

	final, err := cc.Config.LeaveJoint()
	if err != nil {
		t.Fatalf("LeaveJoint: %v", err)
	}
	if final.Config.IsJoint() {
		t.Error("configuration should no longer be joint")
	}
	if !equalIDs(final.Config.Voters, ids(2, 3, 4)) {
		t.Errorf("final voters = %v, want {2,3,4}", final.Config.Voters)
	}

	if _, err := final.Config.LeaveJoint(); err == nil {
		t.Error("LeaveJoint outside a joint configuration should be refused")
	}
}

func TestPeersExcludesSelf(t *testing.T) {
	c := NewConfiguration(ids(1, 2, 3), 4)
	if got, want := c.Peers(2), ids(1, 3, 4); !equalIDs(got, want) {
		t.Errorf("Peers(2) = %v, want %v", got, want)
	}
}

func equalIDs(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
