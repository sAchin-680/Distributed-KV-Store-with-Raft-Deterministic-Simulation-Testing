package raft

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Configuration is the set of nodes that make up the cluster.
//
// All three slices are kept sorted and deduplicated, always. That is not
// cosmetic: the core derives ordered decisions from them — which peer to contact
// next, which match indexes form a quorum — and if that order varied between
// runs, replaying a simulation from a seed would not reproduce the same
// execution. Sorted slices are used throughout the core for exactly this reason,
// and maps only ever for point lookups.
type Configuration struct {
	// Voters is C_new: the configuration the cluster is moving to, or simply
	// the current configuration when no change is in flight.
	Voters []NodeID

	// OldVoters is C_old, non-empty only while a joint configuration is in
	// force. During that window every decision needs a majority of both sets.
	OldVoters []NodeID

	// Learners receive log entries but are never counted toward any majority.
	// Used to let a new node catch up before it can affect availability.
	Learners []NodeID
}

// ErrEmptyConfiguration is returned for a configuration with no voters. Such a
// cluster cannot commit anything, including the change that would fix it.
var ErrEmptyConfiguration = errors.New("raft: configuration has no voters")

// NewConfiguration builds a normalized configuration from the given voters.
func NewConfiguration(voters []NodeID, learners ...NodeID) Configuration {
	c := Configuration{Voters: voters, Learners: learners}
	c.normalize()
	return c
}

// normalize sorts and deduplicates every slice, and drops any learner that is
// also a voter — voter status wins, since a learner entry for a voting member
// would silently exclude it from quorum arithmetic.
func (c *Configuration) normalize() {
	c.Voters = sortedUnique(c.Voters)
	c.OldVoters = sortedUnique(c.OldVoters)
	c.Learners = sortedUnique(c.Learners)

	if len(c.Learners) > 0 {
		c.Learners = slices.DeleteFunc(c.Learners, func(id NodeID) bool {
			return slices.Contains(c.Voters, id) || slices.Contains(c.OldVoters, id)
		})
	}
	if len(c.Voters) == 0 {
		c.Voters = nil
	}
	if len(c.OldVoters) == 0 {
		c.OldVoters = nil
	}
	if len(c.Learners) == 0 {
		c.Learners = nil
	}
}

func sortedUnique(ids []NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

// Clone returns a deep copy. Configurations are stored in log entries and
// snapshots and handed to callers, so sharing backing arrays invites aliasing
// bugs that would only show up under specific interleavings.
func (c Configuration) Clone() Configuration {
	return Configuration{
		Voters:    slices.Clone(c.Voters),
		OldVoters: slices.Clone(c.OldVoters),
		Learners:  slices.Clone(c.Learners),
	}
}

// IsJoint reports whether a membership change is currently in flight.
func (c Configuration) IsJoint() bool { return len(c.OldVoters) > 0 }

// IsEmpty reports whether the configuration has no members at all.
func (c Configuration) IsEmpty() bool {
	return len(c.Voters) == 0 && len(c.OldVoters) == 0 && len(c.Learners) == 0
}

// Validate reports whether the configuration could function.
func (c Configuration) Validate() error {
	if len(c.Voters) == 0 {
		return ErrEmptyConfiguration
	}
	return nil
}

// IsVoter reports whether id is counted toward a majority in either half of the
// configuration.
func (c Configuration) IsVoter(id NodeID) bool {
	return slices.Contains(c.Voters, id) || slices.Contains(c.OldVoters, id)
}

// Contains reports whether id is a member in any role.
func (c Configuration) Contains(id NodeID) bool {
	return c.IsVoter(id) || slices.Contains(c.Learners, id)
}

// Members returns every node in the configuration, sorted, in any role.
func (c Configuration) Members() []NodeID {
	all := make([]NodeID, 0, len(c.Voters)+len(c.OldVoters)+len(c.Learners))
	all = append(all, c.Voters...)
	all = append(all, c.OldVoters...)
	all = append(all, c.Learners...)
	return sortedUnique(all)
}

// Peers returns every member except self, sorted.
func (c Configuration) Peers(self NodeID) []NodeID {
	members := c.Members()
	return slices.DeleteFunc(members, func(id NodeID) bool { return id == self })
}

// HasQuorum reports whether the nodes for which granted returns true form a
// quorum.
//
// In a joint configuration this requires a majority of C_old *and* a majority of
// C_new independently. That double requirement is the entire point of joint
// consensus: because no single set of nodes can be a majority of C_new while a
// disjoint set is a majority of C_old, the two configurations can never
// simultaneously elect different leaders during the handover.
func (c Configuration) HasQuorum(granted func(NodeID) bool) bool {
	if !hasMajority(c.Voters, granted) {
		return false
	}
	if c.IsJoint() && !hasMajority(c.OldVoters, granted) {
		return false
	}
	return true
}

func hasMajority(voters []NodeID, granted func(NodeID) bool) bool {
	if len(voters) == 0 {
		// An empty half cannot produce a majority. Returning true here would
		// let an empty configuration commit anything it liked.
		return false
	}
	n := 0
	for _, v := range voters {
		if granted(v) {
			n++
		}
	}
	return n*2 > len(voters)
}

// CommittedIndex returns the highest index replicated to a quorum, given each
// voter's match index.
//
// This is the raw quorum arithmetic only. It says nothing about whether that
// index may actually be committed — the caller must still apply the rule that a
// leader only commits an index whose entry is from the leader's own term. See
// maybeAdvanceCommit in replication.go, which is where that check lives and why.
func (c Configuration) CommittedIndex(match func(NodeID) Index) Index {
	idx := quorumIndex(c.Voters, match)
	if c.IsJoint() {
		// The transition commits only as fast as the slower half.
		if old := quorumIndex(c.OldVoters, match); old < idx {
			idx = old
		}
	}
	return idx
}

// quorumIndex returns the largest index that a majority of voters have matched.
//
// Sort the match indexes ascending and take element (n-1)/2: every element from
// there to the end is at least that large, and that is a majority of n by
// construction. For n=3 that is the middle element, for n=5 the third smallest.
func quorumIndex(voters []NodeID, match func(NodeID) Index) Index {
	n := len(voters)
	if n == 0 {
		return 0
	}
	idx := make([]Index, n)
	for i, v := range voters {
		idx[i] = match(v)
	}
	slices.Sort(idx)
	return idx[(n-1)/2]
}

// ---------------------------------------------------------------------------
// Configuration changes
// ---------------------------------------------------------------------------

// ConfChangeKind is which half of a joint-consensus transition an entry performs.
type ConfChangeKind uint8

const (
	// ConfChangeEnterJoint moves the cluster from C_old into C_old,new.
	ConfChangeEnterJoint ConfChangeKind = iota

	// ConfChangeLeaveJoint moves it from C_old,new to C_new alone.
	ConfChangeLeaveJoint
)

func (k ConfChangeKind) String() string {
	switch k {
	case ConfChangeEnterJoint:
		return "enter-joint"
	case ConfChangeLeaveJoint:
		return "leave-joint"
	default:
		return fmt.Sprintf("unknown-conf-change(%d)", uint8(k))
	}
}

// ConfChange is the payload of an EntryConfChange log entry. It carries the
// resulting configuration outright rather than a delta, so that a node applying
// it never has to reconstruct history to know what the configuration became.
type ConfChange struct {
	Kind   ConfChangeKind
	Config Configuration
}

func (cc ConfChange) String() string {
	return fmt.Sprintf("%s->%s", cc.Kind, cc.Config)
}

// EnterJoint returns the configuration change that begins a transition from the
// current configuration to target.
func (c Configuration) EnterJoint(target Configuration) (ConfChange, error) {
	if err := target.Validate(); err != nil {
		return ConfChange{}, err
	}
	if c.IsJoint() {
		return ConfChange{}, errors.New("raft: membership change already in progress")
	}
	joint := Configuration{
		Voters:    slices.Clone(target.Voters),
		OldVoters: slices.Clone(c.Voters),
		Learners:  slices.Clone(target.Learners),
	}
	joint.normalize()
	return ConfChange{Kind: ConfChangeEnterJoint, Config: joint}, nil
}

// LeaveJoint returns the configuration change that completes the transition.
func (c Configuration) LeaveJoint() (ConfChange, error) {
	if !c.IsJoint() {
		return ConfChange{}, errors.New("raft: not in a joint configuration")
	}
	final := Configuration{
		Voters:   slices.Clone(c.Voters),
		Learners: slices.Clone(c.Learners),
	}
	final.normalize()
	if err := final.Validate(); err != nil {
		return ConfChange{}, err
	}
	return ConfChange{Kind: ConfChangeLeaveJoint, Config: final}, nil
}

func (c Configuration) String() string {
	var b strings.Builder
	b.WriteByte('(')
	writeIDs(&b, c.Voters)
	if c.IsJoint() {
		b.WriteString("|old:")
		writeIDs(&b, c.OldVoters)
	}
	if len(c.Learners) > 0 {
		b.WriteString("|learners:")
		writeIDs(&b, c.Learners)
	}
	b.WriteByte(')')
	return b.String()
}

func writeIDs(b *strings.Builder, ids []NodeID) {
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(b, "%d", id)
	}
}

// Equal reports whether two configurations have identical membership.
func (c Configuration) Equal(other Configuration) bool {
	return slices.Equal(c.Voters, other.Voters) &&
		slices.Equal(c.OldVoters, other.OldVoters) &&
		slices.Equal(c.Learners, other.Learners)
}
