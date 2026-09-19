package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/sAchin-680/raftkv/raft"
)

// Violation is a breach of one of Raft's safety properties.
//
// It carries the seed, because the seed is the whole point: a violation is not
// a mystery to be reasoned about from a log excerpt, it is a run that can be
// replayed exactly, as many times as needed, on any machine.
type Violation struct {
	Property string
	Detail   string
	Seed     int64
	At       int64 // virtual milliseconds into the run
}

func (v *Violation) Error() string {
	return fmt.Sprintf("safety violation [%s] at t=%dms (seed %d): %s",
		v.Property, v.At, v.Seed, v.Detail)
}

// Properties checked. Named as constants so a failure report says which
// invariant broke rather than describing it in prose each time.
const (
	PropElectionSafety     = "election-safety"
	PropCommittedStable    = "committed-entries-never-change"
	PropLogMatching        = "log-matching"
	PropCommitMonotonic    = "commit-index-never-decreases"
	PropAppliedFollowing   = "applied-never-exceeds-committed"
	PropStateMachineSafety = "state-machines-agree"
)

// committedEntry is what some node reported committed at an index.
type committedEntry struct {
	term    raft.Term
	digest  [32]byte
	by      raft.NodeID
	at      int64
	command []byte
}

// Checker asserts Raft's safety properties against the nodes' internal state.
//
// Checking internal state rather than client-visible behavior is deliberate. A
// linearizability checker can only see what clients observed, so it detects a
// violation after it has already leaked to a client — and only if a client
// happened to look. Reading commit indexes and logs directly catches the
// violation at the instant it occurs, in runs where no client would ever have
// noticed. Client-visible checking is a separate, later layer, not a substitute.
type Checker struct {
	seed int64

	// leaderPerTerm records which node claimed leadership in each term.
	leaderPerTerm map[raft.Term]raft.NodeID

	// committed records the entry every node has ever reported committed at
	// each index. Once recorded, it must never change.
	committed map[raft.Index]committedEntry

	// checkedTo is how far each node's committed prefix has already been
	// verified, so each entry is examined once rather than on every event.
	checkedTo map[raft.NodeID]raft.Index

	// lastCommit is each node's previous commit index, for monotonicity.
	lastCommit map[raft.NodeID]raft.Index

	violations []*Violation
}

// NewChecker returns a checker for one run.
func NewChecker(seed int64) *Checker {
	return &Checker{
		seed:          seed,
		leaderPerTerm: map[raft.Term]raft.NodeID{},
		committed:     map[raft.Index]committedEntry{},
		checkedTo:     map[raft.NodeID]raft.Index{},
		lastCommit:    map[raft.NodeID]raft.Index{},
	}
}

// Observe checks the cheap, incremental properties. Called after every event.
//
// Nodes are visited in sorted order. Ranging a map here would make the order of
// recorded violations vary between runs of the same seed, which would undermine
// the one guarantee this package exists to provide.
func (c *Checker) Observe(s *Simulator) error {
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.crashed() {
			continue
		}
		if err := c.checkElectionSafety(s, n); err != nil {
			return err
		}
		if err := c.checkCommitMonotonic(s, n); err != nil {
			return err
		}
		if err := c.checkCommittedEntries(s, n); err != nil {
			return err
		}
	}
	return nil
}

// checkElectionSafety: at most one leader per term.
func (c *Checker) checkElectionSafety(s *Simulator, n *node) error {
	if n.rn.State() != raft.Leader {
		return nil
	}
	term := n.rn.Term()
	if prev, ok := c.leaderPerTerm[term]; ok && prev != n.id {
		return c.fail(s, PropElectionSafety,
			fmt.Sprintf("nodes %d and %d were both leader in term %d", prev, n.id, term))
	}
	c.leaderPerTerm[term] = n.id
	return nil
}

// checkCommitMonotonic: a commit index is a promise and never retracts.
//
// A restart that comes back with a lower commit index is legitimate — the
// persisted commit index is an optimization that may lag — so this only fires
// while a node is continuously up.
func (c *Checker) checkCommitMonotonic(s *Simulator, n *node) error {
	commit := n.rn.CommitIndex()
	if prev, ok := c.lastCommit[n.id]; ok && commit < prev && !n.restartedSinceCheck {
		return c.fail(s, PropCommitMonotonic,
			fmt.Sprintf("node %d commit index fell from %d to %d", n.id, prev, commit))
	}
	c.lastCommit[n.id] = commit
	n.restartedSinceCheck = false

	if applied := n.rn.AppliedIndex(); applied > commit {
		return c.fail(s, PropAppliedFollowing,
			fmt.Sprintf("node %d applied %d beyond committed %d", n.id, applied, commit))
	}
	return nil
}

// checkCommittedEntries: no committed entry is ever overwritten.
//
// This is State Machine Safety, and it is the property clients actually care
// about — a violation here means a write that was acknowledged has been erased
// or silently replaced.
func (c *Checker) checkCommittedEntries(s *Simulator, n *node) error {
	commit := n.rn.CommitIndex()
	from := c.checkedTo[n.id] + 1

	for i := from; i <= commit; i++ {
		entry, err := n.storage.GetEntry(i)
		if err != nil {
			if errors.Is(err, raft.ErrCompacted) {
				continue // folded into a snapshot; nothing to compare
			}
			return c.fail(s, PropCommittedStable,
				fmt.Sprintf("node %d cannot read its own committed index %d: %v", n.id, i, err))
		}

		digest := sha256.Sum256(entry.Command)
		if prev, seen := c.committed[i]; seen {
			if prev.term != entry.Term || prev.digest != digest {
				return c.fail(s, PropCommittedStable, fmt.Sprintf(
					"committed index %d changed: node %d committed term %d (%s) at t=%dms, "+
						"node %d now has term %d (%s)",
					i, prev.by, prev.term, shortDigest(prev.digest), prev.at,
					n.id, entry.Term, shortDigest(digest)))
			}
		} else {
			c.committed[i] = committedEntry{
				term:    entry.Term,
				digest:  digest,
				by:      n.id,
				at:      s.now,
				command: entry.Command,
			}
		}
	}

	if commit > c.checkedTo[n.id] {
		c.checkedTo[n.id] = commit
	}
	return nil
}

// Final runs the checks that are too expensive to repeat after every event.
func (c *Checker) Final(s *Simulator) error {
	if err := c.checkLogMatching(s); err != nil {
		return err
	}
	return c.checkStateMachinesAgree(s)
}

// checkStateMachinesAgree: no two nodes have applied a different command at the
// same log index.
//
// This is State Machine Safety stated at the level users care about. The
// committed-entry check above looks at logs; this one looks at what was actually
// fed to the state machine, which is a different claim. A bug in the apply path
// — draining twice, skipping an entry, applying before commit — would leave the
// logs identical and the state machines divergent.
//
// Compared by log index rather than by position in each node's list.
//
// Position would be wrong, and was: once snapshotting exists, a node that
// restarted rebuilds its state machine from a snapshot and its history
// legitimately begins partway through the log. Two correct nodes then hold the
// same command at different positions. Indexing by log index states the property
// that actually matters — "no two nodes disagree about what is at index i" —
// and is immune to where each node's history happens to start.
func (c *Checker) checkStateMachinesAgree(s *Simulator) error {
	for ai, a := range s.ids {
		byIndex := make(map[raft.Index]raft.LogEntry, len(s.nodes[a].applied))
		for _, e := range s.nodes[a].applied {
			byIndex[e.Index] = e
		}

		for _, b := range s.ids[ai+1:] {
			for _, eb := range s.nodes[b].applied {
				ea, both := byIndex[eb.Index]
				if !both {
					continue
				}
				if ea.Term != eb.Term ||
					sha256.Sum256(ea.Command) != sha256.Sum256(eb.Command) {
					return c.fail(s, PropStateMachineSafety, fmt.Sprintf(
						"nodes %d and %d applied different commands at index %d: "+
							"term %d (%s) vs term %d (%s)",
						a, b, eb.Index,
						ea.Term, shortDigest(sha256.Sum256(ea.Command)),
						eb.Term, shortDigest(sha256.Sum256(eb.Command))))
				}
			}
		}
	}
	return nil
}

// checkLogMatching: if two logs hold the same term at an index, they agree on
// every entry before it.
//
// Quadratic in cluster size and linear in log length, so it runs once at the end
// rather than continuously. A violation is permanent once it happens, so the end
// of the run is a sufficient place to look.
func (c *Checker) checkLogMatching(s *Simulator) error {
	for ai, a := range s.ids {
		for _, b := range s.ids[ai+1:] {
			na, nb := s.nodes[a], s.nodes[b]
			if na.crashed() || nb.crashed() {
				continue
			}

			limit := min(na.storage.LastIndex(), nb.storage.LastIndex())
			// Walk down to the highest index where the two agree; from there
			// down, every entry must be identical.
			for i := limit; i >= 1; i-- {
				ta, errA := na.storage.Term(i)
				tb, errB := nb.storage.Term(i)
				if errA != nil || errB != nil {
					break // compacted out from under us
				}
				if ta != tb {
					continue
				}
				if err := c.compareBelow(s, na, nb, i); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func (c *Checker) compareBelow(s *Simulator, a, b *node, upto raft.Index) error {
	lo := max(a.storage.FirstIndex(), b.storage.FirstIndex())
	for i := lo; i <= upto; i++ {
		ea, errA := a.storage.GetEntry(i)
		eb, errB := b.storage.GetEntry(i)
		if errA != nil || errB != nil {
			continue
		}
		if ea.Term != eb.Term || sha256.Sum256(ea.Command) != sha256.Sum256(eb.Command) {
			return c.fail(s, PropLogMatching, fmt.Sprintf(
				"nodes %d and %d agree at index %d but differ at index %d "+
					"(terms %d and %d)", a.id, b.id, upto, i, ea.Term, eb.Term))
		}
	}
	return nil
}

func (c *Checker) fail(s *Simulator, property, detail string) error {
	v := &Violation{Property: property, Detail: detail, Seed: c.seed, At: s.now}
	c.violations = append(c.violations, v)
	return v
}

// Violations returns everything found, in the order it was found.
func (c *Checker) Violations() []*Violation { return c.violations }

// CommittedCount is how many distinct indexes were observed committed. A run
// that finds no violations but also commits nothing has proved very little, so
// this is reported alongside the result.
func (c *Checker) CommittedCount() int { return len(c.committed) }

// LeaderTerms is how many distinct terms produced a leader.
func (c *Checker) LeaderTerms() int { return len(c.leaderPerTerm) }

func shortDigest(d [32]byte) string { return hex.EncodeToString(d[:4]) }
