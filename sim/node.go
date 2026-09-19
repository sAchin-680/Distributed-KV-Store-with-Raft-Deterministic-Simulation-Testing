package sim

import (
	"fmt"

	"github.com/sAchin-680/raftkv/raft"
)

// node is one simulated cluster member.
//
// The distinction that matters here is between the RawNode, which is volatile,
// and the MemoryStorage, which is not. Crashing a node discards the RawNode and
// keeps the storage — which is exactly what a real crash does to a process and
// its disk. Restarting builds a fresh RawNode from the surviving storage, so the
// node genuinely has to recover its term, vote and log from durable state rather
// than remembering them.
type node struct {
	id      raft.NodeID
	storage *raft.MemoryStorage

	// rn is nil while the node is down.
	rn *raft.RawNode

	// restartedSinceCheck suppresses the commit-monotonicity check for one
	// observation after a restart: the persisted commit index legitimately lags
	// the in-memory one, so coming back lower is correct, not a violation.
	restartedSinceCheck bool

	crashes  int
	restarts int

	// applied is the simulated state machine: every committed command this node
	// has consumed, in order. Kept so a later layer can compare state machines
	// across nodes; for now its length is a cheap liveness signal.
	applied []raft.LogEntry
}

func (n *node) crashed() bool { return n.rn == nil }

// start builds a RawNode over whatever is in storage.
func (n *node) start(cfg raft.Config) error {
	cfg.ID = n.id
	cfg.Storage = n.storage
	rn, err := raft.NewRawNode(cfg)
	if err != nil {
		return fmt.Errorf("sim: starting node %d: %w", n.id, err)
	}
	n.rn = rn
	return nil
}

// crash stops the node, discarding all volatile state.
//
// That includes the state machine. A KV map lives in memory and dies with the
// process; on restart it is rebuilt by replaying the log from the beginning.
// Keeping it across a crash would model a state machine that is magically
// durable, and would hide any bug where replay produces a different result.
func (n *node) crash() {
	n.rn = nil
	n.applied = nil
	n.crashes++
}

// wipeDisk discards the persisted state too, modelling a replaced machine.
func (n *node) wipeDisk() {
	n.storage = raft.NewMemoryStorage()
	n.applied = nil
}

// drainApplied feeds committed entries to the simulated state machine.
//
// Applying and acknowledging are separate calls in the core so that nothing is
// marked applied before the state machine has taken it; this mirrors that.
func (n *node) drainApplied() error {
	if n.crashed() {
		return nil
	}
	for n.rn.HasCommittedEntries() {
		entries, err := n.rn.CommittedEntries()
		if err != nil {
			return fmt.Errorf("sim: node %d reading committed entries: %w", n.id, err)
		}
		if len(entries) == 0 {
			break
		}
		n.applied = append(n.applied, entries...)
		n.rn.ApplyTo(entries[len(entries)-1].Index)
	}
	return nil
}

func (n *node) String() string {
	if n.crashed() {
		return fmt.Sprintf("node %d [down, %d crashes]", n.id, n.crashes)
	}
	return fmt.Sprintf("node %d [%s term=%d commit=%d last=%d applied=%d]",
		n.id, n.rn.State(), n.rn.Term(), n.rn.CommitIndex(),
		n.rn.LastIndex(), len(n.applied))
}

// simRand draws election-timeout jitter from the simulator's single seeded
// source.
//
// One source for everything is what makes a run reproducible from one integer.
// Giving each node its own generator would work too, but only if every node's
// generator were itself seeded deterministically — and that is an extra place
// for the property to break silently. One source, drawn in a deterministic
// event order, cannot drift.
type simRand struct{ s *Simulator }

func (r simRand) Intn(n int) int { return r.s.rng.Intn(n) }
