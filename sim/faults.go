package sim

import (
	"fmt"
	"slices"
	"strings"

	"github.com/sAchin-680/raftkv/raft"
)

// Faults configures how hostile the simulated world is.
//
// Every rate is a probability in [0, 1] evaluated against the one seeded random
// source, so the same seed produces the same faults in the same order. Turning
// everything to zero yields a perfect network, which is useful for isolating
// whether a failure is caused by a fault or by the algorithm.
type Faults struct {
	// DropRate is the chance a message is discarded outright.
	DropRate float64

	// DuplicateRate is the chance a message is delivered twice, at different
	// times. Duplicates are not exotic: any retrying transport produces them,
	// and they are what catches an AppendEntries handler that truncates on
	// entries it already holds.
	DuplicateRate float64

	// MinLatency and MaxLatency bound a message's delivery delay, in virtual
	// milliseconds.
	//
	// Variance here is what produces reordering: two messages sent in order
	// arrive in either order whenever the spread exceeds the gap between them.
	// That is a more faithful model than explicitly swapping two messages,
	// because it is how real reordering actually arises.
	MinLatency int64
	MaxLatency int64

	// ReorderRate is the chance a message is given an outsized delay on top of
	// its normal latency, pushing it behind messages sent long after it. This
	// reaches interleavings that ordinary latency variance would need an
	// implausibly wide spread to produce.
	ReorderRate float64
	ReorderMax  int64

	// PartitionRate is the chance, at each fault decision point, of splitting
	// the cluster into two groups that cannot talk to each other.
	PartitionRate float64

	// PartitionMin and PartitionMax bound how long a partition lasts.
	PartitionMin int64
	PartitionMax int64

	// CrashRate is the chance, at each fault decision point, of stopping a node.
	CrashRate float64

	// RestartMin and RestartMax bound how long a crashed node stays down.
	RestartMin int64
	RestartMax int64

	// DiskLossRate is the chance a restarting node comes back with no persisted
	// state at all, modelling a replaced machine rather than a reboot.
	//
	// Off by default, and deliberately so: Raft's safety argument assumes the
	// hard state survives a crash. A node that forgets its vote can vote twice
	// in one term and elect two leaders, so enabling this will produce genuine
	// safety violations that are not implementation bugs. It is here to explore
	// that failure mode knowingly, not to run by default.
	DiskLossRate float64

	// FaultInterval is how often, in virtual milliseconds, the simulator
	// considers introducing a new fault.
	FaultInterval int64
}

// DefaultFaults is a world that is unpleasant but not absurd: noticeable loss,
// wide latency spread, occasional duplicates, and regular partitions and
// crashes. Tuned so that a 30-second run reliably exercises leader changes.
func DefaultFaults() Faults {
	return Faults{
		DropRate:      0.02,
		DuplicateRate: 0.02,
		MinLatency:    1,
		MaxLatency:    30,
		ReorderRate:   0.02,
		ReorderMax:    200,
		PartitionRate: 0.15,
		PartitionMin:  200,
		PartitionMax:  2000,
		CrashRate:     0.15,
		RestartMin:    100,
		RestartMax:    3000,
		DiskLossRate:  0,
		FaultInterval: 250,
	}
}

// NoFaults is a perfect network: no loss, no reordering, fixed latency, no
// crashes. A run that fails here has an algorithm problem, not a fault-handling
// problem, which makes it the first thing to try when triaging a failing seed.
func NoFaults() Faults {
	return Faults{
		MinLatency:    5,
		MaxLatency:    5,
		FaultInterval: 1_000_000,
	}
}

// partition splits the cluster into two groups that cannot exchange messages.
//
// Represented as one side's membership: a node is on side A or side B, and
// messages only flow within a side. This models the failure that matters most —
// a network split where each side stays healthy internally and may believe it
// is the whole cluster.
type partition struct {
	sideB map[raft.NodeID]bool
	until int64
}

func (p *partition) blocks(from, to raft.NodeID) bool {
	if p == nil {
		return false
	}
	return p.sideB[from] != p.sideB[to]
}

func (p *partition) String() string {
	if p == nil {
		return "none"
	}
	var b []raft.NodeID
	for id, on := range p.sideB {
		if on {
			b = append(b, id)
		}
	}
	slices.Sort(b)
	parts := make([]string, len(b))
	for i, id := range b {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return "{" + strings.Join(parts, ",") + "}|rest"
}
