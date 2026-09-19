package sim

import (
	"errors"
	"testing"

	"github.com/sAchin-680/raftkv/raft"
)

func run(t *testing.T, cfg Config) *Report {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	report, err := s.Run()
	if err != nil {
		t.Fatalf("Run (seed %d): %v", cfg.Seed, err)
	}
	return report
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

// The property this entire package exists to provide.
//
// Determinism cannot be verified by inspection and it degrades silently: nothing
// announces that replay has stopped working, and the failure mode is that a fuzz
// campaign quietly stops being able to reproduce its own failures. Hashing the
// full event trace and comparing two runs of one seed turns it into a test.
//
// This is also the only check that catches the hazard no static analysis can —
// an ordered decision derived from Go's randomized map iteration, which produces
// a different execution every run while every line of code still looks correct.
func TestSameSeedProducesIdenticalTrace(t *testing.T) {
	seeds := []int64{1, 7, 42, 1337, 99991}
	if testing.Short() {
		seeds = seeds[:2]
	}
	for _, seed := range seeds {
		cfg := DefaultConfig(seed)
		cfg.Duration = 10_000

		first := run(t, cfg)
		second := run(t, cfg)

		if first.TraceHash != second.TraceHash {
			t.Errorf("seed %d replayed differently: %016x then %016x\n"+
				"a seed that does not reproduce its own execution makes every "+
				"reported failure undebuggable",
				seed, first.TraceHash, second.TraceHash)
		}
		if first.Events != second.Events {
			t.Errorf("seed %d: %d events then %d", seed, first.Events, second.Events)
		}
		if first.Committed != second.Committed {
			t.Errorf("seed %d: committed %d then %d", seed, first.Committed, second.Committed)
		}
	}
}

func TestDifferentSeedsProduceDifferentExecutions(t *testing.T) {
	// A trace hash that does not vary with the seed would mean the seed is not
	// actually reaching the decisions, and the fuzzing would be exploring one
	// execution ten thousand times.
	seeds := int64(20)
	if testing.Short() {
		seeds = 5
	}
	seen := map[uint64]int64{}
	for seed := int64(1); seed <= seeds; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Duration = 5_000
		report := run(t, cfg)

		if prev, dup := seen[report.TraceHash]; dup {
			t.Errorf("seeds %d and %d produced identical executions (%016x)",
				prev, seed, report.TraceHash)
		}
		seen[report.TraceHash] = seed
	}
}

// ---------------------------------------------------------------------------
// The runs themselves
// ---------------------------------------------------------------------------

func TestPerfectNetworkCommitsAndStaysSafe(t *testing.T) {
	cfg := DefaultConfig(1)
	cfg.Faults = NoFaults()
	cfg.Duration = 10_000

	report := run(t, cfg)

	if !report.OK() {
		t.Fatalf("violation on a perfect network: %v", report.Violation)
	}
	if report.Committed == 0 {
		t.Error("nothing committed: a run that makes no progress proves nothing, " +
			"because every safety property is satisfied by a system that does nothing")
	}
	if report.LeaderTerms != 1 {
		t.Errorf("%d leader terms on a perfect network, want 1 — an undisturbed "+
			"cluster should not be changing leaders", report.LeaderTerms)
	}
}

func TestFaultyNetworkStillCommitsAndStaysSafe(t *testing.T) {
	seeds := int64(25)
	if testing.Short() {
		seeds = 5
	}
	for seed := int64(1); seed <= seeds; seed++ {
		report := run(t, DefaultConfig(seed))

		if !report.OK() {
			t.Fatalf("%v\nreplay with: simctl run --seed=%d --verbose", report.Violation, seed)
		}
		if report.Committed == 0 {
			t.Errorf("seed %d committed nothing despite %d messages", seed, report.MessagesSent)
		}
	}
}

// A run that never loses a message, never partitions and never crashes anything
// is not testing fault handling. This asserts the faults actually fire.
func TestDefaultFaultsActuallyOccur(t *testing.T) {
	report := run(t, DefaultConfig(3))

	checks := []struct {
		name string
		got  int
	}{
		{"dropped messages", report.Dropped},
		{"duplicated messages", report.Duplicated},
		{"partitions", report.Partitions},
		{"crashes", report.Crashes},
		{"restarts", report.Restarts},
	}
	for _, c := range checks {
		if c.got == 0 {
			t.Errorf("no %s occurred; the fault injection is not doing anything", c.name)
		}
	}
	if report.LeaderTerms < 2 {
		t.Errorf("only %d leader term(s) under crashes and partitions; the run is "+
			"not reaching the interleavings it exists to explore", report.LeaderTerms)
	}
}

// Crashing below quorum stalls the cluster, and a stalled cluster satisfies every
// safety property trivially. The simulator must keep a quorum alive so that runs
// stay meaningful.
func TestQuorumIsNeverDeliberatelyLost(t *testing.T) {
	cfg := DefaultConfig(11)
	cfg.Faults.CrashRate = 0.9
	cfg.Faults.RestartMin = 5_000
	cfg.Faults.RestartMax = 10_000

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if live := s.liveNodes(); live < s.quorum() {
		t.Errorf("%d of %d nodes live, below the quorum of %d",
			live, len(s.ids), s.quorum())
	}
}

func TestVirtualTimeCostsAlmostNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("measures a full simulated minute")
	}
	cfg := DefaultConfig(5)
	cfg.Duration = 60_000 // one simulated minute

	report := run(t, cfg)

	if report.VirtualTime < 59_000 {
		t.Fatalf("only simulated %dms of the requested 60000ms", report.VirtualTime)
	}
	// The whole economics of this approach: nothing sleeps, so simulated time is
	// effectively free and ten thousand runs fit in a CI job.
	if report.Elapsed.Seconds() > 5 {
		t.Errorf("one simulated minute took %s of real time; that budget does not "+
			"scale to a fuzz campaign", report.Elapsed)
	}
	t.Logf("60s of cluster time in %s (%.0fx faster than real time), %d events",
		report.Elapsed.Round(1000), 60.0/report.Elapsed.Seconds(), report.Events)
}

// ---------------------------------------------------------------------------
// The checker must be able to fail
// ---------------------------------------------------------------------------

// A safety checker that cannot detect a violation is worse than none: it reports
// success forever and nobody looks again. These tests inject each violation
// directly and require the checker to notice.

func TestCheckerDetectsTwoLeadersInOneTerm(t *testing.T) {
	// A single-node cluster is its own quorum, so node 1 reaches leadership
	// without any of the machinery this test is not about.
	cfg := DefaultConfig(1)
	cfg.Nodes = 1
	cfg.Faults = NoFaults()
	cfg.Duration = 1_000

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	n := s.nodes[1]
	if n.rn.State() != raft.Leader {
		t.Fatalf("setup: node 1 is %s, want leader", n.rn.State())
	}

	// Record a different node as having already held this term.
	s.checker.leaderPerTerm[n.rn.Term()] = 99

	err = s.checker.checkElectionSafety(s, n)
	var v *Violation
	if !errors.As(err, &v) || v.Property != PropElectionSafety {
		t.Fatalf("checker returned %v, want an election-safety violation", err)
	}
	t.Logf("correctly reported: %v", v)
}

func TestCheckerDetectsAChangedCommittedEntry(t *testing.T) {
	cfg := DefaultConfig(1)
	cfg.Faults = NoFaults()
	cfg.Duration = 3_000

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s.checker.CommittedCount() == 0 {
		t.Fatal("setup: nothing committed, nothing to corrupt")
	}

	// Rewrite a committed entry on one node with a different term, exactly as a
	// leader that violated the commit rule would have done.
	victim := s.nodes[s.ids[len(s.ids)-1]]
	idx := raft.Index(1)
	original, err := victim.storage.GetEntry(idx)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	corrupted := original
	corrupted.Term = original.Term + 99
	corrupted.Command = []byte("not what was committed")
	if err := victim.storage.AppendEntries([]raft.LogEntry{corrupted}); err != nil {
		t.Fatalf("corrupting storage: %v", err)
	}

	// Re-examine from the start; the checker has already verified this prefix.
	s.checker.checkedTo[victim.id] = 0

	err = s.checker.checkCommittedEntries(s, victim)
	var v *Violation
	if !errors.As(err, &v) || v.Property != PropCommittedStable {
		t.Fatalf("checker returned %v, want a committed-entries violation", err)
	}
	t.Logf("correctly reported: %v", v)
}

func TestCheckerDetectsLogMatchingViolation(t *testing.T) {
	cfg := DefaultConfig(2)
	cfg.Faults = NoFaults()
	cfg.Duration = 3_000

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := s.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Give two nodes the same term at their last index but a different entry
	// beneath it — precisely what the log matching property forbids.
	a, b := s.nodes[s.ids[0]], s.nodes[s.ids[1]]
	last := min(a.storage.LastIndex(), b.storage.LastIndex())
	if last < 3 {
		t.Skip("log too short to construct the violation")
	}

	entry, err := b.storage.GetEntry(2)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	entry.Command = []byte("divergent")
	if err := b.storage.AppendEntries([]raft.LogEntry{entry}); err != nil {
		t.Fatalf("corrupting storage: %v", err)
	}
	// Restore the agreeing suffix so the two still match at a higher index.
	rest, err := a.storage.Entries(3, last+1)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if err := b.storage.AppendEntries(rest); err != nil {
		t.Fatalf("restoring suffix: %v", err)
	}

	err = s.checker.checkLogMatching(s)
	var v *Violation
	if !errors.As(err, &v) || v.Property != PropLogMatching {
		t.Fatalf("checker returned %v, want a log-matching violation", err)
	}
	t.Logf("correctly reported: %v", v)
}

// ---------------------------------------------------------------------------
// Event ordering
// ---------------------------------------------------------------------------

// Events scheduled for the same instant must have a total order. Go's heap makes
// no promise about equal elements, and simultaneous events are routine — every
// node's first tick, for one. Without the sequence tiebreaker, replay breaks.
func TestEventQueueOrdersSimultaneousEventsByInsertion(t *testing.T) {
	q := newEventQueue()
	for i := range 10 {
		q.schedule(&event{at: 100, kind: evTick, node: raft.NodeID(i + 1)})
	}
	q.schedule(&event{at: 50, kind: evHeal})

	if first := q.pop(); first.kind != evHeal {
		t.Fatalf("earliest event = %v, want the one at t=50", first.kind)
	}
	for i := range 10 {
		got := q.pop()
		if got.node != raft.NodeID(i+1) {
			t.Fatalf("position %d holds node %d, want %d — simultaneous events "+
				"must come out in insertion order", i, got.node, i+1)
		}
	}
	if q.pop() != nil {
		t.Error("queue should be empty")
	}
}
