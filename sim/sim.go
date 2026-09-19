// Package sim runs a whole Raft cluster inside one deterministic,
// single-threaded event loop.
//
// Every source of nondeterminism a real cluster has — when timers fire, how long
// messages take, which ones are lost, when nodes crash — is replaced by a
// decision drawn from one seeded random source, in an order fixed by the event
// queue. A run is therefore a pure function of its seed: the same integer
// produces the same execution, on any machine, forever.
//
// That is the whole point. Consensus bugs live in rare interleavings, and an
// integration test that stumbles onto one usually cannot produce it again. Here,
// a failure hands back a number, and the number reproduces the failure exactly.
//
// Time is virtual. Nothing sleeps, so a simulated minute of cluster life costs
// milliseconds of real time and ten thousand randomized runs fit in a CI job.
package sim

import (
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/sAchin-680/raftkv/raft"
)

// Config describes one simulation run.
type Config struct {
	// Seed determines the entire execution.
	Seed int64

	// Nodes is the cluster size. Odd numbers only, in practice: four nodes
	// tolerate the same single failure as three while needing one more vote.
	Nodes int

	// Duration is how much virtual time to simulate, in milliseconds.
	Duration int64

	// TickInterval is the virtual time between a node's logical ticks. With the
	// default election and heartbeat ticks this makes an election timeout
	// roughly 100–200ms, which is the order of magnitude real deployments use.
	TickInterval int64

	ElectionTick  int
	HeartbeatTick int
	PreVote       bool

	// WriteInterval is how often a client attempts a write, in virtual
	// milliseconds. Zero disables client traffic, which makes for a quiet run
	// that proves very little.
	WriteInterval int64

	Faults Faults

	// Verbose keeps the full event trace in memory for printing.
	Verbose bool
}

// DefaultConfig is a five-node cluster under the default fault load for thirty
// virtual seconds.
func DefaultConfig(seed int64) Config {
	return Config{
		Seed:          seed,
		Nodes:         5,
		Duration:      30_000,
		TickInterval:  10,
		ElectionTick:  10,
		HeartbeatTick: 2,
		PreVote:       true,
		WriteInterval: 20,
		Faults:        DefaultFaults(),
	}
}

func (c *Config) withDefaults() {
	if c.Nodes == 0 {
		c.Nodes = 5
	}
	if c.Duration == 0 {
		c.Duration = 30_000
	}
	if c.TickInterval == 0 {
		c.TickInterval = 10
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 2
	}
	if c.Faults.FaultInterval == 0 {
		c.Faults.FaultInterval = 250
	}
	if c.Faults.MaxLatency == 0 {
		c.Faults.MaxLatency = c.Faults.MinLatency
	}
}

// Report is the outcome of a run.
type Report struct {
	Seed      int64
	Violation *Violation

	Events       int
	TraceHash    uint64
	VirtualTime  int64
	Elapsed      time.Duration
	Committed    int
	LeaderTerms  int
	Crashes      int
	Restarts     int
	Partitions   int
	MessagesSent int
	Dropped      int
	Duplicated   int

	Trace *Trace
}

// OK reports whether the run found no safety violation.
func (r *Report) OK() bool { return r.Violation == nil }

func (r *Report) String() string {
	status := "ok"
	if r.Violation != nil {
		status = "VIOLATION"
	}
	return fmt.Sprintf(
		"seed %d: %s — %d events in %s (%dms virtual), %d entries committed across "+
			"%d leader terms, %d crashes, %d restarts, %d partitions, "+
			"%d msgs (%d dropped, %d duplicated), trace %016x",
		r.Seed, status, r.Events, r.Elapsed.Round(time.Microsecond), r.VirtualTime,
		r.Committed, r.LeaderTerms, r.Crashes, r.Restarts, r.Partitions,
		r.MessagesSent, r.Dropped, r.Duplicated, r.TraceHash)
}

// Simulator owns every node's clock and network.
type Simulator struct {
	cfg Config
	rng *rand.Rand

	now   int64
	queue *eventQueue

	ids   []raft.NodeID
	nodes map[raft.NodeID]*node
	conf  raft.Configuration

	part *partition

	checker *Checker
	trace   *Trace

	writeSeq int
	stats    struct {
		sent, dropped, duplicated, partitions int
	}
}

// New builds a simulator. It does not run anything yet.
func New(cfg Config) (*Simulator, error) {
	cfg.withDefaults()
	if cfg.Nodes < 1 {
		return nil, fmt.Errorf("sim: need at least one node, got %d", cfg.Nodes)
	}

	s := &Simulator{
		cfg: cfg,
		// One source, seeded once. Every decision in the run is drawn from
		// here, which is what makes the seed sufficient to reproduce a failure.
		rng:     rand.New(rand.NewSource(cfg.Seed)), //nolint:gosec // reproducibility, not secrecy
		queue:   newEventQueue(),
		nodes:   make(map[raft.NodeID]*node, cfg.Nodes),
		checker: NewChecker(cfg.Seed),
		trace:   newTrace(cfg.Verbose),
	}

	for i := 1; i <= cfg.Nodes; i++ {
		id := raft.NodeID(i)
		s.ids = append(s.ids, id)
		s.nodes[id] = &node{id: id, storage: raft.NewMemoryStorage()}
	}
	slices.Sort(s.ids)
	s.conf = raft.NewConfiguration(s.ids)

	for _, id := range s.ids {
		if err := s.nodes[id].start(s.raftConfig()); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Simulator) raftConfig() raft.Config {
	return raft.Config{
		ElectionTick:  s.cfg.ElectionTick,
		HeartbeatTick: s.cfg.HeartbeatTick,
		PreVote:       s.cfg.PreVote,
		Rand:          simRand{s},
		Bootstrap:     s.conf,
	}
}

// Run executes the simulation to completion and checks safety throughout.
func (s *Simulator) Run() (*Report, error) {
	started := time.Now()

	// Stagger the first tick of each node. Starting them all at t=0 is a
	// legitimate state, but it makes every run begin with a simultaneous
	// election, which is not representative of a cluster that has been up.
	for _, id := range s.ids {
		s.queue.schedule(&event{
			at:   int64(s.rng.Intn(int(s.cfg.TickInterval))) + 1,
			kind: evTick,
			node: id,
		})
	}
	s.queue.schedule(&event{at: s.cfg.Faults.FaultInterval, kind: evPartition})
	if s.cfg.WriteInterval > 0 {
		s.queue.schedule(&event{at: s.cfg.WriteInterval, kind: evClientWrite})
	}

	var violation *Violation
	for {
		ev := s.queue.pop()
		if ev == nil || ev.at > s.cfg.Duration {
			break
		}
		s.now = ev.at

		if err := s.apply(ev); err != nil {
			return s.report(started, nil), err
		}
		if err := s.checker.Observe(s); err != nil {
			violation = asViolation(err)
			if violation == nil {
				return s.report(started, nil), err
			}
			break
		}
	}

	if violation == nil {
		if err := s.checker.Final(s); err != nil {
			violation = asViolation(err)
			if violation == nil {
				return s.report(started, nil), err
			}
		}
	}

	return s.report(started, violation), nil
}

func asViolation(err error) *Violation {
	var v *Violation
	if ok := asErr(err, &v); ok {
		return v
	}
	return nil
}

func (s *Simulator) report(started time.Time, v *Violation) *Report {
	crashes, restarts := 0, 0
	for _, id := range s.ids {
		crashes += s.nodes[id].crashes
		restarts += s.nodes[id].restarts
	}
	return &Report{
		Seed:         s.cfg.Seed,
		Violation:    v,
		Events:       s.trace.Events(),
		TraceHash:    s.trace.Hash(),
		VirtualTime:  s.now,
		Elapsed:      time.Since(started),
		Committed:    s.checker.CommittedCount(),
		LeaderTerms:  s.checker.LeaderTerms(),
		Crashes:      crashes,
		Restarts:     restarts,
		Partitions:   s.stats.partitions,
		MessagesSent: s.stats.sent,
		Dropped:      s.stats.dropped,
		Duplicated:   s.stats.duplicated,
		Trace:        s.trace,
	}
}

// ---------------------------------------------------------------------------
// Event application
// ---------------------------------------------------------------------------

func (s *Simulator) apply(ev *event) error {
	switch ev.kind {
	case evTick:
		return s.applyTick(ev)
	case evDeliver:
		return s.applyDeliver(ev)
	case evCrash:
		return s.applyCrash(ev)
	case evRestart:
		return s.applyRestart(ev)
	case evPartition:
		return s.applyFaultDecision()
	case evHeal:
		return s.applyHeal()
	case evClientWrite:
		return s.applyClientWrite()
	default:
		return fmt.Errorf("sim: unknown event kind %v", ev.kind)
	}
}

func (s *Simulator) applyTick(ev *event) error {
	n := s.nodes[ev.node]

	// Reschedule first, so a crashed node resumes ticking on restart without
	// the restart path having to remember to start the timer again.
	s.queue.schedule(&event{at: s.now + s.cfg.TickInterval, kind: evTick, node: ev.node})

	if n.crashed() {
		return nil
	}
	if err := n.rn.Tick(); err != nil {
		return fmt.Errorf("sim: tick on node %d: %w", n.id, err)
	}
	s.trace.record(s.now, "tick %d %s term=%d", n.id, n.rn.State(), n.rn.Term())
	return s.after(n)
}

func (s *Simulator) applyDeliver(ev *event) error {
	n := s.nodes[ev.msg.To]
	if n.crashed() {
		s.trace.record(s.now, "lost %s (recipient down)", ev.msg.Type)
		return nil
	}

	if err := n.rn.Step(ev.msg); err != nil {
		// A stale, duplicated or inapplicable message is normal traffic, not a
		// failure; the network is allowed to produce all three.
		if !isIgnored(err) {
			return fmt.Errorf("sim: step %s on node %d: %w", ev.msg.Type, n.id, err)
		}
		s.trace.record(s.now, "ignore %d->%d %s", ev.msg.From, n.id, ev.msg.Type)
		return nil
	}
	s.trace.record(s.now, "recv %d<-%d %s term=%d", n.id, ev.msg.From, ev.msg.Type, ev.msg.Term)
	return s.after(n)
}

// after drains whatever a node produced: outbound messages and newly committed
// entries.
func (s *Simulator) after(n *node) error {
	if n.crashed() {
		return nil
	}
	for _, m := range n.rn.Messages() {
		s.route(m)
	}
	return n.drainApplied()
}

func (s *Simulator) applyCrash(ev *event) error {
	n := s.nodes[ev.node]
	if n.crashed() {
		return nil
	}

	// Never take the cluster below quorum on purpose. A cluster that cannot
	// elect a leader is not making progress, and a run spent stalled tests
	// nothing — every safety property is trivially satisfied by a system that
	// does nothing at all.
	if s.liveNodes()-1 < s.quorum() {
		return nil
	}

	n.crash()
	s.trace.record(s.now, "crash %d", n.id)

	down := s.cfg.Faults.RestartMin +
		s.randRange(s.cfg.Faults.RestartMax-s.cfg.Faults.RestartMin)
	lose := s.rng.Float64() < s.cfg.Faults.DiskLossRate
	s.queue.schedule(&event{at: s.now + down, kind: evRestart, node: n.id, loseDisk: lose})
	return nil
}

func (s *Simulator) applyRestart(ev *event) error {
	n := s.nodes[ev.node]
	if !n.crashed() {
		return nil
	}
	if ev.loseDisk {
		n.wipeDisk()
		s.trace.record(s.now, "restart %d (disk lost)", n.id)
	} else {
		s.trace.record(s.now, "restart %d", n.id)
	}

	if err := n.start(s.raftConfig()); err != nil {
		return err
	}
	n.restarts++
	n.restartedSinceCheck = true
	return s.after(n)
}

func (s *Simulator) applyHeal() error {
	if s.part == nil {
		return nil
	}
	s.trace.record(s.now, "heal %s", s.part)
	s.part = nil
	return nil
}

// applyFaultDecision is the one place new faults are introduced. Having a single
// decision point, fired on a fixed schedule, keeps the order of random draws
// stable — which is what keeps the seed meaningful.
func (s *Simulator) applyFaultDecision() error {
	s.queue.schedule(&event{at: s.now + s.cfg.Faults.FaultInterval, kind: evPartition})

	if s.part == nil && s.rng.Float64() < s.cfg.Faults.PartitionRate {
		s.startPartition()
	}
	if s.rng.Float64() < s.cfg.Faults.CrashRate {
		victim := s.ids[s.rng.Intn(len(s.ids))]
		return s.applyCrash(&event{node: victim})
	}
	return nil
}

func (s *Simulator) startPartition() {
	// Assign each node to a side independently, then reject splits that leave
	// one side empty — those are not partitions, and silently retrying keeps the
	// number of random draws predictable.
	sideB := map[raft.NodeID]bool{}
	count := 0
	for _, id := range s.ids {
		if s.rng.Intn(2) == 0 {
			sideB[id] = true
			count++
		}
	}
	if count == 0 || count == len(s.ids) {
		return
	}

	dur := s.cfg.Faults.PartitionMin +
		s.randRange(s.cfg.Faults.PartitionMax-s.cfg.Faults.PartitionMin)
	s.part = &partition{sideB: sideB, until: s.now + dur}
	s.stats.partitions++
	s.trace.record(s.now, "partition %s for %dms", s.part, dur)
	s.queue.schedule(&event{at: s.now + dur, kind: evHeal})
}

func (s *Simulator) applyClientWrite() error {
	s.queue.schedule(&event{at: s.now + s.cfg.WriteInterval, kind: evClientWrite})

	// Writes go to whichever node believes it is leader. Several may believe it
	// at once during a partition — that is precisely the situation worth
	// generating, so the write is offered to all of them.
	wrote := false
	for _, id := range s.ids {
		n := s.nodes[id]
		if n.crashed() || n.rn.State() != raft.Leader {
			continue
		}
		s.writeSeq++
		cmd := fmt.Appendf(nil, "w%d", s.writeSeq)
		if _, err := n.rn.Propose(cmd); err != nil {
			continue // lost leadership between the check and the call
		}
		s.trace.record(s.now, "write %d to %d", s.writeSeq, id)
		if err := s.after(n); err != nil {
			return err
		}
		wrote = true
	}
	if !wrote {
		s.trace.record(s.now, "write skipped (no leader)")
	}
	return nil
}

// ---------------------------------------------------------------------------
// The network
// ---------------------------------------------------------------------------

// route decides what the network does to one outbound message.
func (s *Simulator) route(m raft.Message) {
	s.stats.sent++

	if s.part.blocks(m.From, m.To) {
		s.stats.dropped++
		s.trace.record(s.now, "block %d->%d %s", m.From, m.To, m.Type)
		return
	}
	if s.rng.Float64() < s.cfg.Faults.DropRate {
		s.stats.dropped++
		s.trace.record(s.now, "drop %d->%d %s", m.From, m.To, m.Type)
		return
	}

	latency := s.latency()
	s.queue.schedule(&event{at: s.now + latency, kind: evDeliver, msg: m})
	s.trace.record(s.now, "send %d->%d %s +%dms", m.From, m.To, m.Type, latency)

	if s.rng.Float64() < s.cfg.Faults.DuplicateRate {
		extra := s.latency()
		s.queue.schedule(&event{at: s.now + latency + extra, kind: evDeliver, msg: m})
		s.stats.duplicated++
		s.trace.record(s.now, "dup %d->%d %s +%dms", m.From, m.To, m.Type, latency+extra)
	}
}

func (s *Simulator) latency() int64 {
	f := s.cfg.Faults
	d := f.MinLatency + s.randRange(f.MaxLatency-f.MinLatency)
	if f.ReorderRate > 0 && s.rng.Float64() < f.ReorderRate {
		d += s.randRange(f.ReorderMax)
	}
	return d
}

// randRange returns a value in [0, n], drawing nothing when n is not positive
// so that a disabled fault costs no draw and cannot shift the sequence.
func (s *Simulator) randRange(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return s.rng.Int63n(n + 1)
}

func (s *Simulator) liveNodes() int {
	live := 0
	for _, id := range s.ids {
		if !s.nodes[id].crashed() {
			live++
		}
	}
	return live
}

func (s *Simulator) quorum() int { return len(s.ids)/2 + 1 }

// Nodes returns a stable, sorted description of every node, for diagnostics.
func (s *Simulator) Nodes() []string {
	out := make([]string, 0, len(s.ids))
	for _, id := range s.ids {
		out = append(out, s.nodes[id].String())
	}
	return out
}

// Now is the current virtual time in milliseconds.
func (s *Simulator) Now() int64 { return s.now }
