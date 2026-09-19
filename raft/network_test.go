package raft

import (
	"errors"
	"math/rand"
	"slices"
	"testing"
)

// A minimal synchronous message bus for exercising the core in unit tests.
//
// This is not the simulator. It delivers every message immediately, in node-ID
// order, with no delay, loss, reordering or duplication — enough to assert that
// the algorithm does the right thing when nothing goes wrong, and to set up
// specific scenarios by hand. Adversarial scheduling is the simulator's job.
//
// It is still strictly deterministic: nodes are always visited in sorted order,
// never in map order, for the same reason the core keeps its peer lists sorted.
type network struct {
	t     testing.TB
	ids   []NodeID
	nodes map[NodeID]*RawNode

	// partitioned nodes exchange no messages with anyone outside their group.
	partitioned map[NodeID]bool
}

type clusterOptions struct {
	preVote       bool
	electionTick  int
	heartbeatTick int
	learners      []NodeID
	seed          int64

	// jitter, when set, replaces the random source with a fixed offset per
	// node, making election order completely predictable.
	jitter map[NodeID]int

	// logs seeds a node's log before it starts; logs[id][i] is the term of the
	// entry at index i+1.
	logs map[NodeID][]Term
}

type clusterOption func(*clusterOptions)

func withPreVote(on bool) clusterOption { return func(o *clusterOptions) { o.preVote = on } }

func withLearners(ids ...NodeID) clusterOption {
	return func(o *clusterOptions) { o.learners = ids }
}

// withJitter fixes each node's election-timeout offset, so that the node with
// the smallest offset always campaigns first. Removes the randomness from tests
// that are about something other than randomness.
func withSeed(seed int64) clusterOption { return func(o *clusterOptions) { o.seed = seed } }

func withJitter(j map[NodeID]int) clusterOption {
	return func(o *clusterOptions) { o.jitter = j }
}

func withLog(id NodeID, terms ...Term) clusterOption {
	return func(o *clusterOptions) {
		if o.logs == nil {
			o.logs = map[NodeID][]Term{}
		}
		o.logs[id] = terms
	}
}

// fixedRand always returns the same jitter, making a node's election timeout
// exactly ElectionTick + n.
type fixedRand struct{ n int }

func (f fixedRand) Intn(int) int { return f.n }

// seededRand is a real random source, explicitly seeded and passed in — never
// the package-level one, which would make runs unreproducible.
type seededRand struct{ r *rand.Rand }

func (s seededRand) Intn(n int) int { return s.r.Intn(n) }

func newNetwork(t testing.TB, voters []NodeID, opts ...clusterOption) *network {
	t.Helper()

	o := clusterOptions{preVote: true, electionTick: 10, heartbeatTick: 1, seed: 1}
	for _, opt := range opts {
		opt(&o)
	}

	conf := NewConfiguration(voters, o.learners...)
	all := conf.Members()

	n := &network{
		t:           t,
		ids:         all,
		nodes:       make(map[NodeID]*RawNode, len(all)),
		partitioned: map[NodeID]bool{},
	}

	for _, id := range all {
		st := NewMemoryStorage()
		if terms, ok := o.logs[id]; ok && len(terms) > 0 {
			ents := make([]LogEntry, len(terms))
			for i, term := range terms {
				ents[i] = LogEntry{Index: Index(i + 1), Term: term}
			}
			if err := st.AppendEntries(ents); err != nil {
				t.Fatalf("seeding log for %d: %v", id, err)
			}
			// A node whose log is seeded must also know the term it reached,
			// otherwise it would campaign at term 1 with a term-5 log.
			if err := st.SetHardState(HardState{Term: terms[len(terms)-1]}); err != nil {
				t.Fatalf("seeding hard state for %d: %v", id, err)
			}
		}

		var rng Rand
		if o.jitter != nil {
			rng = fixedRand{n: o.jitter[id]}
		} else {
			rng = seededRand{rand.New(rand.NewSource(o.seed + int64(id)))} //nolint:gosec // deterministic by design
		}

		node, err := NewRawNode(Config{
			ID:            id,
			Storage:       st,
			ElectionTick:  o.electionTick,
			HeartbeatTick: o.heartbeatTick,
			PreVote:       o.preVote,
			Rand:          rng,
			Bootstrap:     conf,
		})
		if err != nil {
			t.Fatalf("creating node %d: %v", id, err)
		}
		n.nodes[id] = node
	}
	return n
}

func (n *network) node(id NodeID) *RawNode {
	n.t.Helper()
	node, ok := n.nodes[id]
	if !ok {
		n.t.Fatalf("no node %d", id)
	}
	return node
}

// reachable reports whether a message from one node can reach another. Two
// nodes can talk when they are on the same side of the partition.
func (n *network) reachable(from, to NodeID) bool {
	return n.partitioned[from] == n.partitioned[to]
}

// isolate puts the given nodes on the far side of a partition.
func (n *network) isolate(ids ...NodeID) {
	for _, id := range ids {
		n.partitioned[id] = true
	}
}

// heal removes the partition.
func (n *network) heal() { n.partitioned = map[NodeID]bool{} }

// deliver runs messages to quiescence: drain every node in ID order, step the
// messages into their recipients, repeat until nothing is in flight.
func (n *network) deliver() {
	n.t.Helper()

	const maxRounds = 1000 // a livelock is a bug, not something to wait out
	for round := 0; round < maxRounds; round++ {
		var inFlight []Message
		for _, id := range n.ids {
			inFlight = append(inFlight, n.nodes[id].Messages()...)
		}
		if len(inFlight) == 0 {
			return
		}
		for _, m := range inFlight {
			if !n.reachable(m.From, m.To) {
				continue
			}
			dst, ok := n.nodes[m.To]
			if !ok {
				continue
			}
			// ErrIgnoredMessage is the normal outcome for a stale or
			// inapplicable message; anything else is a real failure.
			if err := dst.Step(m); err != nil && !errors.Is(err, ErrIgnoredMessage) {
				n.t.Fatalf("Step(%s) on node %d: %v", m, m.To, err)
			}
		}
	}
	n.t.Fatalf("messages did not settle after %d rounds", maxRounds)
}

// tick advances one node by n ticks, delivering messages after each.
func (n *network) tick(id NodeID, ticks int) {
	n.t.Helper()
	node := n.node(id)
	for i := 0; i < ticks; i++ {
		if err := node.Tick(); err != nil {
			n.t.Fatalf("Tick on node %d: %v", id, err)
		}
		n.deliver()
	}
}

// tickAll advances every node by n ticks, in ID order.
func (n *network) tickAll(ticks int) {
	n.t.Helper()
	for i := 0; i < ticks; i++ {
		for _, id := range n.ids {
			if err := n.nodes[id].Tick(); err != nil {
				n.t.Fatalf("Tick on node %d: %v", id, err)
			}
		}
		n.deliver()
	}
}

// leaders returns every node currently claiming leadership, sorted.
func (n *network) leaders() []NodeID {
	var out []NodeID
	for _, id := range n.ids {
		if n.nodes[id].State() == Leader {
			out = append(out, id)
		}
	}
	return out
}

// requireSingleLeader asserts exactly one leader exists and returns it. This is
// Election Safety, checked directly.
func (n *network) requireSingleLeader() *RawNode {
	n.t.Helper()
	leaders := n.leaders()
	if len(leaders) != 1 {
		n.t.Fatalf("want exactly one leader, got %v\n%s", leaders, n.dump())
	}
	return n.nodes[leaders[0]]
}

// requireNoTwoLeadersPerTerm asserts that no term has produced two leaders.
func (n *network) requireNoTwoLeadersPerTerm() {
	n.t.Helper()
	seen := map[Term]NodeID{}
	for _, id := range n.ids {
		node := n.nodes[id]
		if node.State() != Leader {
			continue
		}
		if other, dup := seen[node.Term()]; dup {
			n.t.Fatalf("two leaders in term %d: %d and %d\n%s",
				node.Term(), other, id, n.dump())
		}
		seen[node.Term()] = id
	}
}

func (n *network) dump() string {
	s := "cluster state:\n"
	for _, id := range n.ids {
		s += "  " + n.nodes[id].String() + "\n"
	}
	return s
}

func (n *network) terms() []Term {
	out := make([]Term, 0, len(n.ids))
	for _, id := range n.ids {
		out = append(out, n.nodes[id].Term())
	}
	return out
}

func maxTerm(terms []Term) Term { return slices.Max(terms) }

// ---------------------------------------------------------------------------
// Replication helpers
// ---------------------------------------------------------------------------

// propose submits a command to the given node and runs the network to
// quiescence.
func (n *network) propose(id NodeID, cmd string) Index {
	n.t.Helper()
	idx, err := n.node(id).Propose([]byte(cmd))
	if err != nil {
		n.t.Fatalf("Propose on node %d: %v", id, err)
	}
	n.deliver()
	return idx
}

// logTerms returns the term of every entry in a node's log, by index.
func (n *network) logTerms(id NodeID) []Term {
	n.t.Helper()
	node := n.node(id)
	var out []Term
	for i := node.log.firstIndex(); i <= node.log.lastIndex(); i++ {
		term, err := node.log.term(i)
		if err != nil {
			n.t.Fatalf("term(%d) on node %d: %v", i, id, err)
		}
		out = append(out, term)
	}
	return out
}

// committedEntries returns a node's committed prefix.
func (n *network) committedEntries(id NodeID) []LogEntry {
	n.t.Helper()
	node := n.node(id)
	if node.log.committed == 0 {
		return nil
	}
	ents, err := node.log.entries(node.log.firstIndex(), node.log.committed+1)
	if err != nil {
		n.t.Fatalf("reading committed entries of node %d: %v", id, err)
	}
	return ents
}

// requireLogMatching asserts the log matching property across every pair of
// nodes: if two logs hold the same term at an index, they agree on every entry
// before it.
func (n *network) requireLogMatching() {
	n.t.Helper()
	for _, a := range n.ids {
		for _, b := range n.ids {
			if a >= b {
				continue
			}
			ta, tb := n.logTerms(a), n.logTerms(b)
			limit := min(len(ta), len(tb))
			for i := limit - 1; i >= 0; i-- {
				if ta[i] != tb[i] {
					continue
				}
				// Agreement at index i+1 implies agreement below it.
				for j := 0; j <= i; j++ {
					if ta[j] != tb[j] {
						n.t.Fatalf("log matching violated: nodes %d and %d agree at index %d "+
							"but differ at index %d (%v vs %v)", a, b, i+1, j+1, ta, tb)
					}
				}
				break
			}
		}
	}
}

// requireCommittedPrefixesAgree asserts that no two nodes hold a different
// entry at the same committed index. This is State Machine Safety.
func (n *network) requireCommittedPrefixesAgree() {
	n.t.Helper()
	committed := map[Index]LogEntry{}
	for _, id := range n.ids {
		for _, e := range n.committedEntries(id) {
			prev, seen := committed[e.Index]
			if seen && (prev.Term != e.Term || string(prev.Command) != string(e.Command)) {
				n.t.Fatalf("committed entry at index %d differs: %s (term %d) vs %s (term %d)\n%s",
					e.Index, prev.Command, prev.Term, e.Command, e.Term, n.dump())
			}
			committed[e.Index] = e
		}
	}
}
