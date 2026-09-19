// Package node drives the consensus core in a running process.
//
// The core is a pure state machine: it does not tick itself, send its own
// messages, or write its own disk. This package is what does those things, and
// it does them from exactly one goroutine.
//
// That is the whole design. Every mutation of Raft state happens inside run(),
// and nothing else may touch it — not a mutex around shared state, but a single
// goroutine that is the only thing permitted to reach it at all. Callers
// communicate by sending on channels and waiting for a reply. Most broken Raft
// implementations are broken by data races rather than by the algorithm; this
// structure makes that category unrepresentable rather than merely unlikely.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sAchin-680/raftkv/clock"
	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/transport"
)

// Errors a caller can act on.
var (
	// ErrStopped means the node is shutting down or already stopped.
	ErrStopped = errors.New("node: stopped")

	// ErrNotLeader means this node cannot accept the write. The caller should
	// redirect to Status().Lead, or retry once a leader is known.
	ErrNotLeader = raft.ErrNotLeader

	// ErrProposalDropped means leadership changed before the proposal
	// committed.
	//
	// It does NOT mean the command was not applied. The entry may have been
	// replicated and may still commit under the new leader; this node simply
	// stopped being able to tell. Raft is at-least-once, and a client that
	// retries on this error can produce a duplicate. That is exactly why the
	// client protocol carries a session: so the state machine can recognize the
	// retry and replay the original answer instead of applying it twice.
	ErrProposalDropped = errors.New("node: proposal dropped, leadership changed")
)

// StateMachine is the application the log is replicated on behalf of.
//
// Apply is called from the node's single run goroutine, in log order, exactly
// once per committed entry per node, and never concurrently. Implementations do
// not need locking for Apply — but anything serving reads from another
// goroutine does.
//
// Apply must be deterministic. Two nodes applying the same entry must reach the
// same state, or the replicated log stops meaning anything.
//
// The returned value is handed back to whoever proposed the command, and is
// what lets a write answer something more than "it committed" — whether a
// delete removed anything, or whether the command was a duplicate whose stored
// answer was replayed. It travels no further than the proposing node: the entry
// is replicated, the answer is not.
type StateMachine interface {
	Apply(entry raft.LogEntry) (any, error)
}

// Config configures a node.
type Config struct {
	ID      raft.NodeID
	Peers   []transport.Peer
	Storage raft.Storage

	Transport    transport.Transport
	Clock        clock.Clock
	StateMachine StateMachine

	// TickInterval is real time between logical ticks. The election timeout is
	// TickInterval × ElectionTick, randomized up to double that.
	TickInterval time.Duration

	ElectionTick  int
	HeartbeatTick int
	PreVote       bool

	// MaxEntriesPerMessage and MaxApplyEntries bound one AppendEntries and one
	// apply batch.
	MaxEntriesPerMessage int
	MaxApplyEntries      int

	// ProposeBuffer bounds how many proposals can be waiting for the run loop.
	ProposeBuffer int

	Logger *slog.Logger

	// Rand supplies election-timeout jitter. Nil means a source seeded from the
	// node ID and the current time, which is right in production and wrong in a
	// test that wants reproducibility.
	Rand raft.Rand
}

func (c *Config) withDefaults() error {
	switch {
	case c.ID == raft.None:
		return errors.New("node: ID must not be zero")
	case c.Storage == nil:
		return errors.New("node: Storage is required")
	case c.Transport == nil:
		return errors.New("node: Transport is required")
	case c.StateMachine == nil:
		return errors.New("node: StateMachine is required")
	}

	if c.Clock == nil {
		c.Clock = clock.System
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 2
	}
	if c.ProposeBuffer == 0 {
		c.ProposeBuffer = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Rand == nil {
		c.Rand = newSeededRand(c.ID)
	}
	return nil
}

// proposal is one client write waiting on the run loop.
type proposal struct {
	cmd  []byte
	done chan proposeResult
}

type proposeResult struct {
	index    raft.Index
	response any
	err      error
}

// Result is what a completed proposal reports.
type Result struct {
	// Index is where the command landed in the log.
	Index raft.Index

	// Response is whatever the state machine returned for it.
	Response any
}

// awaiting is a proposal that has been appended and is waiting to be applied.
//
// The term is kept alongside the index because an index is not a unique handle
// on an entry: if leadership changes, a new leader can put a *different* entry
// at the same index. Resolving the proposal on index alone would report success
// for a command that was silently replaced.
type awaiting struct {
	term raft.Term
	p    *proposal
}

// Node is a running member of a Raft cluster.
type Node struct {
	cfg Config
	log *slog.Logger

	// rn is owned exclusively by run(). Nothing else may touch it.
	rn *raft.RawNode

	proposeCh chan *proposal
	statusCh  chan chan Status
	readCh    chan *readRequest

	// pending and pendingReads are owned by run() too.
	pending      map[raft.Index]awaiting
	pendingReads map[uint64]*readRequest
	readSeq      uint64

	// lastState and lastTerm detect leadership changes between iterations.
	lastState raft.State
	lastTerm  raft.Term

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// Start creates a node and begins running it.
func Start(cfg Config) (*Node, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}

	voters := make([]raft.NodeID, 0, len(cfg.Peers)+1)
	for _, p := range cfg.Peers {
		voters = append(voters, p.ID)
	}
	if !contains(voters, cfg.ID) {
		voters = append(voters, cfg.ID)
	}

	rn, err := raft.NewRawNode(raft.Config{
		ID:                   cfg.ID,
		Storage:              cfg.Storage,
		ElectionTick:         cfg.ElectionTick,
		HeartbeatTick:        cfg.HeartbeatTick,
		PreVote:              cfg.PreVote,
		Rand:                 cfg.Rand,
		Bootstrap:            raft.NewConfiguration(voters),
		MaxEntriesPerMessage: cfg.MaxEntriesPerMessage,
		MaxApplyEntries:      cfg.MaxApplyEntries,
	})
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:          cfg,
		log:          cfg.Logger.With("component", "node", "node", uint64(cfg.ID)),
		rn:           rn,
		proposeCh:    make(chan *proposal, cfg.ProposeBuffer),
		statusCh:     make(chan chan Status),
		readCh:       make(chan *readRequest, cfg.ProposeBuffer),
		pending:      make(map[raft.Index]awaiting),
		pendingReads: make(map[uint64]*readRequest),
		lastState:    rn.State(),
		lastTerm:     rn.Term(),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}

	go n.run()
	n.log.Info("node started",
		"peers", len(cfg.Peers),
		"tick", cfg.TickInterval,
		"election_timeout", time.Duration(cfg.ElectionTick)*cfg.TickInterval)
	return n, nil
}

// ID is this node's identity.
func (n *Node) ID() raft.NodeID { return n.cfg.ID }

// Done is closed once the run loop has exited.
func (n *Node) Done() <-chan struct{} { return n.doneCh }

// Stop shuts the node down and waits for the run loop to finish. Safe to call
// more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	<-n.doneCh
}

// ---------------------------------------------------------------------------
// The run loop — the only place Raft state is touched
// ---------------------------------------------------------------------------

func (n *Node) run() {
	defer close(n.doneCh)

	ticker := n.cfg.Clock.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	recv := n.cfg.Transport.Recv()

	for {
		select {
		case <-n.stopCh:
			n.failPending(ErrStopped)
			n.failReads(ErrStopped)
			n.log.Info("node stopped")
			return

		case <-ticker.C():
			if err := n.rn.Tick(); err != nil {
				n.fatal("tick", err)
				return
			}

		case m, ok := <-recv:
			if !ok {
				// The transport closed underneath us; there is nothing left to
				// drive this node.
				n.failPending(ErrStopped)
				n.failReads(ErrStopped)
				n.log.Info("transport closed, stopping")
				return
			}
			if err := n.rn.Step(m); err != nil && !errors.Is(err, raft.ErrIgnoredMessage) {
				n.fatal("step", err)
				return
			}

		case p := <-n.proposeCh:
			n.handlePropose(p)

		case req := <-n.readCh:
			n.handleRead(req)

		case reply := <-n.statusCh:
			reply <- n.statusLocked()
		}

		if err := n.flush(); err != nil {
			n.fatal("flush", err)
			return
		}
	}
}

// flush moves everything the core produced to where it belongs: messages to the
// network, committed entries to the state machine, answers to waiting clients.
func (n *Node) flush() error {
	for _, m := range n.rn.Messages() {
		n.cfg.Transport.Send(m)
	}

	n.resolveReads()

	if err := n.applyCommitted(); err != nil {
		return err
	}

	// A node that has stopped being leader cannot say what became of the
	// proposals it was holding.
	if state := n.rn.State(); state != n.lastState || n.rn.Term() != n.lastTerm {
		if n.lastState == raft.Leader && state != raft.Leader {
			n.failPending(ErrProposalDropped)
			n.failReads(ErrNotLeader)
		}
		n.log.Info("state change",
			"from", n.lastState.String(), "to", state.String(),
			"term", uint64(n.rn.Term()), "leader", uint64(n.rn.Lead()))
		n.lastState, n.lastTerm = state, n.rn.Term()
	}
	return nil
}

func (n *Node) applyCommitted() error {
	for n.rn.HasCommittedEntries() {
		entries, err := n.rn.CommittedEntries()
		if err != nil {
			return fmt.Errorf("reading committed entries: %w", err)
		}
		if len(entries) == 0 {
			break
		}

		for _, e := range entries {
			var response any
			// A no-op carries nothing for the application; it exists so the
			// leader has an entry of its own term to commit.
			if e.Type == raft.EntryNormal {
				var err error
				response, err = n.cfg.StateMachine.Apply(e)
				if err != nil {
					return fmt.Errorf("applying index %d: %w", e.Index, err)
				}
			}
			n.resolve(e, response)
		}
		n.rn.ApplyTo(entries[len(entries)-1].Index)
	}
	return nil
}

// resolve answers whoever proposed the entry now being applied.
func (n *Node) resolve(e raft.LogEntry, response any) {
	w, ok := n.pending[e.Index]
	if !ok {
		return
	}
	delete(n.pending, e.Index)

	if w.term != e.Term {
		// A different leader put a different entry at this index. The command
		// this caller proposed was replaced, not applied.
		w.p.done <- proposeResult{err: ErrProposalDropped}
		return
	}
	w.p.done <- proposeResult{index: e.Index, response: response}
}

func (n *Node) handlePropose(p *proposal) {
	index, err := n.rn.Propose(p.cmd)
	if err != nil {
		p.done <- proposeResult{err: err}
		return
	}
	n.pending[index] = awaiting{term: n.rn.Term(), p: p}
}

func (n *Node) failPending(err error) {
	for idx, w := range n.pending {
		delete(n.pending, idx)
		w.p.done <- proposeResult{err: err}
	}
}

// fatal reports an error the node cannot continue from.
//
// Storage failures and safety violations are both in this category. Continuing
// past either would mean operating a node whose durable state is unknown, which
// is strictly worse than having one fewer node: the cluster tolerates a node
// being gone, and does not tolerate one lying.
func (n *Node) fatal(during string, err error) {
	n.log.Error("node failed, stopping", "during", during, "error", err)
	wrapped := fmt.Errorf("node: failed during %s: %w", during, err)
	n.failPending(wrapped)
	n.failReads(wrapped)
}

// ---------------------------------------------------------------------------
// Client surface
// ---------------------------------------------------------------------------

// Propose replicates a command and returns once it has been applied here.
//
// An error does not always mean the command was not applied — see
// ErrProposalDropped.
func (n *Node) Propose(ctx context.Context, cmd []byte) (Result, error) {
	p := &proposal{cmd: cmd, done: make(chan proposeResult, 1)}

	select {
	case n.proposeCh <- p:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-n.doneCh:
		return Result{}, ErrStopped
	}

	select {
	case res := <-p.done:
		if res.err != nil {
			return Result{}, res.err
		}
		return Result{Index: res.index, Response: res.response}, nil
	case <-ctx.Done():
		// The proposal may still commit. The caller cannot tell, which is the
		// honest answer and the reason client sessions exist.
		return Result{}, ctx.Err()
	case <-n.doneCh:
		return Result{}, ErrStopped
	}
}

// Status is a snapshot of what this node currently believes.
type Status struct {
	ID    raft.NodeID
	State raft.State
	Term  raft.Term
	Lead  raft.NodeID

	CommitIndex  raft.Index
	AppliedIndex raft.Index
	LastIndex    raft.Index

	Config raft.Configuration

	// Match is the leader's view of how far each peer has caught up. Empty on a
	// follower. The gap between it and CommitIndex is replication lag, which is
	// what the dashboard and the quorum alert are built on.
	Match map[raft.NodeID]raft.Index
}

// IsLeader reports whether this node currently believes it leads.
//
// "Believes" is not hedging. A partitioned leader that has not yet noticed will
// answer true, which is precisely why a linearizable read cannot be served on
// the strength of this alone.
func (s Status) IsLeader() bool { return s.State == raft.Leader }

// Status asks the run loop what it currently believes.
func (n *Node) Status(ctx context.Context) (Status, error) {
	reply := make(chan Status, 1)
	select {
	case n.statusCh <- reply:
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.doneCh:
		return Status{}, ErrStopped
	}

	select {
	case s := <-reply:
		return s, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.doneCh:
		return Status{}, ErrStopped
	}
}

// statusLocked runs on the run goroutine, where reading core state is safe.
func (n *Node) statusLocked() Status {
	s := Status{
		ID:           n.rn.ID(),
		State:        n.rn.State(),
		Term:         n.rn.Term(),
		Lead:         n.rn.Lead(),
		CommitIndex:  n.rn.CommitIndex(),
		AppliedIndex: n.rn.AppliedIndex(),
		LastIndex:    n.rn.LastIndex(),
		Config:       n.rn.Configuration(),
	}

	if s.State == raft.Leader {
		s.Match = make(map[raft.NodeID]raft.Index)
		for _, id := range s.Config.Members() {
			if pr, ok := n.rn.Progress(id); ok {
				s.Match[id] = pr.Match
			}
		}
	}
	return s
}

func contains(ids []raft.NodeID, id raft.NodeID) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
