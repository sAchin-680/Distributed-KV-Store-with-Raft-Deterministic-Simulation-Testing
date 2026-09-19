package raft

import (
	"errors"
	"fmt"
)

// Rand is the randomness the core needs, which is only ever election-timeout
// jitter.
//
// An interface rather than a *rand.Rand, for two reasons: it keeps math/rand out
// of the core entirely, and it lets the simulator supply a source it seeds and
// controls. Randomized election timeouts are not a detail — without them, nodes
// whose timers expire together split the vote, retry together, and can livelock
// an election indefinitely.
type Rand interface {
	// Intn returns a value in [0, n).
	Intn(n int) int
}

// Config configures a RawNode.
type Config struct {
	// ID is this node's identity. Must not be None.
	ID NodeID

	// Storage holds the durable log and hard state.
	Storage Storage

	// ElectionTick is how many Tick calls may pass without hearing from a
	// leader before this node starts an election. The actual timeout is
	// randomized per campaign in [ElectionTick, 2*ElectionTick).
	ElectionTick int

	// HeartbeatTick is how many Tick calls pass between a leader's heartbeats.
	//
	// It must be meaningfully smaller than ElectionTick. If a leader cannot get
	// several heartbeats out within one election timeout, followers will time
	// out and campaign against a perfectly healthy leader, and the cluster will
	// spend its time holding elections instead of doing work.
	HeartbeatTick int

	// PreVote enables the pre-vote straw poll before a real election.
	//
	// Worth leaving on. Without it, a node that was partitioned away comes back
	// with a term inflated by repeated failed campaigns, and that inflated term
	// forces a healthy leader to step down for no reason at all. The simulator
	// generates this scenario readily.
	PreVote bool

	// Rand supplies election-timeout jitter.
	Rand Rand

	// Bootstrap is the initial cluster configuration, used only when Storage is
	// empty. Afterwards the configuration is recovered from the snapshot and
	// the log, which are authoritative.
	Bootstrap Configuration

	// MaxApplyEntries caps how many committed entries are returned at once, so
	// a large backlog is drained in bounded chunks. Zero means unbounded.
	MaxApplyEntries int

	// MaxEntriesPerMessage caps how many entries ride in one AppendEntries.
	// Without a cap, a follower that is far behind provokes a single message
	// the size of the entire log. Zero uses the default.
	MaxEntriesPerMessage int
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: node ID must not be zero")
	case c.Storage == nil:
		return errors.New("raft: Storage is required")
	case c.Rand == nil:
		return errors.New("raft: Rand is required for election timeout jitter")
	case c.ElectionTick <= 0:
		return errors.New("raft: ElectionTick must be positive")
	case c.HeartbeatTick <= 0:
		return errors.New("raft: HeartbeatTick must be positive")
	case c.HeartbeatTick >= c.ElectionTick:
		return fmt.Errorf(
			"raft: HeartbeatTick (%d) must be less than ElectionTick (%d), or followers "+
				"will time out on a healthy leader", c.HeartbeatTick, c.ElectionTick)
	}
	return nil
}

// Progress is the leader's view of how far one peer has caught up.
//
// Next is optimistic — where the leader will try next — and Match is
// pessimistic, the highest index the follower has actually confirmed. The two
// converge as the log-matching retry loop runs. Only Match may be counted
// toward a commit; counting Next would commit entries nobody acknowledged.
type Progress struct {
	Match Index
	Next  Index
}

func (p Progress) String() string {
	return fmt.Sprintf("{match=%d next=%d}", p.Match, p.Next)
}

// RawNode is one node's Raft state machine.
//
// It is pure and synchronous: no clock, no goroutines, no I/O beyond the Storage
// interface. Callers drive it with Tick and Step, then drain Messages. See
// docs/adr/0001-pure-deterministic-core.md for why.
type RawNode struct {
	id  NodeID
	cfg Config

	// --- durable state ---

	term Term
	vote NodeID

	// persisted mirrors what was last written, so a redundant fsync is skipped
	// when nothing has actually changed.
	persisted HardState

	log *raftLog

	// --- volatile state ---

	state State
	lead  NodeID

	// conf is the cluster configuration currently in force. Derived from the
	// snapshot and the log, never configured directly after bootstrap.
	conf Configuration

	// votes records the responses to the campaign in progress. Read only via
	// point lookup — never ranged over, because Go randomizes map iteration
	// order and any ordered decision taken from it would break seed replay.
	votes map[NodeID]bool

	// progress is leader-only, one entry per peer.
	progress map[NodeID]*Progress

	electionElapsed  int
	heartbeatElapsed int

	// randomizedElectionTimeout is redrawn on every state reset. Fixed timeouts
	// make nodes campaign in lockstep and split the vote forever.
	randomizedElectionTimeout int

	msgs []Message
}

// NewRawNode creates a node, recovering any durable state from Storage.
func NewRawNode(cfg Config) (*RawNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	hs, snap, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: reading initial state: %w", err)
	}

	rlog, err := newRaftLog(cfg.Storage)
	if err != nil {
		return nil, err
	}

	r := &RawNode{
		id:        cfg.ID,
		cfg:       cfg,
		term:      hs.Term,
		vote:      hs.Vote,
		persisted: hs,
		log:       rlog,
		state:     Follower,
		lead:      None,
		votes:     make(map[NodeID]bool),
	}

	// The configuration comes from durable state when there is any. Bootstrap
	// applies only to a node that has never run: once a log exists, it is
	// authoritative about membership, and preferring a caller-supplied list
	// would let a stale command-line flag silently rewrite the cluster.
	switch {
	case !snap.Config.IsEmpty():
		r.conf = snap.Config.Clone()
	default:
		r.conf = cfg.Bootstrap.Clone()
	}

	r.becomeFollower(r.term, None)
	return r, nil
}

// ID returns this node's identity.
func (r *RawNode) ID() NodeID { return r.id }

// State returns the current role.
func (r *RawNode) State() State { return r.state }

// Term returns the current term.
func (r *RawNode) Term() Term { return r.term }

// Lead returns the leader this node currently believes in, or None.
func (r *RawNode) Lead() NodeID { return r.lead }

// Vote returns who this node voted for in the current term, or None.
func (r *RawNode) Vote() NodeID { return r.vote }

// CommitIndex returns the highest index known to be committed.
func (r *RawNode) CommitIndex() Index { return r.log.committed }

// LastIndex returns the last index in this node's log.
func (r *RawNode) LastIndex() Index { return r.log.lastIndex() }

// Configuration returns the membership currently in force.
func (r *RawNode) Configuration() Configuration { return r.conf.Clone() }

// Progress returns the leader's view of a peer, and whether it is tracked.
func (r *RawNode) Progress(id NodeID) (Progress, bool) {
	p, ok := r.progress[id]
	if !ok {
		return Progress{}, false
	}
	return *p, true
}

// Messages drains the outbound messages produced since the last call. The
// caller is responsible for delivering them; the core neither retries nor
// tracks them.
func (r *RawNode) Messages() []Message {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

func (r *RawNode) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

// ---------------------------------------------------------------------------
// Durability
// ---------------------------------------------------------------------------

// persistHardState writes term, vote and commit index if any of them changed.
//
// Every caller must run this *before* the messages it produced are released to
// the network. Raft's safety argument assumes a vote is durable before the
// response admitting to it leaves the node: reply first, crash, come back having
// forgotten, and the node can vote a second time in the same term and elect a
// second leader.
func (r *RawNode) persistHardState() error {
	hs := HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
	if hs == r.persisted {
		return nil
	}
	if err := r.cfg.Storage.SetHardState(hs); err != nil {
		return fmt.Errorf("raft: persisting hard state %s: %w", hs, err)
	}
	r.persisted = hs
	return nil
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

// reset moves the node to term and clears all per-term state.
func (r *RawNode) reset(term Term) {
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.votes = make(map[NodeID]bool)
	r.progress = nil
}

func (r *RawNode) resetRandomizedElectionTimeout() {
	// Drawn from [ElectionTick, 2*ElectionTick). The spread has to exceed the
	// time it takes a message to cross the cluster, or two nodes will keep
	// campaigning simultaneously and keep splitting the vote.
	r.randomizedElectionTimeout = r.cfg.ElectionTick + r.cfg.Rand.Intn(r.cfg.ElectionTick)
}

func (r *RawNode) becomeFollower(term Term, lead NodeID) {
	r.reset(term)
	r.state = Follower
	r.lead = lead
}

// becomePreCandidate starts a straw poll.
//
// Deliberately does not touch the term or the vote, and persists nothing: a
// pre-vote asks "would you vote for me?" without any of the commitments a real
// campaign makes. That is the entire point — a node that cannot win learns so
// without having disrupted anyone.
func (r *RawNode) becomePreCandidate() {
	if r.state == Leader {
		panic("raft: leader cannot become pre-candidate")
	}
	// Note: no reset(). Advancing the term here would defeat the purpose.
	r.state = PreCandidate
	r.votes = make(map[NodeID]bool)
	r.lead = None

	// Redraw the jitter even though the term is untouched. Two nodes whose
	// straw polls collide would otherwise retry in lockstep forever, which is
	// the split vote pre-vote is supposed to help avoid, not reproduce.
	r.resetRandomizedElectionTimeout()
}

func (r *RawNode) becomeCandidate() {
	if r.state == Leader {
		panic("raft: leader cannot become candidate")
	}
	r.reset(r.term + 1)
	r.state = Candidate
	r.vote = r.id
}

func (r *RawNode) becomeLeader() error {
	if r.state == Follower {
		panic("raft: follower cannot become leader without campaigning")
	}
	term := r.term
	r.reset(term)
	r.state = Leader
	r.lead = r.id

	r.progress = make(map[NodeID]*Progress, len(r.conf.Members()))
	last := r.log.lastIndex()
	for _, id := range r.conf.Members() {
		p := &Progress{Next: last + 1}
		if id == r.id {
			// Our own log is trivially up to date with itself.
			p.Match = last
		}
		r.progress[id] = p
	}

	// Append an empty entry of this term immediately.
	//
	// Not bookkeeping: the commit rule forbids committing entries from earlier
	// terms directly, so a leader with no entry of its own can never advance
	// commitIndex over the backlog it inherited, and can never establish a read
	// index. This entry is what unblocks both.
	noop := LogEntry{Term: term, Index: last + 1, Type: EntryNoOp}
	if err := r.log.append([]LogEntry{noop}); err != nil {
		return fmt.Errorf("raft: appending no-op on election: %w", err)
	}
	r.progress[r.id].Match = noop.Index
	r.progress[r.id].Next = noop.Index + 1

	if err := r.persistHardState(); err != nil {
		return err
	}
	r.broadcastAppend()
	return nil
}

// promotable reports whether this node may campaign. A learner, or a node that
// has been removed from the configuration, may not.
func (r *RawNode) promotable() bool {
	return r.conf.IsVoter(r.id)
}

// ---------------------------------------------------------------------------
// Tick
// ---------------------------------------------------------------------------

// Tick advances this node's logical clock by one step.
//
// The caller decides what a tick is worth. In production the driver ticks on a
// real timer; in simulation the simulator ticks a virtual clock, which is why a
// scenario spanning ten simulated seconds costs milliseconds to run.
func (r *RawNode) Tick() error {
	if r.state == Leader {
		return r.tickHeartbeat()
	}
	return r.tickElection()
}

func (r *RawNode) tickElection() error {
	r.electionElapsed++

	if !r.promotable() || r.electionElapsed < r.randomizedElectionTimeout {
		return nil
	}
	r.electionElapsed = 0
	return r.campaign(r.cfg.PreVote)
}

func (r *RawNode) tickHeartbeat() error {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.heartbeatElapsed < r.cfg.HeartbeatTick {
		return nil
	}
	r.heartbeatElapsed = 0
	r.broadcastAppend()
	return nil
}

// ---------------------------------------------------------------------------
// Step
// ---------------------------------------------------------------------------

// ErrIgnoredMessage is returned for a message that was correctly discarded — a
// stale reply, or one addressed to a different node. Not a failure.
var ErrIgnoredMessage = errors.New("raft: message ignored")

// Step delivers one message to this node.
//
// It returns an error only for genuine failures, principally storage errors.
// Messages that are stale, duplicated or simply not applicable are dropped
// silently: the network is allowed to do all three, and treating them as errors
// would make normal operation look like malfunction.
func (r *RawNode) Step(m Message) error {
	if m.To != r.id && m.To != None {
		return fmt.Errorf("raft: message addressed to %d delivered to %d: %w",
			m.To, r.id, ErrIgnoredMessage)
	}

	switch {
	case m.Term > r.term:
		if err := r.stepHigherTerm(m); err != nil {
			return err
		}
	case m.Term < r.term:
		return r.stepLowerTerm(m)
	}

	return r.dispatch(m)
}

// stepHigherTerm handles a message from a later term than ours.
//
// The general rule is absolute — a higher term means we are behind, so we adopt
// it and step down, whatever we were doing. The two exceptions both concern
// pre-votes, and both exist so that a straw poll cannot have the side effect it
// was invented to avoid.
func (r *RawNode) stepHigherTerm(m Message) error {
	switch {
	case m.Type == MsgVoteReq && m.PreVote:
		// A pre-vote request carries the term the sender *would* campaign in,
		// not a term it holds. Adopting it would let any node inflate the whole
		// cluster's term just by asking a question.
		return nil

	case m.Type == MsgVoteResp && m.PreVote && m.Granted:
		// A granted pre-vote is addressed to the hypothetical term we asked
		// about. We advance into that term by campaigning for real, not by
		// reading a reply.
		return nil

	case m.Type == MsgAppendReq || m.Type == MsgSnapshotReq:
		// These come from an established leader, so we learn who it is.
		r.becomeFollower(m.Term, m.From)

	default:
		r.becomeFollower(m.Term, None)
	}
	return r.persistHardState()
}

// stepLowerTerm handles a message from an earlier term than ours.
func (r *RawNode) stepLowerTerm(m Message) error {
	switch m.Type {
	case MsgAppendReq, MsgSnapshotReq:
		// A leader from a previous term has not noticed it was superseded.
		// Replying with our term is what tells it to step down; silence would
		// leave it broadcasting to a cluster that has moved on.
		r.send(Message{Type: MsgAppendResp, To: m.From, Success: false})

	case MsgVoteReq:
		// Reject explicitly rather than dropping. A candidate whose term is
		// behind needs to learn the real term, otherwise it campaigns again
		// with the same doomed term and never makes progress.
		r.send(Message{Type: MsgVoteResp, To: m.From, Granted: false, PreVote: m.PreVote})
	}
	return fmt.Errorf("raft: term %d below current %d: %w", m.Term, r.term, ErrIgnoredMessage)
}

func (r *RawNode) dispatch(m Message) error {
	switch m.Type {
	case MsgVoteReq:
		return r.handleVoteRequest(m)
	case MsgVoteResp:
		return r.handleVoteResponse(m)
	case MsgAppendReq:
		return r.handleAppendRequest(m)
	case MsgAppendResp:
		return r.handleAppendResponse(m)
	default:
		return fmt.Errorf("raft: unhandled message type %s: %w", m.Type, ErrIgnoredMessage)
	}
}

func (r *RawNode) String() string {
	return fmt.Sprintf("node %d [%s term=%d lead=%d vote=%d] %s",
		r.id, r.state, r.term, r.lead, r.vote, r.log)
}
