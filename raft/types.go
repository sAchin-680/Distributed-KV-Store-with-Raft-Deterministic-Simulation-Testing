// Package raft implements the Raft consensus algorithm as a pure, synchronous
// state machine.
//
// The core has no clock, no goroutines, no network and no disk of its own. It is
// driven entirely from outside by three calls — Tick, Step and Propose — and it
// communicates by producing messages and committed entries for its caller to
// drain. Time is counted in logical ticks supplied by the caller; persistence
// goes through the Storage interface; messages are values, not RPCs.
//
// That shape is deliberate. It means a single-threaded simulator can step an
// entire cluster one event at a time and replay any execution exactly from a
// seed, which is what makes the deterministic testing in package sim possible.
// It also means the production driver in package node can own all Raft state in
// exactly one goroutine, so no Raft state is ever shared across goroutines and
// an entire category of bugs cannot be written.
package raft

import (
	"fmt"
	"slices"
)

// None is the zero NodeID, used to mean "no node": no vote cast, no leader known.
const None NodeID = 0

type (
	// NodeID identifies a node. Stable for the lifetime of the cluster — peers
	// address each other by this, never by network address, which is why the
	// Kubernetes deployment needs stable pod identities.
	NodeID uint64

	// Term is Raft's logical clock. It only ever increases.
	Term uint64

	// Index is a position in the replicated log. The log is 1-based; index 0 is
	// the position before the first entry and never holds one.
	Index uint64
)

// State is the role a node is currently playing.
type State uint8

const (
	// Follower is passive: it only responds to candidates and leaders.
	Follower State = iota

	// PreCandidate is running a straw poll to find out whether an election
	// would succeed, without incrementing its term yet. See pre-vote in raft.go.
	PreCandidate

	// Candidate is standing for election at its current term.
	Candidate

	// Leader is serving the cluster and replicating its log.
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("unknown-state(%d)", uint8(s))
	}
}

// EntryType distinguishes what the state machine should do with an entry.
type EntryType uint8

const (
	// EntryNormal carries a client command, opaque to Raft.
	EntryNormal EntryType = iota

	// EntryNoOp carries nothing. A leader appends one to its own log the moment
	// it is elected.
	//
	// This is not bookkeeping. The commit rule forbids a leader from advancing
	// commitIndex over entries from earlier terms until an entry from its own
	// term has been replicated to a quorum. A leader that never appends anything
	// of its own therefore can never commit the backlog it inherited, and can
	// never establish a read index. The no-op is what unblocks both.
	EntryNoOp

	// EntryConfChange carries a marshalled ConfChange. Unlike every other entry
	// type, it takes effect when it is *appended*, not when it commits.
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryNoOp:
		return "noop"
	case EntryConfChange:
		return "conf-change"
	default:
		return fmt.Sprintf("unknown-entry-type(%d)", uint8(t))
	}
}

// LogEntry is one entry in the replicated log. Term and Index together identify
// an entry uniquely across the whole cluster, for all time — that pairing is the
// foundation the log matching property is built on.
type LogEntry struct {
	Term    Term
	Index   Index
	Type    EntryType
	Command []byte
}

func (e LogEntry) String() string {
	return fmt.Sprintf("{%d@%d %s len=%d}", e.Index, e.Term, e.Type, len(e.Command))
}

// HardState is the state Raft requires to survive a crash. Losing any of it can
// violate safety: a node that forgets it voted in a term can vote a second time
// in that term, and elect a second leader.
type HardState struct {
	Term Term
	Vote NodeID // None if this node has not voted in Term

	// Commit is persisted as an optimization only. It can be safely lost and
	// relearned from the leader; it is stored so a restarting node can serve
	// reads sooner instead of replaying from the start of its log.
	Commit Index
}

// IsEmpty reports whether hs carries nothing worth persisting.
func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.Vote == None && hs.Commit == 0
}

func (hs HardState) String() string {
	return fmt.Sprintf("{term=%d vote=%d commit=%d}", hs.Term, hs.Vote, hs.Commit)
}

// Snapshot is a point-in-time image of the state machine, standing in for every
// log entry up to and including LastIncludedIndex.
type Snapshot struct {
	LastIncludedIndex Index
	LastIncludedTerm  Term

	// Config is the cluster configuration in force as of LastIncludedIndex.
	//
	// A snapshot has to carry this. Restoring from one discards the log entries
	// that established the current membership, so without it a restored node
	// comes back not knowing who its peers are — it cannot campaign, cannot
	// count a quorum, and cannot tell a legitimate leader from a stranger.
	Config Configuration

	// Data is the serialized state machine, opaque to Raft.
	Data []byte
}

// IsEmpty reports whether s covers no entries at all.
func (s Snapshot) IsEmpty() bool { return s.LastIncludedIndex == 0 }

// Clone returns a deep copy. Snapshots cross the boundary between Storage, the
// core and the state machine, and a shared backing array between any two of
// those would be a bug that only appears under a specific interleaving.
func (s Snapshot) Clone() Snapshot {
	s.Config = s.Config.Clone()
	s.Data = slices.Clone(s.Data)
	return s
}

func (s Snapshot) String() string {
	return fmt.Sprintf("{last=%d@%d cfg=%s len=%d}",
		s.LastIncludedIndex, s.LastIncludedTerm, s.Config, len(s.Data))
}

// MessageType identifies which RPC a Message represents.
type MessageType uint8

const (
	MsgVoteReq MessageType = iota
	MsgVoteResp
	MsgAppendReq
	MsgAppendResp
	MsgSnapshotReq
	MsgSnapshotResp
)

func (t MessageType) String() string {
	switch t {
	case MsgVoteReq:
		return "vote-req"
	case MsgVoteResp:
		return "vote-resp"
	case MsgAppendReq:
		return "append-req"
	case MsgAppendResp:
		return "append-resp"
	case MsgSnapshotReq:
		return "snapshot-req"
	case MsgSnapshotResp:
		return "snapshot-resp"
	default:
		return fmt.Sprintf("unknown-message(%d)", uint8(t))
	}
}

// Message is every peer RPC and response in one flat struct.
//
// One struct rather than an interface or a union: the simulator needs to compare,
// copy, delay, duplicate and hash messages generically, and the transport needs a
// single conversion point to and from protobuf. Fields not relevant to a given
// Type are simply zero.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// --- vote ---

	// PreVote marks a straw poll: it neither advances the sender's term nor
	// causes the receiver to advance its own.
	PreVote      bool
	LastLogIndex Index
	LastLogTerm  Term
	Granted      bool

	// --- append ---

	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
	Success      bool

	// MatchIndex is how far the follower's log now agrees with the leader's.
	//
	// Sent explicitly rather than inferred by the leader from what it sent,
	// because responses can be delayed, reordered or duplicated. A leader that
	// computes match from its own in-flight state can be walked backwards by a
	// stale reply; one that reads it off the response cannot.
	MatchIndex Index

	// ConflictIndex and ConflictTerm implement the §5.3 optimization: on a
	// mismatch the follower reports where its conflicting term begins, letting
	// the leader skip a whole term per round trip instead of one index.
	ConflictIndex Index
	ConflictTerm  Term

	// ReadID is non-zero on the heartbeat round that confirms leadership for a
	// read-index read, and is echoed back untouched so replies can be matched to
	// the read that caused them.
	ReadID uint64

	// --- snapshot ---

	Snapshot *Snapshot
}

func (m Message) String() string {
	switch m.Type {
	case MsgVoteReq:
		return fmt.Sprintf("%s %d->%d term=%d pre=%t last=%d@%d",
			m.Type, m.From, m.To, m.Term, m.PreVote, m.LastLogIndex, m.LastLogTerm)
	case MsgVoteResp:
		return fmt.Sprintf("%s %d->%d term=%d pre=%t granted=%t",
			m.Type, m.From, m.To, m.Term, m.PreVote, m.Granted)
	case MsgAppendReq:
		return fmt.Sprintf("%s %d->%d term=%d prev=%d@%d n=%d commit=%d read=%d",
			m.Type, m.From, m.To, m.Term, m.PrevLogIndex, m.PrevLogTerm,
			len(m.Entries), m.LeaderCommit, m.ReadID)
	case MsgAppendResp:
		return fmt.Sprintf("%s %d->%d term=%d ok=%t match=%d conflict=%d@%d read=%d",
			m.Type, m.From, m.To, m.Term, m.Success, m.MatchIndex,
			m.ConflictIndex, m.ConflictTerm, m.ReadID)
	case MsgSnapshotReq:
		return fmt.Sprintf("%s %d->%d term=%d snap=%s", m.Type, m.From, m.To, m.Term, m.Snapshot)
	case MsgSnapshotResp:
		return fmt.Sprintf("%s %d->%d term=%d ok=%t match=%d",
			m.Type, m.From, m.To, m.Term, m.Success, m.MatchIndex)
	default:
		return fmt.Sprintf("%s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
	}
}
