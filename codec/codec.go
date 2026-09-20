// Package codec converts between the consensus core's Go types and their
// protobuf representation.
//
// It exists as its own package because two unrelated callers need it: the gRPC
// transport, which puts messages on the wire, and the bbolt storage, which puts
// entries on disk. Keeping the conversion in one place means there is one
// definition of what an entry is on the outside of the process, and one place to
// update when a field is added.
//
// The core itself never imports this. It deals in Go values and knows nothing
// about protobuf, which is what keeps it free to be driven by a simulator.
package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	kvpb "github.com/sAchin-680/raftkv/proto/kvpb"
	raftpb "github.com/sAchin-680/raftkv/proto/raftpb"
	"github.com/sAchin-680/raftkv/raft"
)

// ---------------------------------------------------------------------------
// Log entries
// ---------------------------------------------------------------------------

// EntryToProto converts a log entry to its wire form.
func EntryToProto(e raft.LogEntry) *raftpb.LogEntry {
	return &raftpb.LogEntry{
		Term:    uint64(e.Term),
		Index:   uint64(e.Index),
		Type:    entryTypeToProto(e.Type),
		Command: e.Command,
	}
}

// EntryFromProto converts a log entry back from its wire form.
func EntryFromProto(p *raftpb.LogEntry) raft.LogEntry {
	if p == nil {
		return raft.LogEntry{}
	}
	return raft.LogEntry{
		Term:    raft.Term(p.GetTerm()),
		Index:   raft.Index(p.GetIndex()),
		Type:    entryTypeFromProto(p.GetType()),
		Command: p.GetCommand(),
	}
}

// MarshalEntry encodes an entry for storage on disk.
func MarshalEntry(e raft.LogEntry) ([]byte, error) {
	b, err := proto.Marshal(EntryToProto(e))
	if err != nil {
		return nil, fmt.Errorf("codec: marshalling entry %d: %w", e.Index, err)
	}
	return b, nil
}

// UnmarshalEntry decodes an entry read back from disk.
func UnmarshalEntry(b []byte) (raft.LogEntry, error) {
	var p raftpb.LogEntry
	if err := proto.Unmarshal(b, &p); err != nil {
		return raft.LogEntry{}, fmt.Errorf("codec: unmarshalling entry: %w", err)
	}
	return EntryFromProto(&p), nil
}

func entryTypeToProto(t raft.EntryType) raftpb.EntryType {
	switch t {
	case raft.EntryNormal:
		return raftpb.EntryType_ENTRY_TYPE_NORMAL
	case raft.EntryNoOp:
		return raftpb.EntryType_ENTRY_TYPE_NOOP
	case raft.EntryConfChange:
		return raftpb.EntryType_ENTRY_TYPE_CONF_CHANGE
	default:
		return raftpb.EntryType_ENTRY_TYPE_UNSPECIFIED
	}
}

func entryTypeFromProto(t raftpb.EntryType) raft.EntryType {
	switch t {
	case raftpb.EntryType_ENTRY_TYPE_NOOP:
		return raft.EntryNoOp
	case raftpb.EntryType_ENTRY_TYPE_CONF_CHANGE:
		return raft.EntryConfChange
	default:
		// An unrecognized type decodes as a normal entry rather than failing.
		// A log written by a newer version is not something a node can repair,
		// and refusing to read it would turn a rolling upgrade into an outage.
		return raft.EntryNormal
	}
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// ConfigurationToProto converts a cluster configuration to its wire form.
func ConfigurationToProto(c raft.Configuration) *raftpb.Configuration {
	return &raftpb.Configuration{
		Voters:    idsToProto(c.Voters),
		OldVoters: idsToProto(c.OldVoters),
		Learners:  idsToProto(c.Learners),
	}
}

// ConfigurationFromProto converts a cluster configuration back.
func ConfigurationFromProto(p *raftpb.Configuration) raft.Configuration {
	if p == nil {
		return raft.Configuration{}
	}
	c := raft.Configuration{
		Voters:    idsFromProto(p.GetVoters()),
		OldVoters: idsFromProto(p.GetOldVoters()),
		Learners:  idsFromProto(p.GetLearners()),
	}
	// Round-tripping must not depend on the sender having normalized: rebuild
	// through the constructor so the sorted, deduplicated invariant the core
	// relies on for determinism holds regardless of what arrived.
	out := raft.NewConfiguration(c.Voters, c.Learners...)
	out.OldVoters = c.OldVoters
	return out
}

func idsToProto(ids []raft.NodeID) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func idsFromProto(ids []uint64) []raft.NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]raft.NodeID, len(ids))
	for i, id := range ids {
		out[i] = raft.NodeID(id)
	}
	return out
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// SnapshotToProto converts a snapshot to its wire form.
func SnapshotToProto(s raft.Snapshot) *raftpb.Snapshot {
	return &raftpb.Snapshot{
		LastIncludedIndex: uint64(s.LastIncludedIndex),
		LastIncludedTerm:  uint64(s.LastIncludedTerm),
		Config:            ConfigurationToProto(s.Config),
		Data:              s.Data,
	}
}

// SnapshotFromProto converts a snapshot back.
func SnapshotFromProto(p *raftpb.Snapshot) raft.Snapshot {
	if p == nil {
		return raft.Snapshot{}
	}
	return raft.Snapshot{
		LastIncludedIndex: raft.Index(p.GetLastIncludedIndex()),
		LastIncludedTerm:  raft.Term(p.GetLastIncludedTerm()),
		Config:            ConfigurationFromProto(p.GetConfig()),
		Data:              p.GetData(),
	}
}

// MarshalSnapshot encodes a snapshot for storage.
func MarshalSnapshot(s raft.Snapshot) ([]byte, error) {
	b, err := proto.Marshal(SnapshotToProto(s))
	if err != nil {
		return nil, fmt.Errorf("codec: marshalling snapshot at %d: %w", s.LastIncludedIndex, err)
	}
	return b, nil
}

// UnmarshalSnapshot decodes a snapshot.
func UnmarshalSnapshot(b []byte) (raft.Snapshot, error) {
	var p raftpb.Snapshot
	if err := proto.Unmarshal(b, &p); err != nil {
		return raft.Snapshot{}, fmt.Errorf("codec: unmarshalling snapshot: %w", err)
	}
	return SnapshotFromProto(&p), nil
}

// Configuration changes are encoded by the raft package itself, not here.
//
// The core applies a membership change when it *appends* the entry rather than
// handing it to the application, so it has to be able to read one. That makes a
// single definition in the package that cannot do without it the right answer;
// a second encoding here would be one more thing to keep in step.

// ---------------------------------------------------------------------------
// Hard state
// ---------------------------------------------------------------------------

// HardState has no message of its own in the peer protocol — it never travels
// between nodes, only to disk. It is encoded through the KV package's Session
// message shape only by coincidence of field types, so it gets a small explicit
// encoding instead: three varints, written and read in one place.

// MarshalHardState encodes the state Raft requires to survive a crash.
func MarshalHardState(hs raft.HardState) []byte {
	buf := make([]byte, 0, 24)
	buf = appendUvarint(buf, uint64(hs.Term))
	buf = appendUvarint(buf, uint64(hs.Vote))
	buf = appendUvarint(buf, uint64(hs.Commit))
	return buf
}

// UnmarshalHardState decodes it.
func UnmarshalHardState(b []byte) (raft.HardState, error) {
	term, n, err := readUvarint(b, "term")
	if err != nil {
		return raft.HardState{}, err
	}
	b = b[n:]
	vote, n, err := readUvarint(b, "vote")
	if err != nil {
		return raft.HardState{}, err
	}
	b = b[n:]
	commit, _, err := readUvarint(b, "commit")
	if err != nil {
		return raft.HardState{}, err
	}
	return raft.HardState{
		Term:   raft.Term(term),
		Vote:   raft.NodeID(vote),
		Commit: raft.Index(commit),
	}, nil
}

// ---------------------------------------------------------------------------
// Client sessions
// ---------------------------------------------------------------------------

// SessionFromProto converts a client session, which is how duplicate writes are
// recognized after a client retries a request it never got an answer to.
func SessionFromProto(p *kvpb.Session) (clientID, sequence uint64) {
	if p == nil {
		return 0, 0
	}
	return p.GetClientId(), p.GetSequenceNum()
}
