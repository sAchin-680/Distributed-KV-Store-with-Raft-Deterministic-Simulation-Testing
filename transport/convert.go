package transport

import (
	"fmt"

	"github.com/sAchin-680/raftkv/codec"
	raftpb "github.com/sAchin-680/raftkv/proto/raftpb"
	"github.com/sAchin-680/raftkv/raft"
)

// toProto renders a core message in its wire form.
func toProto(m raft.Message) (*raftpb.RaftMessage, error) {
	out := &raftpb.RaftMessage{From: uint64(m.From), To: uint64(m.To)}

	switch m.Type {
	case raft.MsgVoteReq:
		out.Payload = &raftpb.RaftMessage_VoteRequest{VoteRequest: &raftpb.RequestVoteRequest{
			Term:         uint64(m.Term),
			CandidateId:  uint64(m.From),
			LastLogIndex: uint64(m.LastLogIndex),
			LastLogTerm:  uint64(m.LastLogTerm),
			PreVote:      m.PreVote,
		}}

	case raft.MsgVoteResp:
		out.Payload = &raftpb.RaftMessage_VoteResponse{VoteResponse: &raftpb.RequestVoteResponse{
			Term:        uint64(m.Term),
			VoteGranted: m.Granted,
			PreVote:     m.PreVote,
		}}

	case raft.MsgAppendReq:
		entries := make([]*raftpb.LogEntry, len(m.Entries))
		for i, e := range m.Entries {
			entries[i] = codec.EntryToProto(e)
		}
		out.Payload = &raftpb.RaftMessage_AppendRequest{AppendRequest: &raftpb.AppendEntriesRequest{
			Term:         uint64(m.Term),
			LeaderId:     uint64(m.From),
			PrevLogIndex: uint64(m.PrevLogIndex),
			PrevLogTerm:  uint64(m.PrevLogTerm),
			Entries:      entries,
			LeaderCommit: uint64(m.LeaderCommit),
			ReadId:       m.ReadID,
		}}

	case raft.MsgAppendResp:
		out.Payload = &raftpb.RaftMessage_AppendResponse{AppendResponse: &raftpb.AppendEntriesResponse{
			Term:          uint64(m.Term),
			Success:       m.Success,
			MatchIndex:    uint64(m.MatchIndex),
			ConflictIndex: uint64(m.ConflictIndex),
			ConflictTerm:  uint64(m.ConflictTerm),
			ReadId:        m.ReadID,
		}}

	case raft.MsgSnapshotReq:
		if m.Snapshot == nil {
			return nil, fmt.Errorf("transport: snapshot message from %d carries no snapshot", m.From)
		}
		out.Payload = &raftpb.RaftMessage_SnapshotRequest{SnapshotRequest: &raftpb.InstallSnapshotRequest{
			Term:     uint64(m.Term),
			LeaderId: uint64(m.From),
			Snapshot: codec.SnapshotToProto(*m.Snapshot),
		}}

	case raft.MsgSnapshotResp:
		out.Payload = &raftpb.RaftMessage_SnapshotResponse{SnapshotResponse: &raftpb.InstallSnapshotResponse{
			Term:       uint64(m.Term),
			Success:    m.Success,
			MatchIndex: uint64(m.MatchIndex),
		}}

	default:
		return nil, fmt.Errorf("transport: cannot encode message type %s", m.Type)
	}
	return out, nil
}

// fromProto rebuilds a core message from the wire.
func fromProto(p *raftpb.RaftMessage) (raft.Message, error) {
	if p == nil {
		return raft.Message{}, fmt.Errorf("transport: nil message")
	}
	m := raft.Message{From: raft.NodeID(p.GetFrom()), To: raft.NodeID(p.GetTo())}

	switch payload := p.GetPayload().(type) {
	case *raftpb.RaftMessage_VoteRequest:
		r := payload.VoteRequest
		m.Type = raft.MsgVoteReq
		m.Term = raft.Term(r.GetTerm())
		m.LastLogIndex = raft.Index(r.GetLastLogIndex())
		m.LastLogTerm = raft.Term(r.GetLastLogTerm())
		m.PreVote = r.GetPreVote()
		// Trust the envelope over the body. They should agree; if a buggy or
		// hostile peer disagrees with itself, the envelope is what routing and
		// the checker already used.
		if m.From == raft.None {
			m.From = raft.NodeID(r.GetCandidateId())
		}

	case *raftpb.RaftMessage_VoteResponse:
		r := payload.VoteResponse
		m.Type = raft.MsgVoteResp
		m.Term = raft.Term(r.GetTerm())
		m.Granted = r.GetVoteGranted()
		m.PreVote = r.GetPreVote()

	case *raftpb.RaftMessage_AppendRequest:
		r := payload.AppendRequest
		m.Type = raft.MsgAppendReq
		m.Term = raft.Term(r.GetTerm())
		m.PrevLogIndex = raft.Index(r.GetPrevLogIndex())
		m.PrevLogTerm = raft.Term(r.GetPrevLogTerm())
		m.LeaderCommit = raft.Index(r.GetLeaderCommit())
		m.ReadID = r.GetReadId()
		if n := len(r.GetEntries()); n > 0 {
			m.Entries = make([]raft.LogEntry, n)
			for i, e := range r.GetEntries() {
				m.Entries[i] = codec.EntryFromProto(e)
			}
		}
		if m.From == raft.None {
			m.From = raft.NodeID(r.GetLeaderId())
		}

	case *raftpb.RaftMessage_AppendResponse:
		r := payload.AppendResponse
		m.Type = raft.MsgAppendResp
		m.Term = raft.Term(r.GetTerm())
		m.Success = r.GetSuccess()
		m.MatchIndex = raft.Index(r.GetMatchIndex())
		m.ConflictIndex = raft.Index(r.GetConflictIndex())
		m.ConflictTerm = raft.Term(r.GetConflictTerm())
		m.ReadID = r.GetReadId()

	case *raftpb.RaftMessage_SnapshotRequest:
		r := payload.SnapshotRequest
		m.Type = raft.MsgSnapshotReq
		m.Term = raft.Term(r.GetTerm())
		snap := codec.SnapshotFromProto(r.GetSnapshot())
		m.Snapshot = &snap
		if m.From == raft.None {
			m.From = raft.NodeID(r.GetLeaderId())
		}

	case *raftpb.RaftMessage_SnapshotResponse:
		r := payload.SnapshotResponse
		m.Type = raft.MsgSnapshotResp
		m.Term = raft.Term(r.GetTerm())
		m.Success = r.GetSuccess()
		m.MatchIndex = raft.Index(r.GetMatchIndex())

	default:
		return raft.Message{}, fmt.Errorf("transport: message from %d has no recognized payload", m.From)
	}
	return m, nil
}
