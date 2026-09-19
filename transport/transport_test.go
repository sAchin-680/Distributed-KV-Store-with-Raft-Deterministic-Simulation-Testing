package transport

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sAchin-680/raftkv/raft"
)

// ---------------------------------------------------------------------------
// Wire conversion
// ---------------------------------------------------------------------------

// Every field, both directions.
//
// Conversion is where a forgotten field becomes silent misbehaviour rather than
// a compile error: a vote request that arrives with LastLogTerm zero is not a
// crash, it is an election restriction that quietly stops restricting.
func TestMessageRoundTripsEveryField(t *testing.T) {
	snap := raft.Snapshot{
		LastIncludedIndex: 77, LastIncludedTerm: 9,
		Config: raft.NewConfiguration([]raft.NodeID{1, 2, 3}, 8),
		Data:   []byte("state"),
	}

	for _, want := range []raft.Message{
		{
			Type: raft.MsgVoteReq, From: 2, To: 3, Term: 5,
			LastLogIndex: 42, LastLogTerm: 4, PreVote: true,
		},
		{Type: raft.MsgVoteResp, From: 3, To: 2, Term: 5, Granted: true, PreVote: true},
		{Type: raft.MsgVoteResp, From: 3, To: 2, Term: 5, Granted: false},
		{
			Type: raft.MsgAppendReq, From: 1, To: 2, Term: 7,
			PrevLogIndex: 10, PrevLogTerm: 6, LeaderCommit: 9, ReadID: 1234,
			Entries: []raft.LogEntry{
				{Index: 11, Term: 7, Type: raft.EntryNormal, Command: []byte("set a=1")},
				{Index: 12, Term: 7, Type: raft.EntryNoOp},
			},
		},
		{
			Type: raft.MsgAppendResp, From: 2, To: 1, Term: 7,
			Success: false, ConflictIndex: 8, ConflictTerm: 5, ReadID: 1234,
		},
		{Type: raft.MsgAppendResp, From: 2, To: 1, Term: 7, Success: true, MatchIndex: 12},
		{Type: raft.MsgSnapshotReq, From: 1, To: 4, Term: 9, Snapshot: &snap},
		{Type: raft.MsgSnapshotResp, From: 4, To: 1, Term: 9, Success: true, MatchIndex: 77},
	} {
		t.Run(want.Type.String(), func(t *testing.T) {
			p, err := toProto(want)
			if err != nil {
				t.Fatalf("toProto(%s): %v", want, err)
			}
			got, err := fromProto(p)
			if err != nil {
				t.Fatalf("fromProto: %v", err)
			}
			assertMessageEqual(t, got, want)
		})
	}
}

func assertMessageEqual(t *testing.T, got, want raft.Message) {
	t.Helper()

	for _, f := range []struct {
		name      string
		got, want any
	}{
		{"type", got.Type, want.Type},
		{"from", got.From, want.From},
		{"to", got.To, want.To},
		{"term", got.Term, want.Term},
		{"pre-vote", got.PreVote, want.PreVote},
		{"last log index", got.LastLogIndex, want.LastLogIndex},
		{"last log term", got.LastLogTerm, want.LastLogTerm},
		{"granted", got.Granted, want.Granted},
		{"prev log index", got.PrevLogIndex, want.PrevLogIndex},
		{"prev log term", got.PrevLogTerm, want.PrevLogTerm},
		{"leader commit", got.LeaderCommit, want.LeaderCommit},
		{"success", got.Success, want.Success},
		{"match index", got.MatchIndex, want.MatchIndex},
		{"conflict index", got.ConflictIndex, want.ConflictIndex},
		{"conflict term", got.ConflictTerm, want.ConflictTerm},
		{"read id", got.ReadID, want.ReadID},
	} {
		if f.got != f.want {
			t.Errorf("%s = %v, want %v", f.name, f.got, f.want)
		}
	}

	if len(got.Entries) != len(want.Entries) {
		t.Fatalf("entries: got %d, want %d", len(got.Entries), len(want.Entries))
	}
	for i := range want.Entries {
		if got.Entries[i].Index != want.Entries[i].Index ||
			got.Entries[i].Term != want.Entries[i].Term ||
			got.Entries[i].Type != want.Entries[i].Type ||
			string(got.Entries[i].Command) != string(want.Entries[i].Command) {
			t.Errorf("entry %d: got %+v, want %+v", i, got.Entries[i], want.Entries[i])
		}
	}

	switch {
	case want.Snapshot == nil && got.Snapshot != nil:
		t.Error("snapshot appeared from nowhere")
	case want.Snapshot != nil && got.Snapshot == nil:
		t.Fatal("snapshot lost in transit")
	case want.Snapshot != nil:
		if got.Snapshot.LastIncludedIndex != want.Snapshot.LastIncludedIndex ||
			got.Snapshot.LastIncludedTerm != want.Snapshot.LastIncludedTerm {
			t.Errorf("snapshot boundary: got %s, want %s", got.Snapshot, want.Snapshot)
		}
		if !got.Snapshot.Config.Equal(want.Snapshot.Config) {
			t.Errorf("snapshot configuration: got %s, want %s — without it a "+
				"restoring node does not know its peers",
				got.Snapshot.Config, want.Snapshot.Config)
		}
	}
}

func TestSnapshotMessageWithoutASnapshotIsRejected(t *testing.T) {
	_, err := toProto(raft.Message{Type: raft.MsgSnapshotReq, From: 1, To: 2, Term: 1})
	if err == nil {
		t.Error("encoding a snapshot message with no snapshot should fail loudly, " +
			"not send an empty one a follower would install")
	}
}

// ---------------------------------------------------------------------------
// Over a real network
// ---------------------------------------------------------------------------

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newPair starts two transports on loopback, wired to each other.
func newPair(t *testing.T) (*GRPC, *GRPC) {
	t.Helper()

	// Bind to port 0 first to learn the addresses, then restart with peers
	// configured — the two nodes need each other's addresses up front.
	addrs := make([]string, 2)
	for i := range addrs {
		l, err := listenLoopback()
		if err != nil {
			t.Fatalf("reserving port: %v", err)
		}
		addrs[i] = l.Addr().String()
		_ = l.Close()
	}

	peers := []Peer{{ID: 1, Address: addrs[0]}, {ID: 2, Address: addrs[1]}}

	a, err := NewGRPC(GRPCConfig{ID: 1, Listen: addrs[0], Peers: peers, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("starting node 1: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := NewGRPC(GRPCConfig{ID: 2, Listen: addrs[1], Peers: peers, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("starting node 2: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	return a, b
}

// recvWithin waits for one message, failing the test on timeout.
func recvWithin(t *testing.T, tr Transport, d time.Duration) raft.Message {
	t.Helper()
	select {
	case m, ok := <-tr.Recv():
		if !ok {
			t.Fatal("transport closed while waiting for a message")
		}
		return m
	case <-time.After(d):
		t.Fatalf("no message within %s", d)
		return raft.Message{}
	}
}

// sendUntilDelivered retries because the peer stream is established
// asynchronously: the first few sends can legitimately land before the
// connection is up, and Raft's own retries are what cover that in production.
func sendUntilDelivered(t *testing.T, from Transport, to Transport, m raft.Message) raft.Message {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		from.Send(m)
		select {
		case got, ok := <-to.Recv():
			if !ok {
				t.Fatal("transport closed")
			}
			return got
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("message never arrived")
		}
	}
}

func TestMessagesCrossTheNetwork(t *testing.T) {
	a, b := newPair(t)

	want := raft.Message{
		Type: raft.MsgAppendReq, From: 1, To: 2, Term: 3,
		PrevLogIndex: 5, PrevLogTerm: 2, LeaderCommit: 4,
		Entries: []raft.LogEntry{{Index: 6, Term: 3, Command: []byte("hello")}},
	}

	got := sendUntilDelivered(t, a, b, want)
	assertMessageEqual(t, got, want)

	// And back the other way, over the peer's own stream.
	reply := raft.Message{Type: raft.MsgAppendResp, From: 2, To: 1, Term: 3,
		Success: true, MatchIndex: 6}
	b.Send(reply)
	assertMessageEqual(t, recvWithin(t, a, 10*time.Second), reply)
}

func TestManyMessagesArriveInOrder(t *testing.T) {
	a, b := newPair(t)

	// Establish the stream before measuring order.
	sendUntilDelivered(t, a, b, raft.Message{Type: raft.MsgAppendResp, From: 1, To: 2, Term: 1})

	const n = 200
	for i := range n {
		a.Send(raft.Message{
			Type: raft.MsgAppendReq, From: 1, To: 2, Term: 1,
			PrevLogIndex: raft.Index(i),
			Entries:      []raft.LogEntry{{Index: raft.Index(i + 1), Term: 1}},
		})
	}

	// A single stream preserves order, which Raft does not require but which
	// makes the common case cheaper: a follower that receives appends in order
	// never has to reject one it would otherwise have to.
	for i := range n {
		got := recvWithin(t, b, 10*time.Second)
		if got.PrevLogIndex != raft.Index(i) {
			t.Fatalf("message %d arrived with prevLogIndex %d; a single stream "+
				"should preserve order", i, got.PrevLogIndex)
		}
	}
}

// A message for a node this transport has no route to must be dropped quietly.
// Raft addresses peers by ID and a configuration can legitimately name a node
// this one has not been told how to reach.
func TestSendToUnknownPeerIsDropped(t *testing.T) {
	a, _ := newPair(t)
	a.Send(raft.Message{Type: raft.MsgAppendReq, From: 1, To: 99, Term: 1})
	// Nothing to assert beyond not panicking or blocking; reaching here is it.
}

// An unreachable peer must not block the sender. In a real cluster this is the
// normal state during a rolling restart, and a transport that blocked would stop
// the node driver's single goroutine — and with it the whole node.
func TestSendToDownPeerDoesNotBlock(t *testing.T) {
	addr, err := listenLoopback()
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	local := addr.Addr().String()
	_ = addr.Close()

	tr, err := NewGRPC(GRPCConfig{
		ID: 1, Listen: local, Logger: quietLogger(),
		// A peer at an address nothing is listening on.
		Peers:      []Peer{{ID: 2, Address: "127.0.0.1:1"}},
		OutboxSize: 8,
	})
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	defer func() { _ = tr.Close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Far more than the outbox holds: the excess must be dropped, not queued.
		for i := range 1000 {
			tr.Send(raft.Message{
				Type: raft.MsgAppendReq, From: 1, To: 2, Term: raft.Term(i),
			})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sending to an unreachable peer blocked; that would stall the " +
			"node's run loop and take the node down with the peer")
	}
}

func TestCloseIsIdempotentAndClosesRecv(t *testing.T) {
	a, _ := newPair(t)

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}

	select {
	case _, ok := <-a.Recv():
		if ok {
			t.Error("Recv delivered a message after Close")
		}
	case <-time.After(time.Second):
		t.Error("Recv channel was not closed")
	}
}

func TestPeerAddressAndIDAreSeparate(t *testing.T) {
	// Not a behaviour test so much as a statement of intent: consensus addresses
	// peers by a stable ID, while the address is an implementation detail that
	// changes whenever a process is rescheduled. Conflating them is what breaks
	// a Raft deployment the first time a pod is replaced.
	p := Peer{ID: 7, Address: "kv-2.kv-headless.default.svc.cluster.local:9001"}
	if p.ID != 7 {
		t.Fatalf("id = %d", p.ID)
	}
	if fmt.Sprint(p.ID) == p.Address {
		t.Fatal("identity and address should never be the same thing")
	}
}
