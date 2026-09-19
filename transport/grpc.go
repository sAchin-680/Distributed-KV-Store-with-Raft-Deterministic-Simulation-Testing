package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	raftpb "github.com/sAchin-680/raftkv/proto/raftpb"
	"github.com/sAchin-680/raftkv/raft"
)

// GRPCConfig configures a gRPC transport.
type GRPCConfig struct {
	// ID is this node's identity, stamped on every outbound message.
	ID raft.NodeID

	// Listen is the address to serve on, e.g. ":9001".
	Listen string

	// Peers is every other node and where to reach it.
	Peers []Peer

	// InboxSize bounds the queue of received messages.
	//
	// When it fills, messages are dropped rather than blocking the gRPC
	// handler. Blocking would apply backpressure from one slow node to the
	// whole cluster; dropping loses a message the algorithm will resend anyway.
	// Of the two, dropping is the one Raft is designed for.
	InboxSize int

	// OutboxSize bounds the per-peer send queue, with the same reasoning.
	OutboxSize int

	// DialTimeout bounds the initial connection attempt to a peer.
	DialTimeout time.Duration

	Logger *slog.Logger
}

func (c *GRPCConfig) withDefaults() {
	if c.InboxSize == 0 {
		c.InboxSize = 1024
	}
	if c.OutboxSize == 0 {
		c.OutboxSize = 256
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// GRPC is a Transport over gRPC.
//
// One long-lived client stream per peer, in each direction. A healthy cluster
// heartbeats several times a second to every peer forever; paying a
// request/response round trip and HTTP/2 headers per heartbeat would be pure
// overhead, and a stream amortizes it away.
type GRPC struct {
	cfg GRPCConfig
	log *slog.Logger

	server   *grpc.Server
	listener net.Listener

	inbox chan raft.Message

	mu    sync.RWMutex
	peers map[raft.NodeID]*peerConn

	closeOnce sync.Once
	closed    chan struct{}

	// Peer goroutines and the serve goroutine are tracked separately because
	// shutdown has to drain the former before stopping the latter. See Close.
	peerWG  sync.WaitGroup
	serveWG sync.WaitGroup
}

var _ Transport = (*GRPC)(nil)

// NewGRPC starts a transport: it binds the listen address and begins dialling
// peers in the background.
//
// Peers are dialled lazily and reconnected forever, because a cluster routinely
// starts with peers that do not exist yet. Treating an unreachable peer as a
// startup error would mean the first node to boot could never succeed.
func NewGRPC(cfg GRPCConfig) (*GRPC, error) {
	cfg.withDefaults()
	if cfg.ID == raft.None {
		return nil, errors.New("transport: node ID must not be zero")
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("transport: listening on %s: %w", cfg.Listen, err)
	}

	t := &GRPC{
		cfg:      cfg,
		log:      cfg.Logger.With("component", "transport", "node", uint64(cfg.ID)),
		listener: listener,
		inbox:    make(chan raft.Message, cfg.InboxSize),
		peers:    make(map[raft.NodeID]*peerConn, len(cfg.Peers)),
		closed:   make(chan struct{}),
	}

	t.server = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Detect a peer that has vanished without closing its connection —
			// the usual outcome of a machine being killed rather than shut down.
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	raftpb.RegisterRaftServer(t.server, &server{inbox: t.inbox, log: t.log})

	for _, p := range cfg.Peers {
		if p.ID == cfg.ID {
			continue
		}
		pc := newPeerConn(t, p)
		t.peers[p.ID] = pc
		t.peerWG.Add(1)
		go func() {
			defer t.peerWG.Done()
			pc.run()
		}()
	}

	t.serveWG.Add(1)
	go func() {
		defer t.serveWG.Done()
		if err := t.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.log.Error("serve stopped", "error", err)
		}
	}()

	t.log.Info("transport listening", "address", listener.Addr().String(), "peers", len(t.peers))
	return t, nil
}

// Addr is the address actually bound, which differs from the configured one
// when the port was left to the OS.
func (t *GRPC) Addr() string { return t.listener.Addr().String() }

func (t *GRPC) Recv() <-chan raft.Message { return t.inbox }

func (t *GRPC) Send(m raft.Message) {
	t.mu.RLock()
	pc, ok := t.peers[m.To]
	t.mu.RUnlock()
	if !ok {
		t.log.Debug("no route to peer", "to", uint64(m.To), "type", m.Type.String())
		return
	}
	pc.enqueue(m)
}

// Close shuts the transport down.
//
// The order matters, and the obvious order deadlocks.
//
// GracefulStop waits for every active server RPC to finish. Our inbound RPCs are
// the peers' long-lived Deliver streams, and those only end when the *remote*
// client closes them — which a healthy peer will never do. Calling GracefulStop
// here waits forever, and a node that cannot shut down turns a rolling restart
// into an outage.
//
// So: stop our own outbound streams first and wait for them to flush, then stop
// the server forcefully. Cancelling inbound streams at that point loses nothing
// worth keeping. There is no request/response in flight whose reply a peer is
// waiting on — this is a one-way pipe — and anything the peer sent that we drop
// is something Raft will resend on its next heartbeat.
func (t *GRPC) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)

		// Let outbound streams finish what they are writing.
		t.mu.RLock()
		for _, pc := range t.peers {
			pc.close()
		}
		t.mu.RUnlock()
		t.peerWG.Wait()

		// Stop, not GracefulStop — see above. This blocks until every handler
		// has returned, which is what makes closing the inbox below safe.
		t.server.Stop()
		t.serveWG.Wait()

		close(t.inbox)
	})
	return nil
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

type server struct {
	raftpb.UnimplementedRaftServer
	inbox chan<- raft.Message
	log   *slog.Logger
}

func (s *server) Deliver(stream grpc.ClientStreamingServer[raftpb.RaftMessage, raftpb.DeliverAck]) error {
	for {
		p, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&raftpb.DeliverAck{})
		}
		if err != nil {
			return err
		}

		m, err := fromProto(p)
		if err != nil {
			// A message we cannot decode is dropped rather than tearing down the
			// stream: one malformed message should not cost a peer every
			// subsequent heartbeat.
			s.log.Warn("dropping undecodable message", "error", err)
			continue
		}

		select {
		case s.inbox <- m:
		default:
			// Full inbox. Dropping is the right failure here — see InboxSize.
			s.log.Warn("inbox full, dropping message",
				"from", uint64(m.From), "type", m.Type.String())
		}
	}
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

// peerConn owns one outbound connection and keeps it alive.
type peerConn struct {
	t    *GRPC
	peer Peer
	log  *slog.Logger

	outbox chan raft.Message
	done   chan struct{}
	once   sync.Once
}

func newPeerConn(t *GRPC, p Peer) *peerConn {
	return &peerConn{
		t:      t,
		peer:   p,
		log:    t.log.With("peer", uint64(p.ID), "address", p.Address),
		outbox: make(chan raft.Message, t.cfg.OutboxSize),
		done:   make(chan struct{}),
	}
}

func (pc *peerConn) enqueue(m raft.Message) {
	select {
	case pc.outbox <- m:
	case <-pc.done:
	default:
		// The peer is unreachable or slower than we are producing. Raft will
		// resend anything that matters, so the useful thing to do is give up on
		// this copy rather than grow an unbounded backlog of stale messages
		// that would be useless by the time they arrived.
		pc.log.Debug("outbox full, dropping message", "type", m.Type.String())
	}
}

func (pc *peerConn) close() {
	pc.once.Do(func() { close(pc.done) })
}

// run dials the peer and keeps a stream open, reconnecting for as long as the
// transport lives.
func (pc *peerConn) run() {
	conn, err := grpc.NewClient(pc.peer.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				// Capped low: a peer that has been down for a while is exactly
				// the one about to come back during a rolling restart, and
				// waiting two minutes to notice would extend every rollout.
				MaxDelay: 3 * time.Second,
			},
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		pc.log.Error("creating client", "error", err)
		return
	}
	defer func() { _ = conn.Close() }()

	client := raftpb.NewRaftClient(conn)

	for {
		select {
		case <-pc.done:
			return
		default:
		}

		if err := pc.pump(client); err != nil {
			select {
			case <-pc.done:
				return
			default:
			}
			pc.log.Debug("stream ended, reconnecting", "error", err)
			// gRPC handles connection backoff; this only paces stream retries.
			select {
			case <-time.After(200 * time.Millisecond):
			case <-pc.done:
				return
			}
		}
	}
}

// pump opens one stream and writes to it until it fails.
func (pc *peerConn) pump(client raftpb.RaftClient) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Deliver(ctx)
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}

	for {
		select {
		case <-pc.done:
			_, _ = stream.CloseAndRecv()
			return nil

		case m := <-pc.outbox:
			p, err := toProto(m)
			if err != nil {
				pc.log.Warn("dropping unencodable message", "error", err)
				continue
			}
			if err := stream.Send(p); err != nil {
				// Put nothing back: by the time a new stream is open this
				// message is stale, and Raft's own retry will produce a fresher
				// one. Re-sending stale state is worse than sending nothing.
				return fmt.Errorf("sending %s: %w", m.Type, err)
			}
		}
	}
}
