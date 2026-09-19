// Package kvserver exposes the replicated store to clients over gRPC.
//
// It serves on a different port from the peer transport. Consensus traffic and
// client traffic have nothing to do with each other: they have different
// authentication needs, different rate limits, and different blast radii when
// one is overloaded. Mixing them means a flood of client reads can starve the
// heartbeats that keep the cluster's leader in office.
package kvserver

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sAchin-680/raftkv/codec"
	"github.com/sAchin-680/raftkv/kvstore"
	"github.com/sAchin-680/raftkv/node"
	kvpb "github.com/sAchin-680/raftkv/proto/kvpb"
	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/transport"
)

// Config configures the client-facing server.
type Config struct {
	Node  *node.Node
	Store *kvstore.Store

	// Listen is the client address, separate from the peer address.
	Listen string

	// Peers lets a NotLeader error name where the leader can be reached, so a
	// client redirects in one hop instead of polling every node.
	Peers []transport.Peer

	// WriteTimeout bounds how long a write waits to commit before the client is
	// told it does not know the outcome.
	WriteTimeout time.Duration

	// ReadTimeout bounds a linearizable read's leadership confirmation.
	ReadTimeout time.Duration

	Logger *slog.Logger
}

func (c *Config) withDefaults() error {
	switch {
	case c.Node == nil:
		return errors.New("kvserver: Node is required")
	case c.Store == nil:
		return errors.New("kvserver: Store is required")
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// Server implements the KV and Cluster services.
type Server struct {
	kvpb.UnimplementedKVServer
	kvpb.UnimplementedClusterServer

	cfg   Config
	log   *slog.Logger
	addrs map[raft.NodeID]string

	grpc     *grpc.Server
	listener net.Listener

	closeOnce sync.Once
	wg        sync.WaitGroup
}

// Serve starts the client API and returns once it is listening.
func Serve(cfg Config) (*Server, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("kvserver: listening on %s: %w", cfg.Listen, err)
	}

	s := &Server{
		cfg:      cfg,
		log:      cfg.Logger.With("component", "kvserver"),
		addrs:    make(map[raft.NodeID]string, len(cfg.Peers)),
		listener: listener,
		grpc:     grpc.NewServer(),
	}
	for _, p := range cfg.Peers {
		s.addrs[p.ID] = p.Address
	}

	kvpb.RegisterKVServer(s.grpc, s)
	kvpb.RegisterClusterServer(s.grpc, s)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.grpc.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error("client server stopped", "error", err)
		}
	}()

	s.log.Info("client API listening", "address", listener.Addr().String())
	return s, nil
}

// Addr is the address actually bound.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Stop shuts the client API down.
//
// GracefulStop is right here, unlike in the peer transport: client RPCs are
// short unary calls that finish on their own, so waiting for them lets
// in-flight requests answer instead of failing. The peer transport's streams
// are long-lived and never end by themselves, which is why it cannot.
func (s *Server) Stop() {
	s.closeOnce.Do(func() {
		s.grpc.GracefulStop()
		s.wg.Wait()
	})
}

// ---------------------------------------------------------------------------
// Client sessions
// ---------------------------------------------------------------------------

// Register hands out a client id for deduplicating retries.
//
// The id is random rather than allocated through the log. Allocation would need
// a replicated counter, which means an extra round of consensus before a client
// can do anything — and the only property actually required is uniqueness.
// Sixty-four random bits give that with room to spare: a cluster would need
// billions of clients before a collision became plausible, and the id is
// meaningless outside the session table.
func (s *Server) Register(context.Context, *kvpb.RegisterRequest) (*kvpb.RegisterResponse, error) {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return nil, status.Errorf(codes.Internal, "generating a client id: %v", err)
	}
	id := binary.BigEndian.Uint64(buf[:])
	if id == 0 {
		// Zero means "no session"; never hand it out.
		id = 1
	}
	return &kvpb.RegisterResponse{ClientId: id}, nil
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

func (s *Server) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	clientID, seq := codec.SessionFromProto(req.GetSession())
	cmd, err := kvstore.SessionSetCommand(clientID, seq, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &kvpb.SetResponse{Index: uint64(result.Index)}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	clientID, seq := codec.SessionFromProto(req.GetSession())
	cmd, err := kvstore.SessionDeleteCommand(clientID, seq, req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := s.propose(ctx, cmd)
	if err != nil {
		return nil, err
	}

	existed := false
	if r, ok := result.Response.(kvstore.Result); ok {
		existed = r.Existed
	}
	return &kvpb.DeleteResponse{Index: uint64(result.Index), Existed: existed}, nil
}

func (s *Server) propose(ctx context.Context, cmd []byte) (node.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.WriteTimeout)
	defer cancel()

	result, err := s.cfg.Node.Propose(ctx, cmd)
	if err != nil {
		return node.Result{}, s.writeError(ctx, err)
	}
	return result, nil
}

// writeError turns a driver error into something a client can act on.
func (s *Server) writeError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, node.ErrNotLeader):
		return s.notLeader(ctx)

	case errors.Is(err, node.ErrProposalDropped):
		// Deliberately Unavailable rather than Aborted or FailedPrecondition.
		// The command may still commit under the new leader; this node simply
		// stopped being able to tell. A client that retries with the same
		// session sequence gets the original answer rather than applying twice,
		// which is exactly what sessions are for.
		return status.Error(codes.Unavailable,
			"leadership changed before this write committed; retry with the same "+
				"session sequence, which will be deduplicated if it did commit")

	case errors.Is(err, node.ErrStopped):
		return status.Error(codes.Unavailable, "this node is shutting down")

	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded,
			"the write did not commit in time; its outcome is unknown, so retry "+
				"with the same session sequence")

	default:
		return status.Errorf(codes.Internal, "write failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	// An unspecified consistency level means linearizable.
	//
	// Defaults decide what happens when nobody thought about it, so the default
	// is the safe one. A caller who genuinely wants a possibly-stale read has to
	// say so, rather than getting one because a field was left unset.
	if req.GetConsistency() == kvpb.ReadConsistency_READ_CONSISTENCY_STALE {
		value, found := s.cfg.Store.Get(req.GetKey())
		return &kvpb.GetResponse{
			Value:     value,
			Found:     found,
			ReadIndex: uint64(s.cfg.Store.AppliedIndex()),
		}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.ReadTimeout)
	defer cancel()

	var resp kvpb.GetResponse
	err := s.cfg.Node.LinearizableRead(ctx, func() error {
		value, found := s.cfg.Store.Get(req.GetKey())
		resp.Value, resp.Found = value, found
		resp.ReadIndex = uint64(s.cfg.Store.AppliedIndex())
		return nil
	})
	if err != nil {
		return nil, s.readError(ctx, err)
	}
	return &resp, nil
}

func (s *Server) readError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, node.ErrNotLeader), errors.Is(err, raft.ErrNotLeader):
		return s.notLeader(ctx)

	case errors.Is(err, raft.ErrReadIndexUnavailable):
		// Transient: the leader is waiting for its own no-op to commit. A retry
		// in a few milliseconds succeeds.
		return status.Error(codes.Unavailable,
			"this leader has not yet committed an entry of its own term; retry shortly")

	case errors.Is(err, node.ErrStopped):
		return status.Error(codes.Unavailable, "this node is shutting down")

	case errors.Is(err, context.DeadlineExceeded):
		// The leadership confirmation did not come back. Returning an error is
		// the point of the whole mechanism: this node cannot rule out another
		// leader having committed writes it has never seen, so it does not know
		// the current value and says so.
		return status.Error(codes.Unavailable,
			"could not confirm leadership with a quorum, so this read might be "+
				"stale and is refused")

	default:
		return status.Errorf(codes.Internal, "read failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

func (s *Server) Status(ctx context.Context, _ *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	st, err := s.cfg.Node.Status(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reading status: %v", err)
	}

	resp := &kvpb.StatusResponse{
		NodeId:       uint64(st.ID),
		State:        st.State.String(),
		Term:         uint64(st.Term),
		LeaderId:     uint64(st.Lead),
		CommitIndex:  uint64(st.CommitIndex),
		AppliedIndex: uint64(st.AppliedIndex),
		LastIndex:    uint64(st.LastIndex),
		Config:       codec.ConfigurationToProto(st.Config),
	}
	if len(st.Match) > 0 {
		resp.MatchIndex = make(map[uint64]uint64, len(st.Match))
		for id, match := range st.Match {
			resp.MatchIndex[uint64(id)] = uint64(match)
		}
	}
	return resp, nil
}

func (s *Server) ChangeMembership(context.Context, *kvpb.ChangeMembershipRequest) (*kvpb.ChangeMembershipResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"membership changes are not implemented yet")
}

// notLeader builds an error carrying where the leader can be reached, so a
// client redirects in one hop rather than polling every node in turn.
func (s *Server) notLeader(ctx context.Context) error {
	detail := &kvpb.NotLeaderError{}

	if st, err := s.cfg.Node.Status(ctx); err == nil && st.Lead != raft.None {
		detail.LeaderHint = uint64(st.Lead)
		// The peer address is where consensus traffic goes, not where clients
		// connect, so it is a hint about identity rather than a URL to dial.
		// A deployment with a predictable naming scheme can derive the client
		// address from it; one without has to look the node up.
		detail.LeaderAddress = s.addrs[st.Lead]
	}

	st := status.New(codes.FailedPrecondition, "this node is not the leader")
	withDetail, err := st.WithDetails(detail)
	if err != nil {
		return st.Err()
	}
	return withDetail.Err()
}
