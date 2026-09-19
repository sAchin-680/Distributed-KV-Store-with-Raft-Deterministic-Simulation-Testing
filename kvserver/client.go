package kvserver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	kvpb "github.com/sAchin-680/raftkv/proto/kvpb"
)

// Client talks to a cluster, not to a node.
//
// Two things it handles that a raw gRPC stub would not.
//
// It follows leadership. A write sent to a follower is refused with a hint
// about who leads, and the client retries there rather than surfacing an error
// the caller can do nothing useful with. Leadership moves on its own schedule;
// a client that treats "not the leader" as a failure is broken by an ordinary
// election.
//
// And it carries a session. Every write gets a sequence number from a counter
// that only moves forward, and a retry reuses the number of the attempt it is
// retrying. That is what makes a retry safe: Raft applies a command at least
// once, so without it a retried write can be applied twice, and a history with
// a duplicated write is not linearizable even though the map looks fine.
type Client struct {
	endpoints []string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	// leader is the endpoint that answered last, tried first next time.
	leader string

	clientID atomic.Uint64
	sequence atomic.Uint64

	// MaxAttempts bounds redirects and retries for one call.
	MaxAttempts int
}

// NewClient connects lazily to the given client-API endpoints.
func NewClient(endpoints ...string) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("kvserver: at least one endpoint is required")
	}
	return &Client{
		endpoints:   endpoints,
		conns:       make(map[string]*grpc.ClientConn, len(endpoints)),
		MaxAttempts: 2*len(endpoints) + 2,
	}, nil
}

// Close releases every connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, addr)
	}
	return nil
}

func (c *Client) connect(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("kvserver: connecting to %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// order returns endpoints with the last known leader first.
func (c *Client) order() []string {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	if leader == "" {
		return c.endpoints
	}
	out := make([]string, 0, len(c.endpoints))
	out = append(out, leader)
	for _, e := range c.endpoints {
		if e != leader {
			out = append(out, e)
		}
	}
	return out
}

func (c *Client) rememberLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

// Register obtains a client id, enabling deduplication of retried writes.
//
// Optional. Without it writes still work, but a retry that the network made
// look like a failure can be applied twice.
func (c *Client) Register(ctx context.Context) (uint64, error) {
	var id uint64
	err := c.call(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := kvpb.NewKVClient(conn).Register(ctx, &kvpb.RegisterRequest{})
		if err != nil {
			return err
		}
		id = resp.GetClientId()
		return nil
	})
	if err != nil {
		return 0, err
	}
	c.clientID.Store(id)
	return id, nil
}

// session returns the session for one logical request. Called once per
// operation, not once per attempt: every retry of the same operation must carry
// the same sequence number, or it is a different request as far as the state
// machine is concerned and gets applied again.
func (c *Client) session() *kvpb.Session {
	id := c.clientID.Load()
	if id == 0 {
		return nil
	}
	return &kvpb.Session{ClientId: id, SequenceNum: c.sequence.Add(1)}
}

// Set writes a key.
func (c *Client) Set(ctx context.Context, key, value []byte) (uint64, error) {
	session := c.session()
	var index uint64
	err := c.call(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := kvpb.NewKVClient(conn).Set(ctx, &kvpb.SetRequest{
			Session: session, Key: key, Value: value,
		})
		if err != nil {
			return err
		}
		index = resp.GetIndex()
		return nil
	})
	return index, err
}

// Delete removes a key, reporting whether it was there.
func (c *Client) Delete(ctx context.Context, key []byte) (bool, error) {
	session := c.session()
	var existed bool
	err := c.call(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := kvpb.NewKVClient(conn).Delete(ctx, &kvpb.DeleteRequest{
			Session: session, Key: key,
		})
		if err != nil {
			return err
		}
		existed = resp.GetExisted()
		return nil
	})
	return existed, err
}

// Get reads a key linearizably.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	return c.get(ctx, key, kvpb.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE)
}

// GetStale reads whatever the contacted node has applied, without confirming
// leadership. Fast, and may return a value that is arbitrarily out of date.
func (c *Client) GetStale(ctx context.Context, key []byte) ([]byte, bool, error) {
	return c.get(ctx, key, kvpb.ReadConsistency_READ_CONSISTENCY_STALE)
}

func (c *Client) get(ctx context.Context, key []byte, level kvpb.ReadConsistency) ([]byte, bool, error) {
	var (
		value []byte
		found bool
	)
	err := c.call(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := kvpb.NewKVClient(conn).Get(ctx, &kvpb.GetRequest{
			Key: key, Consistency: level,
		})
		if err != nil {
			return err
		}
		value, found = resp.GetValue(), resp.GetFound()
		return nil
	})
	return value, found, err
}

// Status reads one node's view of the cluster, without redirecting: the point
// of asking a particular node is to hear what that node believes.
func (c *Client) Status(ctx context.Context, endpoint string) (*kvpb.StatusResponse, error) {
	conn, err := c.connect(endpoint)
	if err != nil {
		return nil, err
	}
	return kvpb.NewClusterClient(conn).Status(ctx, &kvpb.StatusRequest{})
}

// Endpoints returns the configured endpoints.
func (c *Client) Endpoints() []string { return c.endpoints }

// call runs fn against endpoints until one answers, following leader hints.
func (c *Client) call(ctx context.Context, fn func(context.Context, *grpc.ClientConn) error) error {
	var lastErr error

	for attempt := 0; attempt < c.MaxAttempts; attempt++ {
		for _, addr := range c.order() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			conn, err := c.connect(addr)
			if err != nil {
				lastErr = err
				continue
			}

			err = fn(ctx, conn)
			if err == nil {
				c.rememberLeader(addr)
				return nil
			}
			lastErr = err

			if !retryable(err) {
				return err
			}
		}

		// Every endpoint refused. Usually an election is in progress, which
		// resolves within an election timeout.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("kvserver: no endpoint answered after %d attempts: %w",
		c.MaxAttempts, lastErr)
}

// retryable reports whether another endpoint is worth trying.
func retryable(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.FailedPrecondition:
		// "Not the leader" — the canonical redirect.
		return true
	case codes.Unavailable:
		// The node is down, shutting down, or lost leadership mid-write.
		return true
	default:
		// InvalidArgument, DeadlineExceeded and the rest are the caller's
		// problem, and trying a different node would only repeat them.
		return false
	}
}

// LeaderHint extracts the redirect from a NotLeader error, if there is one.
func LeaderHint(err error) (*kvpb.NotLeaderError, bool) {
	s, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	for _, d := range s.Details() {
		if hint, ok := d.(*kvpb.NotLeaderError); ok {
			return hint, true
		}
	}
	return nil, false
}
