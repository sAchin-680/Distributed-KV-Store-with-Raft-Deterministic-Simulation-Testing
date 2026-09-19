package node_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sAchin-680/raftkv/kvstore"
	"github.com/sAchin-680/raftkv/node"
	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/storage"
	"github.com/sAchin-680/raftkv/transport"
)

// A real cluster: separate storage, separate gRPC transports, real sockets,
// real goroutines, real time.
//
// Everything up to here has been verified against a *model* of the network and
// the disk. The simulator can prove the algorithm correct under any interleaving
// it can generate, and says nothing about whether the gRPC wiring, the bbolt
// usage, or the driver's own concurrency are right. That gap is what these tests
// and the later chaos work exist to probe.

type cluster struct {
	t     *testing.T
	nodes map[raft.NodeID]*node.Node
	kv    map[raft.NodeID]*kvstore.Store
	ids   []raft.NodeID

	// Kept so a node can be stopped and started again on the same durable
	// state, which is what a crash and restart actually is.
	peers   []transport.Peer
	addrs   map[raft.NodeID]string
	dbPaths map[raft.NodeID]string

	// dbs holds the live handle per node. Restarting has to close the old one
	// before reopening: bbolt takes an exclusive file lock, so a stale handle
	// makes the restart fail with a lock timeout. That lock is deliberate — it
	// is what stops a terminating pod and its replacement writing the same
	// volume at once — so the test works with it rather than around it.
	dbs        map[raft.NodeID]*storage.Bolt
	transports map[raft.NodeID]*transport.GRPC
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// reservePorts hands back addresses nothing is listening on. Every node needs
// every other node's address before any of them start.
func reservePorts(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving port: %v", err)
		}
		addrs[i] = l.Addr().String()
		_ = l.Close()
	}
	return addrs
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()

	addrs := reservePorts(t, size)
	peers := make([]transport.Peer, size)
	for i := range peers {
		peers[i] = transport.Peer{ID: raft.NodeID(i + 1), Address: addrs[i]}
	}

	dir := t.TempDir()
	c := &cluster{
		t:          t,
		nodes:      make(map[raft.NodeID]*node.Node, size),
		kv:         make(map[raft.NodeID]*kvstore.Store, size),
		peers:      peers,
		addrs:      make(map[raft.NodeID]string, size),
		dbPaths:    make(map[raft.NodeID]string, size),
		dbs:        make(map[raft.NodeID]*storage.Bolt, size),
		transports: make(map[raft.NodeID]*transport.GRPC, size),
	}

	for i, p := range peers {
		c.addrs[p.ID] = addrs[i]
		c.dbPaths[p.ID] = filepath.Join(dir, fmt.Sprintf("node-%d.db", p.ID))

		db, err := storage.Open(
			c.dbPaths[p.ID],
			// fsync costs about 8ms per append, so a test that did a hundred
			// writes would spend a second in the kernel. Durability itself is
			// covered by the storage package's own tests.
			storage.Options{NoSync: true, Timeout: time.Second},
		)
		if err != nil {
			t.Fatalf("opening storage for node %d: %v", p.ID, err)
		}
		c.dbs[p.ID] = db

		tr, err := transport.NewGRPC(transport.GRPCConfig{
			ID: p.ID, Listen: addrs[i], Peers: peers, Logger: quiet(),
		})
		if err != nil {
			t.Fatalf("starting transport for node %d: %v", p.ID, err)
		}

		sm := kvstore.New()
		n, err := node.Start(node.Config{
			ID: p.ID, Peers: peers, Storage: db, Transport: tr, StateMachine: sm,
			// 30ms ticks give a 300–600ms election timeout: slow enough that a
			// loaded CI machine does not trigger spurious elections, fast enough
			// that the test finishes.
			TickInterval:  30 * time.Millisecond,
			ElectionTick:  10,
			HeartbeatTick: 2,
			PreVote:       true,
			Logger:        quiet(),
		})
		if err != nil {
			t.Fatalf("starting node %d: %v", p.ID, err)
		}

		c.ids = append(c.ids, p.ID)
		c.nodes[p.ID] = n
		c.kv[p.ID] = sm
		c.transports[p.ID] = tr
	}

	// Shut down in the reverse of startup: node, then transport, then storage.
	// The run loop reads from the transport, so closing that first would make it
	// exit on a closed channel rather than on its own stop signal.
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.shutdown(id)
		}
	})
	return c
}

// shutdown stops one node and releases everything it holds.
func (c *cluster) shutdown(id raft.NodeID) {
	if n, ok := c.nodes[id]; ok && n != nil {
		n.Stop()
	}
	if tr, ok := c.transports[id]; ok && tr != nil {
		_ = tr.Close()
		delete(c.transports, id)
	}
	if db, ok := c.dbs[id]; ok && db != nil {
		_ = db.Close()
		delete(c.dbs, id)
	}
}

// restart stops a node and brings it back on the same durable state, keeping
// its identity and address. The state machine is rebuilt from an empty map,
// because a KV map lives in memory and dies with the process — recovering it is
// the log's job.
func (c *cluster) restart(id raft.NodeID) {
	c.t.Helper()
	c.shutdown(id)

	db, err := storage.Open(c.dbPaths[id], storage.Options{NoSync: true, Timeout: 5 * time.Second})
	if err != nil {
		c.t.Fatalf("reopening storage for node %d: %v", id, err)
	}
	c.dbs[id] = db

	tr, err := transport.NewGRPC(transport.GRPCConfig{
		ID: id, Listen: c.addrs[id], Peers: c.peers, Logger: quiet(),
	})
	if err != nil {
		c.t.Fatalf("restarting transport for node %d: %v", id, err)
	}
	c.transports[id] = tr

	sm := kvstore.New()
	n, err := node.Start(node.Config{
		ID: id, Peers: c.peers, Storage: db, Transport: tr, StateMachine: sm,
		TickInterval: 30 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 2,
		PreVote: true, Logger: quiet(),
	})
	if err != nil {
		c.t.Fatalf("restarting node %d: %v", id, err)
	}

	c.nodes[id] = n
	c.kv[id] = sm
}

func (c *cluster) status(id raft.NodeID) node.Status {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.nodes[id].Status(ctx)
	if err != nil {
		c.t.Fatalf("status of node %d: %v", id, err)
	}
	return s
}

// awaitLeader polls until exactly one node claims leadership.
func (c *cluster) awaitLeader(timeout time.Duration) (raft.NodeID, time.Duration) {
	c.t.Helper()
	began := time.Now()
	deadline := time.After(timeout)

	for {
		var leaders []raft.NodeID
		for _, id := range c.ids {
			if c.status(id).IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0], time.Since(began)
		}
		if len(leaders) > 1 {
			c.t.Fatalf("two nodes claim leadership: %v — election safety is broken", leaders)
		}

		select {
		case <-deadline:
			c.t.Fatalf("no leader within %s\n%s", timeout, c.dump())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// awaitLead polls until a node recognizes the given leader.
func (c *cluster) awaitLead(id, want raft.NodeID, timeout time.Duration) {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		if got := c.status(id).Lead; got == want {
			return
		}
		select {
		case <-deadline:
			c.t.Errorf("node %d never recognized leader %d (sees %d) within %s",
				id, want, c.status(id).Lead, timeout)
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *cluster) dump() string {
	s := "cluster:\n"
	for _, id := range c.ids {
		st := c.status(id)
		s += fmt.Sprintf("  node %d %s term=%d lead=%d commit=%d applied=%d last=%d\n",
			st.ID, st.State, st.Term, st.Lead, st.CommitIndex, st.AppliedIndex, st.LastIndex)
	}
	return s
}

// awaitConvergence waits until every node's state machine holds the same keys.
func (c *cluster) awaitConvergence(want int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		converged := true
		for _, id := range c.ids {
			if c.kv[id].Len() != want {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		select {
		case <-deadline:
			counts := map[raft.NodeID]int{}
			for _, id := range c.ids {
				counts[id] = c.kv[id].Len()
			}
			c.t.Fatalf("state machines did not converge on %d keys within %s: %v\n%s",
				want, timeout, counts, c.dump())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *cluster) set(id raft.NodeID, key, value string) error {
	c.t.Helper()
	cmd, err := kvstore.SetCommand([]byte(key), []byte(value))
	if err != nil {
		c.t.Fatalf("encoding command: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.nodes[id].Propose(ctx, cmd)
	return err
}

// ---------------------------------------------------------------------------

func TestRealClusterElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)

	leader, took := c.awaitLeader(15 * time.Second)
	t.Logf("elected node %d in %s", leader, took.Round(time.Millisecond))

	// Polled, not asserted on the instant: a follower learns who won on the
	// next heartbeat, so checking the moment the leader appears is a race with
	// the network rather than a test of anything.
	for _, id := range c.ids {
		c.awaitLead(id, leader, 5*time.Second)
	}
}

func TestRealClusterReplicatesWrites(t *testing.T) {
	c := newCluster(t, 3)
	leader, _ := c.awaitLeader(15 * time.Second)

	const writes = 25
	for i := range writes {
		if err := c.set(leader, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	c.awaitConvergence(writes, 10*time.Second)

	// Every node must hold identical data, not merely the same count.
	want := c.kv[leader].Keys()
	for _, id := range c.ids {
		got := c.kv[id].Keys()
		if len(got) != len(want) {
			t.Fatalf("node %d has %d keys, want %d", id, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("node %d differs at key %d: %q vs %q", id, i, got[i], want[i])
			}
		}
		for _, k := range want {
			v, ok := c.kv[id].Get([]byte(k))
			if !ok {
				t.Fatalf("node %d is missing %q", id, k)
			}
			leaderValue, _ := c.kv[leader].Get([]byte(k))
			if string(v) != string(leaderValue) {
				t.Errorf("node %d has %q=%q, leader has %q", id, k, v, leaderValue)
			}
		}
	}
}

func TestWriteToFollowerIsRefused(t *testing.T) {
	c := newCluster(t, 3)
	leader, _ := c.awaitLeader(15 * time.Second)

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if err := c.set(id, "k", "v"); err == nil {
			t.Errorf("node %d accepted a write while not leader", id)
		}

		// The caller needs somewhere to go, not just a refusal. This is polled
		// rather than asserted outright: a follower legitimately shows no leader
		// for the moment between stepping into a term and hearing its first
		// heartbeat, and asserting on the instant makes the test a race.
		c.awaitLead(id, leader, 5*time.Second)
	}
}

// Killing the leader must produce a new one, and the data written before the
// failure must still be there afterwards.
func TestLeaderFailoverKeepsCommittedData(t *testing.T) {
	c := newCluster(t, 3)
	oldLeader, _ := c.awaitLeader(15 * time.Second)

	for i := range 5 {
		if err := c.set(oldLeader, fmt.Sprintf("before-%d", i), "x"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	c.awaitConvergence(5, 10*time.Second)

	c.shutdown(oldLeader)

	var survivors []raft.NodeID
	for _, id := range c.ids {
		if id != oldLeader {
			survivors = append(survivors, id)
		}
	}

	// Watch only the survivors; the stopped node answers nothing.
	began := time.Now()
	var newLeader raft.NodeID
	deadline := time.After(20 * time.Second)
	for newLeader == 0 {
		for _, id := range survivors {
			if c.status(id).IsLeader() {
				newLeader = id
			}
		}
		select {
		case <-deadline:
			t.Fatalf("no new leader within 20s\n%s", c.dump())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Logf("failover: node %d took over in %s", newLeader, time.Since(began).Round(time.Millisecond))

	// Everything committed under the old leader must have survived.
	for i := range 5 {
		if _, ok := c.kv[newLeader].Get(fmt.Appendf(nil, "before-%d", i)); !ok {
			t.Errorf("committed key before-%d was lost across failover", i)
		}
	}

	// And the cluster must still accept writes.
	if err := c.set(newLeader, "after", "y"); err != nil {
		t.Fatalf("write to the new leader: %v", err)
	}
	if _, ok := c.kv[newLeader].Get([]byte("after")); !ok {
		t.Error("the write to the new leader was not applied")
	}
}

func TestSingleNodeClusterWorks(t *testing.T) {
	c := newCluster(t, 1)
	leader, took := c.awaitLeader(10 * time.Second)
	t.Logf("single node elected itself in %s", took.Round(time.Millisecond))

	if err := c.set(leader, "solo", "value"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if v, ok := c.kv[leader].Get([]byte("solo")); !ok || string(v) != "value" {
		t.Errorf("got %q, %v; want \"value\", true", v, ok)
	}
}

// The leader's view of how far each follower has caught up is what replication
// lag and the quorum alert are built on, so it has to be populated.
func TestLeaderReportsFollowerProgress(t *testing.T) {
	c := newCluster(t, 3)
	leader, _ := c.awaitLeader(15 * time.Second)

	for i := range 5 {
		if err := c.set(leader, fmt.Sprintf("k-%d", i), "v"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	c.awaitConvergence(5, 10*time.Second)

	s := c.status(leader)
	if len(s.Match) != len(c.ids) {
		t.Fatalf("leader tracks %d peers, want %d", len(s.Match), len(c.ids))
	}
	for id, match := range s.Match {
		if match == 0 {
			t.Errorf("leader shows node %d matched at 0 after convergence", id)
		}
	}

	// A follower is not tracking anyone, and should not pretend to.
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if len(c.status(id).Match) != 0 {
			t.Errorf("follower %d reports peer progress it cannot know", id)
		}
	}
}

func TestStopIsIdempotent(t *testing.T) {
	c := newCluster(t, 1)
	c.awaitLeader(10 * time.Second)

	c.nodes[1].Stop()
	c.nodes[1].Stop() // must not hang or panic

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.nodes[1].Propose(ctx, []byte("x")); err == nil {
		t.Error("a stopped node accepted a proposal")
	}
}

// Crash, restart, catch up — the reason durable storage exists.
//
// The restarted node comes back with an empty state machine and has to recover
// everything from its log: the entries it had before the crash, plus everything
// the cluster committed while it was gone.
func TestNodeRecoversFromDiskAfterRestart(t *testing.T) {
	c := newCluster(t, 3)
	leader, _ := c.awaitLeader(15 * time.Second)

	var victim raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}

	for i := range 5 {
		if err := c.set(leader, fmt.Sprintf("before-%d", i), "x"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	c.awaitConvergence(5, 10*time.Second)

	// Take it down and keep writing without it. Two of three is still a quorum.
	c.nodes[victim].Stop()
	for i := range 5 {
		if err := c.set(leader, fmt.Sprintf("during-%d", i), "y"); err != nil {
			t.Fatalf("write while a node was down: %v", err)
		}
	}

	c.restart(victim)

	// It must recover the entries it had, and the ones it missed.
	deadline := time.After(20 * time.Second)
	for c.kv[victim].Len() < 10 {
		select {
		case <-deadline:
			t.Fatalf("restarted node holds %d of 10 keys\n%s", c.kv[victim].Len(), c.dump())
		case <-time.After(20 * time.Millisecond):
		}
	}

	for i := range 5 {
		if _, ok := c.kv[victim].Get(fmt.Appendf(nil, "before-%d", i)); !ok {
			t.Errorf("before-%d was lost across the restart", i)
		}
		if _, ok := c.kv[victim].Get(fmt.Appendf(nil, "during-%d", i)); !ok {
			t.Errorf("during-%d, written while it was down, was never caught up", i)
		}
	}

	// And its durable term and vote survived, which is what stops it voting
	// twice in one term.
	if s := c.status(victim); s.Term == 0 {
		t.Error("the restarted node came back at term 0; its persisted term was lost")
	}
}
