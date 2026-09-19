// Command raftd runs one member of a raftkv cluster.
//
//	raftd --id=1 --listen=:9001 --data=./data/1 \
//	      --peers=1@localhost:9001,2@localhost:9002,3@localhost:9003
//
// Peers are named by ID and address together, and every node is given the whole
// list including itself. That is deliberate: the ID is the identity consensus
// uses for the lifetime of the cluster, while the address is where that identity
// currently answers. A deployment that conflates the two breaks the first time a
// process is rescheduled onto a different IP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sAchin-680/raftkv/internal/buildinfo"
	"github.com/sAchin-680/raftkv/kvserver"
	"github.com/sAchin-680/raftkv/kvstore"
	"github.com/sAchin-680/raftkv/node"
	"github.com/sAchin-680/raftkv/raft"
	"github.com/sAchin-680/raftkv/storage"
	"github.com/sAchin-680/raftkv/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftd:", err)
		os.Exit(1)
	}
}

type options struct {
	id            uint64
	listen        string
	clientListen  string
	dataDir       string
	peers         string
	tickInterval  time.Duration
	electionTick  int
	heartbeatTick int
	preVote       bool
	unsafeNoSync  bool
	logLevel      string
	statusEvery   time.Duration
	version       bool
}

func run() error {
	var o options
	flag.Uint64Var(&o.id, "id", 0, "this node's identity (required, non-zero)")
	flag.StringVar(&o.listen, "listen", ":9001", "address to serve peer traffic on")
	flag.StringVar(&o.clientListen, "client-listen", "",
		"address to serve the client API on (empty disables it)\n"+
			"\tKept separate from --listen so a flood of client traffic cannot\n"+
			"\tstarve the heartbeats that keep the leader in office.")
	flag.StringVar(&o.dataDir, "data", "", "directory for durable state (required)")
	flag.StringVar(&o.peers, "peers", "", "every node as id@address, comma separated (required)")
	flag.DurationVar(&o.tickInterval, "tick", 100*time.Millisecond, "real time per logical tick")
	flag.IntVar(&o.electionTick, "election-ticks", 10, "ticks without a leader before campaigning")
	flag.IntVar(&o.heartbeatTick, "heartbeat-ticks", 2, "ticks between a leader's heartbeats")
	flag.BoolVar(&o.preVote, "pre-vote", true, "run a straw poll before campaigning for real")
	flag.BoolVar(&o.unsafeNoSync, "unsafe-no-fsync", false,
		"skip fsync on every write\n"+
			"\tAbout 190x faster and unsafe: Raft's correctness assumes a vote is\n"+
			"\tdurable before the response admitting to it is sent. For benchmarks only.")
	flag.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	flag.DurationVar(&o.statusEvery, "status-every", 0, "log cluster status on this interval (0 disables)")
	flag.BoolVar(&o.version, "version", false, "print the version and exit")
	flag.Parse()

	if o.version {
		fmt.Println("raftd", buildinfo.String())
		return nil
	}

	logger, err := newLogger(o.logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	peers, err := parsePeers(o.peers)
	if err != nil {
		return err
	}
	if err := validate(&o, peers); err != nil {
		return err
	}

	id := raft.NodeID(o.id)
	log := logger.With("node", o.id)

	dbPath := filepath.Join(o.dataDir, "raft.db")
	db, err := storage.Open(dbPath, storage.Options{
		NoSync:  o.unsafeNoSync,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if o.unsafeNoSync {
		log.Warn("fsync disabled: a crash can lose a vote and elect two leaders in one term")
	}

	tr, err := transport.NewGRPC(transport.GRPCConfig{
		ID: id, Listen: o.listen, Peers: peers, Logger: logger,
	})
	if err != nil {
		return err
	}

	store := kvstore.New()
	n, err := node.Start(node.Config{
		ID:            id,
		Peers:         peers,
		Storage:       db,
		Transport:     tr,
		StateMachine:  store,
		TickInterval:  o.tickInterval,
		ElectionTick:  o.electionTick,
		HeartbeatTick: o.heartbeatTick,
		PreVote:       o.preVote,
		Logger:        logger,
	})
	if err != nil {
		return err
	}

	var clientAPI *kvserver.Server
	if o.clientListen != "" {
		clientAPI, err = kvserver.Serve(kvserver.Config{
			Node: n, Store: store, Listen: o.clientListen, Peers: peers, Logger: logger,
		})
		if err != nil {
			return err
		}
	}

	log.Info("raftd running",
		"version", buildinfo.Version,
		"listen", tr.Addr(),
		"client_listen", o.clientListen,
		"data", dbPath,
		"peers", len(peers),
		"election_timeout", time.Duration(o.electionTick)*o.tickInterval)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if o.statusEvery > 0 {
		go logStatus(ctx, n, log, o.statusEvery)
	}

	select {
	case <-ctx.Done():
		log.Info("signal received, shutting down")
	case <-n.Done():
		log.Warn("node stopped on its own")
	}

	// Order matters, and it is the reverse of startup. Clients are turned away
	// first so none are left waiting on a node that is about to stop. The node's
	// run loop reads from the transport, so stopping the node before the
	// transport lets it exit on its own signal rather than on a closed channel.
	if clientAPI != nil {
		clientAPI.Stop()
	}
	n.Stop()
	if err := tr.Close(); err != nil {
		log.Error("closing transport", "error", err)
	}
	log.Info("stopped")
	return nil
}

func validate(o *options, peers []transport.Peer) error {
	if o.id == 0 {
		return errors.New("--id is required and must not be zero")
	}
	if o.dataDir == "" {
		return errors.New("--data is required")
	}
	if len(peers) == 0 {
		return errors.New("--peers is required")
	}

	found := false
	for _, p := range peers {
		if uint64(p.ID) == o.id {
			found = true
		}
	}
	if !found {
		// A node missing from its own peer list would campaign for a cluster it
		// is not a member of, and never win a vote it is not entitled to.
		return fmt.Errorf("--peers does not include this node's own id %d", o.id)
	}

	if o.heartbeatTick >= o.electionTick {
		return fmt.Errorf(
			"--heartbeat-ticks (%d) must be well below --election-ticks (%d), or "+
				"followers will campaign against a healthy leader",
			o.heartbeatTick, o.electionTick)
	}
	if len(peers)%2 == 0 {
		slog.Warn("even cluster size tolerates no more failures than one node smaller",
			"size", len(peers), "tolerates", (len(peers)-1)/2)
	}
	return nil
}

// parsePeers reads the id@address form.
func parsePeers(s string) ([]transport.Peer, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}

	var peers []transport.Peer
	seen := map[raft.NodeID]string{}

	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "@")
		if !ok {
			return nil, fmt.Errorf("peer %q is not in id@address form", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q has an unparseable id: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("peer %q has id 0, which is reserved for 'no node'", part)
		}
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return nil, fmt.Errorf("peer %q has no address", part)
		}
		if prev, dup := seen[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("id %d is given twice, as %s and %s", id, prev, addr)
		}
		seen[raft.NodeID(id)] = addr
		peers = append(peers, transport.Peer{ID: raft.NodeID(id), Address: addr})
	}
	return peers, nil
}

func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("unrecognized --log-level %q", level)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

// logStatus prints what the node believes, on an interval. Useful while
// watching a cluster by hand, and a stand-in until Prometheus metrics land.
func logStatus(ctx context.Context, n *node.Node, log *slog.Logger, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statusCtx, cancel := context.WithTimeout(ctx, time.Second)
			s, err := n.Status(statusCtx)
			cancel()
			if err != nil {
				continue
			}

			attrs := []any{
				"state", s.State.String(),
				"term", uint64(s.Term),
				"leader", uint64(s.Lead),
				"commit", uint64(s.CommitIndex),
				"applied", uint64(s.AppliedIndex),
			}
			// Replication lag is only knowable at the leader, and it is the
			// number an operator actually wants.
			if s.IsLeader() {
				lag := map[string]uint64{}
				for id, match := range s.Match {
					if s.CommitIndex > match {
						lag[strconv.FormatUint(uint64(id), 10)] = uint64(s.CommitIndex - match)
					}
				}
				if len(lag) > 0 {
					attrs = append(attrs, "lag", lag)
				}
			}
			log.Info("status", attrs...)
		}
	}
}
