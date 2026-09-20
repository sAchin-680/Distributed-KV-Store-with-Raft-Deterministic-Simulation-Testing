// Package metrics exposes what an operator needs to know about a node.
//
// The set is deliberately small. Every metric here answers a question someone
// would actually ask during an incident: who is the leader, is there one at
// all, how far behind is each follower, and is the cluster electing rather than
// working. Metrics nobody would page on are noise that makes the useful ones
// harder to find.
package metrics

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sAchin-680/raftkv/node"
	"github.com/sAchin-680/raftkv/raft"
)

const namespace = "raftkv"

// Collector reports a node's consensus state to Prometheus.
//
// Implemented as a Collector that asks the node on each scrape, rather than a
// set of gauges updated from a background goroutine. Two reasons: the values
// are always current as of the scrape rather than as of whenever the goroutine
// last ran, and there is no extra goroutine reaching into the node between
// scrapes. The node's run loop owns its state and answers a Status request like
// any other caller.
type Collector struct {
	node    *node.Node
	timeout time.Duration
	log     *slog.Logger

	// Counters are kept here rather than derived on each scrape, because a
	// count of things that have happened cannot be read off current state.
	mu             sync.Mutex
	lastTerm       raft.Term
	lastLeader     raft.NodeID
	elections      float64
	leaderChanges  float64
	scrapeFailures float64

	// Descriptors, built once.
	up              *prometheus.Desc
	term            *prometheus.Desc
	state           *prometheus.Desc
	isLeader        *prometheus.Desc
	hasLeader       *prometheus.Desc
	commitIndex     *prometheus.Desc
	appliedIndex    *prometheus.Desc
	lastIndex       *prometheus.Desc
	replicationLag  *prometheus.Desc
	peerMatchIndex  *prometheus.Desc
	voters          *prometheus.Desc
	jointConfig     *prometheus.Desc
	electionsTotal  *prometheus.Desc
	leaderChanges_  *prometheus.Desc
	scrapeFailures_ *prometheus.Desc
}

// NewCollector returns a collector for one node.
func NewCollector(n *node.Node, logger *slog.Logger) *Collector {
	if logger == nil {
		logger = slog.Default()
	}
	id := []string{"node"}

	return &Collector{
		node:    n,
		timeout: 2 * time.Second,
		log:     logger.With("component", "metrics"),

		up: prometheus.NewDesc(namespace+"_up",
			"1 when the node answered this scrape.", id, nil),
		term: prometheus.NewDesc(namespace+"_raft_term",
			"The Raft term this node currently believes it is in.", id, nil),
		state: prometheus.NewDesc(namespace+"_raft_state",
			"1 for the state this node is in, 0 for the others.",
			[]string{"node", "state"}, nil),
		isLeader: prometheus.NewDesc(namespace+"_raft_is_leader",
			"1 if this node believes it is the leader. Summed across the cluster "+
				"this is the single most useful alerting signal: 0 means no leader "+
				"and therefore no progress, and more than 1 for longer than an "+
				"election timeout means something is badly wrong.", id, nil),
		hasLeader: prometheus.NewDesc(namespace+"_raft_has_leader",
			"1 if this node knows of a leader for its current term.", id, nil),
		commitIndex: prometheus.NewDesc(namespace+"_raft_commit_index",
			"The highest log index known to be committed.", id, nil),
		appliedIndex: prometheus.NewDesc(namespace+"_raft_applied_index",
			"The highest log index handed to the state machine. Always at or "+
				"below the commit index; a persistent gap means applying is the "+
				"bottleneck.", id, nil),
		lastIndex: prometheus.NewDesc(namespace+"_raft_last_index",
			"The last index in this node's log, committed or not.", id, nil),
		replicationLag: prometheus.NewDesc(namespace+"_raft_replication_lag",
			"How many committed entries a peer is behind, as the leader sees it. "+
				"Leader only, and the number to watch before a rolling upgrade: "+
				"restarting a node that is already behind is how a rollout loses "+
				"quorum.", []string{"node", "peer"}, nil),
		peerMatchIndex: prometheus.NewDesc(namespace+"_raft_peer_match_index",
			"The highest index a peer has confirmed, as the leader sees it.",
			[]string{"node", "peer"}, nil),
		voters: prometheus.NewDesc(namespace+"_cluster_voters",
			"How many nodes count toward a majority.", id, nil),
		jointConfig: prometheus.NewDesc(namespace+"_cluster_joint_configuration",
			"1 while a membership change is in flight. A cluster stuck here needs "+
				"majorities of two configurations for everything and is less "+
				"available than either.", id, nil),
		electionsTotal: prometheus.NewDesc(namespace+"_raft_elections_total",
			"How many times this node has observed the term advance. A rising "+
				"rate means the cluster is electing instead of working.", id, nil),
		leaderChanges_: prometheus.NewDesc(namespace+"_raft_leader_changes_total",
			"How many times the leader has changed, as this node saw it.", id, nil),
		scrapeFailures_: prometheus.NewDesc(namespace+"_scrape_failures_total",
			"How many scrapes the node failed to answer.", id, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	id := strconv.FormatUint(uint64(c.node.ID()), 10)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	status, err := c.node.Status(ctx)
	if err != nil {
		// A node that cannot answer is itself the signal. Reporting up=0 rather
		// than reporting nothing is what lets an alert distinguish "this node
		// is wedged" from "Prometheus lost the target".
		c.mu.Lock()
		c.scrapeFailures++
		failures := c.scrapeFailures
		c.mu.Unlock()

		ch <- gauge(c.up, 0, id)
		ch <- counter(c.scrapeFailures_, failures, id)
		c.log.Warn("scrape failed", "error", err)
		return
	}

	c.observe(status)

	ch <- gauge(c.up, 1, id)
	ch <- gauge(c.term, float64(status.Term), id)
	ch <- gauge(c.commitIndex, float64(status.CommitIndex), id)
	ch <- gauge(c.appliedIndex, float64(status.AppliedIndex), id)
	ch <- gauge(c.lastIndex, float64(status.LastIndex), id)
	ch <- gauge(c.isLeader, boolToFloat(status.IsLeader()), id)
	ch <- gauge(c.hasLeader, boolToFloat(status.Lead != raft.None), id)
	ch <- gauge(c.voters, float64(len(status.Config.Voters)), id)
	ch <- gauge(c.jointConfig, boolToFloat(status.Config.IsJoint()), id)

	// One series per state rather than a single number, so a dashboard can
	// stack them and a query does not have to know that 2 means candidate.
	for _, s := range []raft.State{raft.Follower, raft.PreCandidate, raft.Candidate, raft.Leader} {
		ch <- gauge(c.state, boolToFloat(status.State == s), id, s.String())
	}

	// Replication lag is only knowable at the leader. Followers report nothing
	// rather than zero, because zero would look like "fully caught up".
	//
	// The leader tracks its own progress too, since its own log counts toward
	// the majority that commits an entry. That entry is skipped here: a node's
	// lag behind itself is zero by definition, and publishing it puts a line on
	// the dashboard that looks like a perfectly healthy follower but is not a
	// follower at all — the exact confusion the paragraph above avoids.
	for peer, match := range status.Match {
		if peer == status.ID {
			continue
		}
		peerID := strconv.FormatUint(uint64(peer), 10)
		lag := float64(0)
		if status.CommitIndex > match {
			lag = float64(status.CommitIndex - match)
		}
		ch <- gauge(c.replicationLag, lag, id, peerID)
		ch <- gauge(c.peerMatchIndex, float64(match), id, peerID)
	}

	c.mu.Lock()
	elections, changes, failures := c.elections, c.leaderChanges, c.scrapeFailures
	c.mu.Unlock()

	ch <- counter(c.electionsTotal, elections, id)
	ch <- counter(c.leaderChanges_, changes, id)
	ch <- counter(c.scrapeFailures_, failures, id)
}

// observe updates the counters that cannot be read off current state.
func (c *Collector) observe(status node.Status) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if status.Term > c.lastTerm {
		// Counts term advances rather than campaigns. A node cannot see
		// elections it did not take part in, but it does see every term that
		// won one, which is the number an operator actually cares about.
		c.elections += float64(status.Term - c.lastTerm)
		c.lastTerm = status.Term
	}
	if status.Lead != raft.None && status.Lead != c.lastLeader {
		if c.lastLeader != raft.None {
			c.leaderChanges++
		}
		c.lastLeader = status.Lead
	}
}

func gauge(d *prometheus.Desc, v float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

func counter(d *prometheus.Desc, v float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Server exposes /metrics and a readiness endpoint.
type Server struct {
	http     *http.Server
	listener net.Listener
	log      *slog.Logger
	once     sync.Once
}

// Serve starts the metrics endpoint on its own port.
//
// Separate from both the peer and client ports. Scraping must keep working when
// the client API is overloaded — an unreachable metrics endpoint during an
// incident is exactly when it is needed most — and it must not be exposed to
// clients.
func Serve(addr string, n *node.Node, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		NewCollector(n, logger),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	// Readiness means "this node is part of a working cluster", not "the process
	// started". A node that is up but sees no leader cannot serve a
	// linearizable read, and routing traffic to it would only produce errors.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status, err := n.Status(ctx)
		switch {
		case err != nil:
			http.Error(w, "node is not answering", http.StatusServiceUnavailable)
		case status.Lead == raft.None:
			http.Error(w, "no leader known", http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte("ok\n"))
		}
	})

	// Liveness is deliberately weaker. A node with no leader is not ready, but
	// it is not broken — restarting it would remove a voter the cluster needs
	// to elect one, turning a recoverable outage into a longer one.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if _, err := n.Status(ctx); err != nil {
			http.Error(w, "node is not answering", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		http:     &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		listener: listener,
		log:      logger.With("component", "metrics"),
	}
	go func() {
		if err := s.http.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.log.Error("metrics server stopped", "error", err)
		}
	}()

	s.log.Info("metrics listening", "address", listener.Addr().String())
	return s, nil
}

// Addr is the address actually bound.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Stop shuts the endpoint down.
func (s *Server) Stop() {
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.http.Shutdown(ctx)
	})
}
