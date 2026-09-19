// Package transport moves consensus messages between nodes.
//
// The interface is deliberately thin, and deliberately unreliable. Raft already
// assumes messages can be lost, delayed, reordered and duplicated, and it
// already has a retry mechanism for every message that matters — a leader
// re-sends AppendEntries on its heartbeat, a candidate re-campaigns on its
// election timeout. A transport that added its own retries and acknowledgements
// would be a second, weaker recovery mechanism layered underneath the real one,
// and the two would interfere.
//
// So Send is fire-and-forget. If a message does not arrive, the algorithm
// notices and handles it. That is not a limitation being tolerated; it is the
// failure model the algorithm was designed against.
package transport

import (
	"github.com/sAchin-680/raftkv/raft"
)

// Transport delivers messages to peers and surfaces messages from them.
//
// Implementations must be safe for concurrent use: the driver sends from its
// run loop while the network delivers on its own goroutines.
type Transport interface {
	// Send delivers msg to msg.To, best effort. It does not block on the
	// network and it does not report delivery failures, because the caller has
	// nothing useful to do with one.
	Send(msg raft.Message)

	// Recv returns the stream of messages from peers. The channel is closed
	// when the transport is closed.
	Recv() <-chan raft.Message

	// Close releases resources and stops delivery. Safe to call more than once.
	Close() error
}

// Peer is a node's network identity: the stable ID consensus uses, and the
// address that currently resolves to it.
//
// The two are separate on purpose. Raft peers address each other by ID, for the
// lifetime of the cluster; the address is an implementation detail that changes
// whenever a process is rescheduled. Conflating them is what makes a Raft
// deployment break the first time a pod is replaced — and is why the Kubernetes
// manifests later need stable network identities rather than pod IPs.
type Peer struct {
	ID      raft.NodeID
	Address string
}
