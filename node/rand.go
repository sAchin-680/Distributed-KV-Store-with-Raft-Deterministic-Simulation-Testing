package node

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sAchin-680/raftkv/raft"
)

// seededRand is the production source of election-timeout jitter.
//
// Seeded from the node ID as well as the clock so that nodes started in the
// same instant — which is exactly what a Kubernetes rollout does — do not draw
// the same timeout and campaign in lockstep. The whole point of the jitter is
// to break that symmetry, and a shared seed would defeat it.
//
// Guarded by a mutex because the core draws from it inside the run loop while
// nothing else does today, but the cost is negligible and the alternative is a
// latent data race the moment that stops being true.
type seededRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newSeededRand(id raft.NodeID) raft.Rand {
	seed := time.Now().UnixNano() ^ int64(uint64(id)*0x9E3779B97F4A7C15)
	return &seededRand{r: rand.New(rand.NewSource(seed))} //nolint:gosec // jitter, not secrecy
}

func (s *seededRand) Intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Intn(n)
}
