package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sAchin-680/raftkv/kvserver"
)

// Workload drives concurrent clients against a cluster and records what they
// saw.
type Workload struct {
	// Endpoints are the cluster's client addresses.
	Endpoints []string

	// Clients is how many run concurrently.
	//
	// Concurrency is what makes the check meaningful: operations that never
	// overlap have only one possible ordering, so a sequential history is
	// linearizable almost by construction. It is also what makes it expensive,
	// since the search is exponential in the number of overlapping operations.
	Clients int

	// Keys bounds the key space.
	//
	// Small on purpose. The history is partitioned by key, and a violation can
	// only be found within one partition, so spreading operations over a
	// thousand keys means a thousand histories of two operations each and
	// nothing to check. A handful of keys under heavy contention is where
	// violations live.
	Keys int

	// Duration is how long to run for.
	Duration time.Duration

	// Timeout bounds one operation. Exceeding it records the outcome as
	// unknown rather than as a failure, because the write may still have
	// committed.
	Timeout time.Duration

	// ReadFraction is the proportion of operations that are reads.
	ReadFraction float64

	// DeleteFraction is the proportion of writes that are deletes.
	DeleteFraction float64

	// Seed makes the choice of operations reproducible. The *timing* is not,
	// and cannot be — that is the whole difference between this and the
	// simulator.
	Seed int64

	// KeyPrefix namespaces this run's keys.
	//
	// Not cosmetic. A cluster keeps its data between runs, so a second run
	// against the same cluster reads values the first one wrote — values that
	// appear nowhere in the second run's history. The checker then correctly
	// reports a read of a value nobody wrote, and the violation is entirely the
	// harness's fault.
	//
	// The first chaos runs here failed for exactly that reason. A history has to
	// be self-contained: every value it reads must be one it can see written.
	KeyPrefix string
}

func (w *Workload) withDefaults() {
	if w.Clients == 0 {
		w.Clients = 8
	}
	if w.Keys == 0 {
		w.Keys = 5
	}
	if w.Duration == 0 {
		w.Duration = 30 * time.Second
	}
	if w.Timeout == 0 {
		w.Timeout = 2 * time.Second
	}
	if w.ReadFraction == 0 {
		w.ReadFraction = 0.5
	}
	if w.DeleteFraction == 0 {
		w.DeleteFraction = 0.15
	}
	if w.KeyPrefix == "" {
		w.KeyPrefix = fmt.Sprintf("r%d", time.Now().UnixNano())
	}
}

// Run drives the workload and returns the history.
func (w *Workload) Run(ctx context.Context) (*History, error) {
	w.withDefaults()
	if len(w.Endpoints) == 0 {
		return nil, errors.New("chaos: at least one endpoint is required")
	}

	history := NewHistory()
	deadline := time.Now().Add(w.Duration)

	var wg sync.WaitGroup
	errs := make([]error, w.Clients)

	for i := range w.Clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.runClient(ctx, i, history, deadline)
		}()
	}
	wg.Wait()

	// A client that could not connect at all is a setup problem, not a result.
	// One that merely saw errors during the run is exactly what is being
	// tested, and those are in the history.
	for i, err := range errs {
		if err != nil {
			return history, fmt.Errorf("chaos: client %d: %w", i, err)
		}
	}
	return history, nil
}

func (w *Workload) runClient(ctx context.Context, id int, history *History, deadline time.Time) error {
	client, err := kvserver.NewClient(w.Endpoints...)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	// Each client registers a session, so a retry the transport made look like
	// a failure is recognized rather than applied twice. Without it the history
	// would contain duplicated writes that no amount of correct consensus could
	// make linearizable.
	registerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = client.Register(registerCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("registering a session: %w", err)
	}

	// Per-client generator, seeded from the workload seed and the client index,
	// so which operations run is reproducible even though their timing is not.
	rng := rand.New(rand.NewSource(w.Seed + int64(id)*7919)) //nolint:gosec // reproducibility, not secrecy

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		w.step(ctx, client, id, history, rng)
	}
	return nil
}

func (w *Workload) step(ctx context.Context, client *kvserver.Client, id int, history *History, rng *rand.Rand) {
	key := fmt.Sprintf("%s-k%d", w.KeyPrefix, rng.Intn(w.Keys))

	var in Input
	switch {
	case rng.Float64() < w.ReadFraction:
		in = Input{Op: OpGet, Key: key}
	case rng.Float64() < w.DeleteFraction:
		in = Input{Op: OpDelete, Key: key}
	default:
		in = Input{Op: OpPut, Key: key, Value: fmt.Sprintf("c%d-%d", id, rng.Int63())}
	}

	opCtx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()

	pending := history.Begin(id, in)

	switch in.Op {
	case OpGet:
		value, found, err := client.Get(opCtx, []byte(in.Key))
		if err != nil {
			pending.Unknown()
			return
		}
		pending.Complete(Output{Value: string(value), Found: found})

	case OpPut:
		if _, err := client.Set(opCtx, []byte(in.Key), []byte(in.Value)); err != nil {
			// The write may have committed anyway — the answer was lost, not
			// necessarily the command. Recording it as unknown lets the checker
			// consider both, which is the truth.
			pending.Unknown()
			return
		}
		pending.Complete(Output{})

	case OpDelete:
		existed, err := client.Delete(opCtx, []byte(in.Key))
		if err != nil {
			pending.Unknown()
			return
		}
		pending.Complete(Output{Found: existed})
	}
}
