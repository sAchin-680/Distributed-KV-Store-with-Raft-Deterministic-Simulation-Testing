// Command chaosctl runs a client workload against a cluster and checks the
// result is linearizable.
//
//	chaosctl --endpoints=localhost:18001,... --duration=60s
//
// Run it while deploy/chaos/chaos.sh is damaging the network. On its own it
// only proves the cluster works when nothing is wrong, which is not in doubt.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/sAchin-680/raftkv/chaos"
	"github.com/sAchin-680/raftkv/internal/buildinfo"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chaosctl:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		endpoints = flag.String("endpoints", "localhost:18001",
			"comma-separated client addresses of the cluster")
		duration = flag.Duration("duration", 30*time.Second, "how long to drive the workload")
		clients  = flag.Int("clients", 8,
			"concurrent clients\n"+
				"\tConcurrency is what makes the check meaningful: a history whose\n"+
				"\toperations never overlap has only one possible ordering.")
		keys = flag.Int("keys", 5,
			"size of the key space\n"+
				"\tSmall on purpose. The history is partitioned by key, so spreading\n"+
				"\toperations thinly leaves nothing in each partition to check.")
		timeout      = flag.Duration("op-timeout", 2*time.Second, "per-operation timeout")
		checkTimeout = flag.Duration("check-timeout", 60*time.Second,
			"how long to search for a linearization before giving up")
		readFraction = flag.Float64("reads", 0.5, "proportion of operations that are reads")
		seed         = flag.Int64("seed", 1, "makes the choice of operations reproducible (not the timing)")
		keyPrefix    = flag.String("key-prefix", "",
			"namespace for this run's keys (default: unique per run)\n"+
				"\tA cluster keeps its data between runs, so without this a run reads\n"+
				"\tvalues an earlier one wrote — values absent from this history — and\n"+
				"\tthe check fails for a reason that has nothing to do with the store.")
		out     = flag.String("out", "", "directory to write the history and any visualization to")
		recheck = flag.String("check", "",
			"re-check a saved history instead of running a workload\n"+
				"\tThe history is the evidence, so a result that came back UNKNOWN can\n"+
				"\tbe decided later with a longer --check-timeout rather than by\n"+
				"\trunning the whole experiment again and getting a different history.")
		version = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *version {
		fmt.Println("chaosctl", buildinfo.String())
		return nil
	}

	if *recheck != "" {
		return recheckHistory(*recheck, *checkTimeout, *out)
	}

	if *recheck != "" {
		return recheckHistory(*recheck, *checkTimeout, *out)
	}

	workload := &chaos.Workload{
		Endpoints:    splitEndpoints(*endpoints),
		Clients:      *clients,
		Keys:         *keys,
		Duration:     *duration,
		Timeout:      *timeout,
		ReadFraction: *readFraction,
		Seed:         *seed,
		KeyPrefix:    *keyPrefix,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("driving %d clients over %d keys for %s against %v\n",
		workload.Clients, workload.Keys, workload.Duration, workload.Endpoints)

	history, err := workload.Run(ctx)
	if err != nil {
		return err
	}
	if history.Len() == 0 {
		return errors.New("no operations were recorded; is the cluster reachable?")
	}

	fmt.Println(history.Stats())
	fmt.Printf("checking linearizability (giving up after %s)...\n", *checkTimeout)

	result := chaos.Check(history, *checkTimeout)
	fmt.Println(result)

	if *out != "" {
		if err := writeArtifacts(*out, history, result); err != nil {
			return err
		}
	}

	switch result.Status {
	case porcupine.Ok:
		return nil
	case porcupine.Illegal:
		return errors.New("the cluster returned answers no single machine could have produced")
	default:
		// An exhausted search is not a pass. Saying so loudly matters more than
		// it looks: a checker that reports "unknown" as success is decorative.
		return errors.New("the linearizability search did not finish, so the run " +
			"proved nothing; re-run with fewer operations or a longer --check-timeout")
	}
}

// recheckHistory decides a history that was saved earlier.
func recheckHistory(path string, timeout time.Duration, out string) error {
	raw, err := os.ReadFile(path) //nolint:gosec // a path the operator named
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	// Histories are large and compress about tenfold, so the ones kept as
	// evidence are stored gzipped. Reading both forms saves every caller from
	// having to know which they have.
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return fmt.Errorf("decompressing %s: %w", path, err)
		}
		raw, err = io.ReadAll(zr)
		if err != nil {
			return fmt.Errorf("decompressing %s: %w", path, err)
		}
		_ = zr.Close()
	}

	var events []chaos.Event
	if err := json.Unmarshal(raw, &events); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}

	history := chaos.HistoryFromEvents(events)
	fmt.Printf("re-checking %s\n%s\n", path, history.Stats())

	result := chaos.Check(history, timeout)
	fmt.Println(result)

	if out != "" && !result.OK() {
		if err := writeArtifacts(out, history, result); err != nil {
			return err
		}
	}
	switch result.Status {
	case porcupine.Ok:
		return nil
	case porcupine.Illegal:
		return errors.New("the cluster returned answers no single machine could have produced")
	default:
		return errors.New("the search still did not finish; give it longer or " +
			"record a smaller history")
	}
}

// writeArtifacts saves the history and, for a failure, the visualization.
//
// The history is worth keeping either way: it is the evidence, and a result
// nobody can inspect afterwards is an assertion rather than a finding.
func writeArtifacts(dir string, history *chaos.History, result chaos.Result) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}
	stamp := time.Now().UTC().Format("20060102-150405")

	path := filepath.Join(dir, fmt.Sprintf("history-%s.json", stamp))
	f, err := os.Create(path) //nolint:gosec // a directory the operator named
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(history.Events())
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	fmt.Println("history written to", path)

	if result.OK() {
		return nil
	}
	visual := filepath.Join(dir, fmt.Sprintf("violation-%s.html", stamp))
	if err := result.WriteVisualization(visual); err != nil {
		return err
	}
	fmt.Println("visualization written to", visual)
	return nil
}

func splitEndpoints(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
