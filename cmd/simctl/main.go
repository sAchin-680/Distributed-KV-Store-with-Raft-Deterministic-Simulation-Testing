// Command simctl drives the deterministic Raft simulator.
//
// Two things it does:
//
//	simctl run --seed=12345      reproduce one exact execution
//	simctl fuzz --count=10000    search for a seed that breaks safety
//
// The relationship between them is the point. A fuzz run that finds a violation
// reports a single integer, and that integer is sufficient for anyone, on any
// machine, to reproduce the identical failure — the same messages, in the same
// order, with the same crashes at the same instants. A randomized test that
// cannot do this reports failures nobody can act on.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sAchin-680/raftkv/internal/buildinfo"
	"github.com/sAchin-680/raftkv/sim"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "fuzz":
		err = cmdFuzz(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Println("simctl", buildinfo.String())
	case "help", "--help", "-h":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "simctl: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "simctl:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `simctl — deterministic Raft simulation

Usage:
  simctl run   --seed=N [flags]    run one seed, reproducibly
  simctl fuzz  --count=N [flags]   run many seeds and report failures
  simctl version

Run 'simctl run -h' or 'simctl fuzz -h' for the flags of each.

Exit status is 1 if any run found a safety violation.
`)
}

// simFlags registers the knobs shared by both subcommands.
func simFlags(fs *flag.FlagSet) *sim.Config {
	cfg := sim.DefaultConfig(0)
	fs.IntVar(&cfg.Nodes, "nodes", cfg.Nodes, "cluster size")
	fs.Int64Var(&cfg.Duration, "duration", cfg.Duration, "virtual milliseconds to simulate")
	fs.Int64Var(&cfg.WriteInterval, "write-interval", cfg.WriteInterval,
		"virtual ms between client writes (0 disables client traffic)")
	fs.BoolVar(&cfg.PreVote, "pre-vote", cfg.PreVote, "enable the pre-vote straw poll")

	f := &cfg.Faults
	fs.Float64Var(&f.DropRate, "drop", f.DropRate, "probability a message is dropped")
	fs.Float64Var(&f.DuplicateRate, "duplicate", f.DuplicateRate, "probability a message is delivered twice")
	fs.Int64Var(&f.MinLatency, "min-latency", f.MinLatency, "minimum message delay, virtual ms")
	fs.Int64Var(&f.MaxLatency, "max-latency", f.MaxLatency, "maximum message delay, virtual ms")
	fs.Float64Var(&f.ReorderRate, "reorder", f.ReorderRate, "probability a message is delayed far behind later ones")
	fs.Float64Var(&f.PartitionRate, "partition", f.PartitionRate, "probability of a network split per fault interval")
	fs.Float64Var(&f.CrashRate, "crash", f.CrashRate, "probability of crashing a node per fault interval")
	fs.Float64Var(&f.DiskLossRate, "disk-loss", f.DiskLossRate,
		"probability a restarting node loses its persisted state\n"+
			"\t(Raft's safety argument assumes it does not; expect real violations)")

	return &cfg
}

// finish applies -no-faults last, so it wins regardless of the order the flags
// appeared on the command line.
func finish(cfg *sim.Config, perfect bool) {
	if perfect {
		cfg.Faults = sim.NoFaults()
	}
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfg := simFlags(fs)
	seed := fs.Int64("seed", 1, "the seed that determines the entire execution")
	verbose := fs.Bool("verbose", false, "print the full event trace")
	traceLimit := fs.Int("trace-lines", 200, "trace lines to print (0 for all)")
	perfect := fs.Bool("no-faults", false, "run with a perfect network")
	if err := fs.Parse(args); err != nil {
		return err
	}
	finish(cfg, *perfect)

	cfg.Seed = *seed
	cfg.Verbose = *verbose

	s, err := sim.New(*cfg)
	if err != nil {
		return err
	}
	report, err := s.Run()
	if err != nil {
		return fmt.Errorf("seed %d failed to run: %w", *seed, err)
	}

	if *verbose {
		lines := report.Trace.Lines()
		if *traceLimit > 0 && len(lines) > *traceLimit {
			fmt.Printf("... showing the last %d of %d trace lines ...\n", *traceLimit, len(lines))
			lines = lines[len(lines)-*traceLimit:]
		}
		for _, line := range lines {
			fmt.Println(line)
		}
		fmt.Println()
	}

	fmt.Println(report)
	fmt.Println("\nfinal cluster state:")
	for _, line := range s.Nodes() {
		fmt.Println(" ", line)
	}

	if report.Violation != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", report.Violation)
		return errors.New("safety violation")
	}
	return nil
}

// ---------------------------------------------------------------------------
// fuzz
// ---------------------------------------------------------------------------

func cmdFuzz(args []string) error {
	fs := flag.NewFlagSet("fuzz", flag.ExitOnError)
	cfg := simFlags(fs)
	count := fs.Int64("count", 1000, "how many seeds to run")
	start := fs.Int64("start", 1, "first seed")
	workers := fs.Int("workers", runtime.NumCPU(), "parallel workers")
	maxReport := fs.Int("max-failures", 10, "stop after this many failing seeds (0 for no limit)")
	quiet := fs.Bool("quiet", false, "suppress the progress line")
	perfect := fs.Bool("no-faults", false, "run with a perfect network")
	if err := fs.Parse(args); err != nil {
		return err
	}
	finish(cfg, *perfect)

	if *workers < 1 {
		*workers = 1
	}

	fmt.Printf("fuzzing seeds %d..%d across %d workers "+
		"(%d nodes, %dms virtual each)\n",
		*start, *start+*count-1, *workers, cfg.Nodes, cfg.Duration)

	type failure struct {
		seed   int64
		detail string
	}

	var (
		mu        sync.Mutex
		failures  []failure
		done      atomic.Int64
		committed atomic.Int64
		events    atomic.Int64
		stop      atomic.Bool
		seeds     = make(chan int64, *workers*4)
		wg        sync.WaitGroup
	)

	began := time.Now()
	if !*quiet {
		go progress(&done, *count, began, &stop)
	}

	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range seeds {
				if stop.Load() {
					return
				}
				c := *cfg
				c.Seed = seed

				detail := ""
				s, err := sim.New(c)
				if err != nil {
					detail = err.Error()
				} else if report, runErr := s.Run(); runErr != nil {
					detail = runErr.Error()
				} else {
					committed.Add(int64(report.Committed))
					events.Add(int64(report.Events))
					if report.Violation != nil {
						detail = report.Violation.Error()
					}
				}

				if detail != "" {
					mu.Lock()
					failures = append(failures, failure{seed: seed, detail: detail})
					if *maxReport > 0 && len(failures) >= *maxReport {
						stop.Store(true)
					}
					mu.Unlock()
				}
				done.Add(1)
			}
		}()
	}

	for seed := *start; seed < *start+*count; seed++ {
		if stop.Load() {
			break
		}
		seeds <- seed
	}
	close(seeds)
	wg.Wait()
	stop.Store(true)
	elapsed := time.Since(began)

	ran := done.Load()
	fmt.Printf("\r%s\r", spaces(78))
	fmt.Printf("ran %d seeds in %s (%.0f seeds/sec, %d entries committed, %d events)\n",
		ran, elapsed.Round(time.Millisecond),
		float64(ran)/elapsed.Seconds(), committed.Load(), events.Load())

	if len(failures) == 0 {
		fmt.Printf("no safety violations\n")
		return nil
	}

	// Sorted so the reported failures do not depend on which worker finished
	// first. The lowest seed is the one to debug.
	sort.Slice(failures, func(i, j int) bool { return failures[i].seed < failures[j].seed })

	fmt.Printf("\n%d failing seed(s):\n", len(failures))
	for _, f := range failures {
		fmt.Printf("  seed %d: %s\n", f.seed, f.detail)
	}
	fmt.Printf("\nreproduce the first one with:\n  simctl run --seed=%d --verbose\n",
		failures[0].seed)
	return errors.New("safety violations found")
}

func progress(done *atomic.Int64, total int64, began time.Time, stop *atomic.Bool) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if stop.Load() {
			return
		}
		n := done.Load()
		if n == 0 {
			continue
		}
		rate := float64(n) / time.Since(began).Seconds()
		remaining := time.Duration(float64(total-n)/rate) * time.Second
		fmt.Printf("\r  %d/%d seeds  %.0f/sec  eta %s   ",
			n, total, rate, remaining.Round(time.Second))
	}
}

func spaces(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}
