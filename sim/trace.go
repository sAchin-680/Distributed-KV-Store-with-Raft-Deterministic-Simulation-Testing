package sim

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// Trace records what happened during a run, and hashes it as it goes.
//
// The hash is the mechanism that keeps seed replay honest. Determinism is the
// one property this package cannot verify by inspection, and it degrades
// silently: nothing announces that a run has stopped being reproducible. Running
// a seed twice and comparing trace hashes turns that into a test.
//
// It also catches the hazard no static check can — deriving an ordered decision
// from Go's randomized map iteration. That produces a different execution on
// every run while every line of code still looks correct.
type Trace struct {
	hash   uint64
	events int

	// lines are kept only when the trace is verbose, since a long run produces
	// far more of them than anyone wants in memory during a fuzz campaign.
	lines   []string
	verbose bool
	limit   int
}

func newTrace(verbose bool) *Trace {
	h := fnv.New64a()
	return &Trace{hash: h.Sum64(), verbose: verbose, limit: 100_000}
}

// record folds one line into the hash, and keeps it if the trace is verbose.
func (t *Trace) record(at int64, format string, args ...any) {
	line := fmt.Sprintf("%d %s", at, fmt.Sprintf(format, args...))

	h := fnv.New64a()
	var buf [8]byte
	for i := range buf {
		buf[i] = byte(t.hash >> (8 * i))
	}
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(line))
	t.hash = h.Sum64()

	t.events++
	if t.verbose && len(t.lines) < t.limit {
		t.lines = append(t.lines, line)
	}
}

// Hash identifies this execution. Two runs of the same seed must produce the
// same value; if they do not, something in the system is not deterministic.
func (t *Trace) Hash() uint64 { return t.hash }

// Events is how many things happened.
func (t *Trace) Events() int { return t.events }

// Lines returns the recorded trace, empty unless the run was verbose.
func (t *Trace) Lines() []string { return t.lines }

func (t *Trace) String() string {
	if len(t.lines) == 0 {
		return fmt.Sprintf("<%d events, hash %016x>", t.events, t.hash)
	}
	return strings.Join(t.lines, "\n")
}
