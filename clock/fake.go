package clock

import (
	"sort"
	"sync"
	"time"
)

// Fake is a clock that only moves when a test tells it to.
//
// It exists so driver tests can cover behaviour that spans seconds of logical
// time without spending seconds of real time, and without the flakiness that
// comes from sleeping and hoping. A test that says "advance ten election
// timeouts" gets exactly that, deterministically.
//
// Unlike the simulator's virtual clock, this one is safe for concurrent use: the
// driver it tests is genuinely multi-goroutine, so the clock it reads has to be.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at      time.Time
	ch      chan time.Time
	period  time.Duration // non-zero for a ticker
	stopped bool
	fake    *Fake
}

// NewFake returns a fake clock starting at the given time. A zero start time is
// replaced with a fixed, arbitrary instant, so that a test printing a timestamp
// gets a stable value rather than the Unix epoch.
func NewFake(start time.Time) *Fake {
	if start.IsZero() {
		start = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Fake{now: start}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Sleep blocks until the clock has been advanced past d.
func (f *Fake) Sleep(d time.Duration) { <-f.NewTimer(d).C() }

func (f *Fake) NewTimer(d time.Duration) Timer {
	return fakeTimer{f.add(d, 0)}
}

func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: ticker period must be positive")
	}
	return fakeTicker{f.add(d, d)}
}

// Ticker.Stop and Timer.Stop have different signatures, so one waiter is
// presented through two thin wrappers rather than trying to satisfy both.
type fakeTicker struct{ w *waiter }

func (t fakeTicker) C() <-chan time.Time { return t.w.ch }
func (t fakeTicker) Stop()               { t.w.stop() }

type fakeTimer struct{ w *waiter }

func (t fakeTimer) C() <-chan time.Time        { return t.w.ch }
func (t fakeTimer) Stop() bool                 { return t.w.stop() }
func (t fakeTimer) Reset(d time.Duration) bool { return t.w.reset(d) }

func (f *Fake) add(d, period time.Duration) *waiter {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &waiter{
		at:     f.now.Add(d),
		ch:     make(chan time.Time, 1),
		period: period,
		fake:   f,
	}
	f.waiters = append(f.waiters, w)
	return w
}

// Advance moves the clock forward, firing every timer and ticker due in that
// interval, in time order.
//
// Firing in order matters: a test that advances past two elections' worth of
// time should see them in the sequence they would really occur, not whichever
// happened to be registered first.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)

	for {
		due := f.dueLocked(target)
		if len(due) == 0 {
			break
		}
		// Step the clock to each deadline in turn, so anything observing Now
		// during a fire sees the time that timer was scheduled for.
		w := due[0]
		f.now = w.at
		if w.period > 0 {
			w.at = w.at.Add(w.period)
		} else {
			f.removeLocked(w)
		}
		fireAt := f.now

		f.mu.Unlock()
		select {
		case w.ch <- fireAt:
		default:
			// A tick nobody collected. Real tickers drop these too rather than
			// queueing, so that a slow consumer does not accumulate a backlog it
			// can never catch up on.
		}
		f.mu.Lock()
	}

	f.now = target
	f.mu.Unlock()
}

// dueLocked returns waiters due at or before target, earliest first.
func (f *Fake) dueLocked(target time.Time) []*waiter {
	var due []*waiter
	for _, w := range f.waiters {
		if !w.stopped && !w.at.After(target) {
			due = append(due, w)
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].at.Before(due[j].at) })
	return due
}

func (f *Fake) removeLocked(target *waiter) {
	for i, w := range f.waiters {
		if w == target {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			return
		}
	}
}

// Waiters is how many timers and tickers are currently registered. Useful for
// asserting that a driver cleaned up after itself.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

func (w *waiter) stop() bool {
	w.fake.mu.Lock()
	defer w.fake.mu.Unlock()
	if w.stopped {
		return false
	}
	w.stopped = true
	w.fake.removeLocked(w)
	return true
}

func (w *waiter) reset(d time.Duration) bool {
	w.fake.mu.Lock()
	defer w.fake.mu.Unlock()
	active := !w.stopped
	if w.stopped {
		w.stopped = false
		w.fake.waiters = append(w.fake.waiters, w)
	}
	w.at = w.fake.now.Add(d)
	return active
}

var (
	_ Clock  = (*Fake)(nil)
	_ Ticker = fakeTicker{}
	_ Timer  = fakeTimer{}
)
