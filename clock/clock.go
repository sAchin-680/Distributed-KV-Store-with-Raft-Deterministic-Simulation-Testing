// Package clock is the seam between the driver and real time.
//
// The consensus core needs none of this — it counts logical ticks handed to it
// from outside. The *driver* is what needs a clock, to decide when those ticks
// happen and to time out client requests. Putting that behind an interface means
// a driver test can advance time instantly instead of sleeping, and it keeps the
// one rule that makes the whole verification strategy work: nothing in this
// system reads the wall clock except code that was explicitly given one.
package clock

import "time"

// Clock is a source of time.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// NewTicker returns a ticker that fires every d.
	NewTicker(d time.Duration) Ticker

	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer

	// Sleep blocks for d.
	Sleep(d time.Duration)
}

// Ticker fires repeatedly.
type Ticker interface {
	// C is the channel ticks arrive on.
	C() <-chan time.Time
	// Stop halts the ticker. It does not close C.
	Stop()
}

// Timer fires once.
type Timer interface {
	C() <-chan time.Time
	// Stop prevents the timer firing, reporting whether it had not yet fired.
	Stop() bool
	// Reset restarts the timer for d.
	Reset(d time.Duration) bool
}

// ---------------------------------------------------------------------------
// Real time
// ---------------------------------------------------------------------------

// Real is the production clock, backed by the time package.
type Real struct{}

// System is the shared real clock. It holds no state, so one value serves
// everything.
var System Clock = Real{}

func (Real) Now() time.Time                   { return time.Now() }
func (Real) Sleep(d time.Duration)            { time.Sleep(d) }
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }
func (Real) NewTimer(d time.Duration) Timer   { return realTimer{time.NewTimer(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
