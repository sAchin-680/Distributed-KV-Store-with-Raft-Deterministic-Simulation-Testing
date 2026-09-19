package clock_test

import (
	"testing"
	"time"

	"github.com/sAchin-680/raftkv/clock"
)

func TestFakeTimeOnlyMovesWhenTold(t *testing.T) {
	c := clock.NewFake(time.Time{})
	start := c.Now()

	// Real time passing must not move a fake clock, or a test that takes a
	// moment longer on a loaded machine starts behaving differently.
	time.Sleep(5 * time.Millisecond)
	if !c.Now().Equal(start) {
		t.Errorf("clock moved on its own: %s then %s", start, c.Now())
	}

	c.Advance(time.Minute)
	if got := c.Now().Sub(start); got != time.Minute {
		t.Errorf("advanced by %s, want 1m", got)
	}
}

func TestFakeTimerFiresOnce(t *testing.T) {
	c := clock.NewFake(time.Time{})
	timer := c.NewTimer(100 * time.Millisecond)

	c.Advance(99 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("timer fired early")
	default:
	}

	c.Advance(time.Millisecond)
	select {
	case <-timer.C():
	default:
		t.Fatal("timer did not fire at its deadline")
	}

	c.Advance(time.Hour)
	select {
	case <-timer.C():
		t.Error("a one-shot timer fired twice")
	default:
	}
}

func TestFakeTickerRepeats(t *testing.T) {
	c := clock.NewFake(time.Time{})
	ticker := c.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// Ticks are collected one at a time, because a ticker's channel holds one
	// and drops the rest — the same as a real ticker, so a slow consumer cannot
	// build a backlog it will never catch up on.
	for i := range 5 {
		c.Advance(10 * time.Millisecond)
		select {
		case <-ticker.C():
		default:
			t.Fatalf("no tick %d", i+1)
		}
	}
}

func TestFakeTimersFireInTimeOrder(t *testing.T) {
	c := clock.NewFake(time.Time{})

	// Registered out of order on purpose.
	third := c.NewTimer(30 * time.Millisecond)
	first := c.NewTimer(10 * time.Millisecond)
	second := c.NewTimer(20 * time.Millisecond)

	c.Advance(time.Second)

	order := []struct {
		name  string
		timer clock.Timer
	}{{"first", first}, {"second", second}, {"third", third}}

	var fired []time.Time
	for _, o := range order {
		select {
		case at := <-o.timer.C():
			fired = append(fired, at)
		default:
			t.Fatalf("%s timer did not fire", o.name)
		}
	}
	for i := 1; i < len(fired); i++ {
		if !fired[i].After(fired[i-1]) {
			t.Errorf("timers fired out of order: %v", fired)
		}
	}
}

// Each timer should see the time it was scheduled for, not the time the caller
// advanced to. A driver that reads Now() inside a timer callback would otherwise
// see the future.
func TestFakeTimerSeesItsOwnDeadline(t *testing.T) {
	c := clock.NewFake(time.Time{})
	start := c.Now()
	timer := c.NewTimer(10 * time.Millisecond)

	c.Advance(time.Hour)

	at := <-timer.C()
	if want := start.Add(10 * time.Millisecond); !at.Equal(want) {
		t.Errorf("timer fired reporting %s, want its own deadline %s", at, want)
	}
}

func TestFakeStopPreventsFiring(t *testing.T) {
	c := clock.NewFake(time.Time{})
	timer := c.NewTimer(10 * time.Millisecond)

	if !timer.Stop() {
		t.Error("Stop on a live timer should report true")
	}
	if timer.Stop() {
		t.Error("Stop on an already-stopped timer should report false")
	}

	c.Advance(time.Hour)
	select {
	case <-timer.C():
		t.Error("a stopped timer fired")
	default:
	}
	if got := c.Waiters(); got != 0 {
		t.Errorf("%d waiters left registered, want 0", got)
	}
}

func TestFakeResetRestartsTheTimer(t *testing.T) {
	c := clock.NewFake(time.Time{})
	timer := c.NewTimer(10 * time.Millisecond)

	c.Advance(5 * time.Millisecond)
	timer.Reset(20 * time.Millisecond)

	c.Advance(10 * time.Millisecond) // past the original deadline
	select {
	case <-timer.C():
		t.Fatal("timer fired at its original deadline after being reset")
	default:
	}

	c.Advance(10 * time.Millisecond)
	select {
	case <-timer.C():
	default:
		t.Fatal("timer did not fire at the reset deadline")
	}
}

func TestFakeWaitersTracksRegistrations(t *testing.T) {
	c := clock.NewFake(time.Time{})
	if got := c.Waiters(); got != 0 {
		t.Fatalf("%d waiters on a fresh clock", got)
	}

	ticker := c.NewTicker(time.Second)
	timer := c.NewTimer(time.Second)
	if got := c.Waiters(); got != 2 {
		t.Errorf("%d waiters, want 2", got)
	}

	// Firing a one-shot timer retires it; a ticker stays.
	c.Advance(time.Second)
	<-timer.C()
	if got := c.Waiters(); got != 1 {
		t.Errorf("%d waiters after the timer fired, want 1", got)
	}

	ticker.Stop()
	if got := c.Waiters(); got != 0 {
		t.Errorf("%d waiters after stopping the ticker, want 0", got)
	}
}

func TestRealClockAdvances(t *testing.T) {
	before := clock.System.Now()
	timer := clock.System.NewTimer(time.Millisecond)
	<-timer.C()
	if !clock.System.Now().After(before) {
		t.Error("the real clock did not move")
	}
}
