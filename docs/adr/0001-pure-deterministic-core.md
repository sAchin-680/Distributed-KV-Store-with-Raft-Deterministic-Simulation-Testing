# 0001 — The consensus core is a pure state machine

## Status

Accepted.

## Context

This project has two goals that pull in different directions: implement Raft
correctly, and *demonstrate* that it is correct. The second is the harder one.

Consensus bugs are overwhelmingly concurrency and timing bugs. They surface
under a specific interleaving of message delivery, timer expiry and crashes —
one that may occur in one run out of a hundred thousand. An integration test
that catches such a bug typically cannot catch it again: the interleaving that
produced it is not under the test's control, so the failure is unreproducible,
and an unreproducible failure is nearly undebuggable.

The established answer is deterministic simulation testing: drive the entire
system from a single seeded random source on a single thread, so that a run is a
pure function of its seed. FoundationDB, TigerBeetle and Antithesis all work this
way. It only works if every source of nondeterminism is under the harness's
control — and in a Go program that means time, network, disk, *and the goroutine
scheduler*.

The scheduler is the one that forces the architecture. Goroutines cannot be
replayed. If the consensus algorithm itself is spread across goroutines
communicating over channels, no amount of virtual clock or in-memory network
will make a run reproducible, because the order in which the runtime schedules
those goroutines is not ours to choose and not recorded anywhere.

So the decision had to be made before the first line of consensus code, not
after. A core written against real time and real gRPC cannot be retrofitted; it
can only be rewritten.

## Decision

The `raft` package is a **pure, synchronous state machine**. It contains no
clock, no goroutines, no channels, no mutexes, no network calls. Its entire
surface is:

```go
func (r *RawNode) Tick()
func (r *RawNode) Step(m Message) error
func (r *RawNode) Propose(cmd []byte) (Index, error)
func (r *RawNode) Messages() []Message
func (r *RawNode) Applied() []LogEntry
```

Time arrives as `Tick()` calls counted by the caller. Messages are plain values
in and plain values out. Persistence goes through the `Storage` interface.

Everything nondeterministic lives outside, behind an interface:

| Concern | Production | Simulation |
| --- | --- | --- |
| Time | `time.Now` / `time.Timer` | virtual clock advanced by the simulator |
| Network | gRPC | in-memory queue with fault injection |
| Disk | bbolt | in-memory map |
| Scheduling | one goroutine per node | one single-threaded event loop |

`scripts/check-determinism.sh` runs in CI and fails the build if a clock, a
goroutine, a channel, a mutex or a package-level `math/rand` call appears inside
`raft/`. The guard exists because this property degrades silently: nothing
announces that replay has stopped working, and by the time a fuzz run fails to
reproduce its own failure, the cause is far behind.

A fourth hazard — deriving an ordered decision from Go's randomized map
iteration — is not statically detectable. It is handled by convention (every
peer list in the core is a sorted slice; maps are used only for point lookups)
and verified by a replay test that runs a seed twice and compares a hash of the
full event trace.

## Consequences

**What this buys.**

- Any execution replays exactly from one integer. A fuzz run that fails at seed
  4,182 hands over everything needed to debug it, forever, on any machine.
- Simulated time is free. A ten-second scenario with election timeouts and
  partitions completes in milliseconds, so ten thousand randomized runs fit in a
  CI job rather than a weekend.
- The safety checker can inspect every node's internal state after every event,
  rather than inferring violations from client-visible behavior.
- In production, one goroutine per node owns all Raft state, so no Raft state is
  shared across goroutines. Data races in consensus logic are not merely
  unlikely — they are unrepresentable.

**What it costs.**

- The core cannot do anything for itself. Every timeout, retry and send has to be
  driven by a caller, which makes the driver in `node/` genuinely load-bearing
  rather than a thin wrapper.
- Storage is called synchronously, so appends are not batched or pipelined
  across the network round trip the way a throughput-oriented implementation
  would batch them. This is a real throughput ceiling, accepted deliberately;
  see ADR-0003.
- The interface indirection is visible everywhere, including in places where a
  direct call would be shorter and clearer.
- Contributors have to be told about the constraint. Hence the CI guard: the
  rule is enforced mechanically rather than by review attention.

## Alternatives considered

**Goroutine-per-node with channels, the idiomatic Go shape.** Reads more
naturally and needs no driver layer. Rejected because it forfeits replay
entirely — the defining feature of the project. A goroutine-based core can be
tested, but its failures cannot be reproduced.

**Real code plus a mocking layer in tests.** Cheaper up front. Rejected because
mocks only control the interactions someone thought to mock; the goroutine
scheduler stays uncontrolled, so rare interleavings remain unreachable and
unreproducible. It would test the code that was written rather than search for
the interleaving that breaks it.

**Building the core first and extracting the interfaces later.** Rejected on the
reasoning above: the extraction is a rewrite, and doing it after snapshots and
membership changes exist means debugging those features twice — once without the
tool that finds their bugs, once with it. This is also why the simulator is
scheduled before snapshotting rather than after all of Raft.
