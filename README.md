# Distributed KV Store with Raft + Deterministic Simulation Testing

**A replicated key/value store on hand-written Raft — verified by a deterministic
simulator that replays any failure from a single integer.**

[![CI](https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing/actions/workflows/ci.yml/badge.svg)](https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue)

Raft, implemented from the paper: leader election, log replication, snapshotting,
and joint-consensus membership changes. On top of it, a linearizable key/value
store. Around it, the part that matters — a **deterministic simulation framework**
that controls time and the network, injects partitions and crashes from a seeded
random source, and reproduces any execution exactly.

When a randomized run finds a safety violation, it prints one number. That number
is enough for anyone to replay the identical failure, byte for byte, forever.

> **Status:** in active development. Consensus is complete — election,
> replication, snapshotting, linearizable reads and joint-consensus membership
> changes — and a containerised cluster has been checked for linearizability
> under real network faults. Deployment comes next. See [Status](#status).

```text
                     ┌───────────────────────────────────────────────┐
   client ──gRPC──>  │  kv service  │  node driver  │   raft core    │
                     │              │ (1 goroutine) │ (pure, no I/O) │
                     └───────┬──────────────┬────────────────┬───────┘
                             │              │                │
                          Storage       Transport          Clock
                             │              │                │
      production:          bbolt          gRPC           time.Now
      simulation:        in-memory     event queue     virtual clock
```

Those three interfaces are the entire design. The consensus core has no clock, no
goroutines, no network and no disk — time arrives as `Tick()` calls, messages are
plain values, persistence goes through an interface. Swap what sits beneath it and
the *same* consensus code runs either as a real distributed system or inside a
single-threaded simulator where a ten-second scenario completes in milliseconds.

## Why it is built this way

**Determinism is architectural, not a testing technique.** A core that calls
`time.Now()` or dials gRPC gives a simulator nothing to control. So the core is
pure from the first commit, and [`scripts/check-determinism.sh`](scripts/check-determinism.sh)
fails CI if a clock, a goroutine, a channel or a package-level `math/rand` ever
appears inside it.

**One goroutine owns all Raft state.** Not a mutex around shared state — a single
goroutine that is the only thing permitted to touch it. Most broken Raft
implementations are broken by data races rather than by the algorithm; this
structure makes that category unrepresentable.

**The hard parts are not skipped.** Joint consensus for membership changes, the
current-term-only commit rule, read-index for linearizable reads, and
session-based deduplication so retried writes cannot corrupt the client-visible
history. These are the details that separate a working Raft from a plausible one.

## Verification strategy

Four layers, each catching what the others structurally cannot.

| Layer | Catches | Blind to |
| --- | --- | --- |
| Unit tests on the pure core | Algorithm logic, log-boundary edge cases | Anything emergent from interleaving |
| Deterministic simulation | Rare interleavings, partition and crash races, safety violations in internal state — **reproducibly** | Bugs in gRPC wiring, bbolt usage, serialization |
| Real chaos + [Porcupine](https://github.com/anishathalye/porcupine) | The gap between model and implementation; client-visible linearizability | Rare interleavings — it cannot replay a failure |
| Metrics and alerting | Behavior at real scale and duration | Correctness, directly |

The simulator proves the *algorithm* under adversarial scheduling. The chaos rig
proves the *implementation* under real gRPC deadlines, real fsyncs and real OS
scheduling. Neither is redundant; each covers the other's blind spot.

## The simulation campaign

```
$ simctl fuzz --count=10000
fuzzing seeds 1..10000 across 12 workers (5 nodes, 30000ms virtual each)
ran 10000 seeds in 4m2.867s (41 seeds/sec, 13535461 entries committed,
                             2092853788 events)
no safety violations
```

| | |
| --- | --- |
| Seeds | 10,000 |
| Simulated events | 1,713,783,733 |
| Entries committed | 12,257,429 |
| Cluster time simulated | **83.3 hours in 3m38s** — 1,376× real time |
| Safety violations | **0** |

Every feature is exercised throughout rather than mentioned. A typical
thirty-second run takes **100+ snapshots**, transfers ~70 of them to followers
that fell behind the start of the leader's log, and performs **~9 membership
changes** — all while partitioning, crashing and restarting nodes.

Checked after every single event: at most one leader per term, no committed entry
ever changes, commit index never decreases, applied never exceeds committed. At
the end of each run: log matching pairwise across all nodes, and no two nodes
having applied a different command at the same position.

### One integer reproduces any failure

That is the whole point of building it this way. Three runs of the same seed:

```
284481 events, 12633ms virtual, trace 39e3e8aef5e8191d
284481 events, 12633ms virtual, trace 39e3e8aef5e8191d
284481 events, 12633ms virtual, trace 39e3e8aef5e8191d

safety violation [committed-entries-never-change] at t=12633ms (seed 2):
  committed index 157 changed: node 4 committed term 1 (bca6269a) at t=3255ms,
  node 1 now has term 2 (da3a08fa)
```

Identical event count, identical trace hash, identical violation at the same
index and the same millisecond — on any machine, indefinitely. A randomized test
that cannot do this reports failures nobody can act on.

### Bugs it found

Enabling snapshotting made the simulator fail within the first twenty seeds.
Three were real defects, each only reachable once compaction existed:

| Bug | Why it hid | Consequence |
| --- | --- | --- |
| A delayed `AppendEntries` below the commit index was scanned for conflicts | Before compaction the scan could verify the old entries and correctly found none | An entry that is merely *unknown* is reported as *conflicting*, and the node refuses it as an attempt to overwrite its committed prefix |
| The bbolt store conflated the log floor with the snapshot boundary | They are the same until a leader keeps a tail of entries past the snapshot | `Compact` decides it has already run and silently does nothing — the log grows for ever behind a snapshot that claimed to have shortened it |
| A node restarting never restored its state machine from its own snapshot | Without compaction, a restart replays the whole log and gets there anyway | The node comes back holding a fraction of its state, silently |

The first was reported as seed 188 and reproduced exactly from that integer while
it was being diagnosed. All three are now regression-tested and seeded into the
mutation suite.

A fourth failure — eleven seeds reporting state machine divergence — turned out
to be the simulator's own model of a state machine, not a Raft defect. Reported
as such rather than counted as a find.

### The checker is validated against a real violation

Ten thousand clean runs prove nothing until the instrument is shown to detect
what it is looking for. A checker that never fires is indistinguishable from a
broken one.

Raft's safety argument assumes a node's term and vote survive a crash.
`--disk-loss` removes that assumption, which *must* produce two leaders in a term
and therefore overwritten committed entries. It does — **7 failing seeds out of
41** — caught two independent ways: the simulator's checker spotting a changed
committed entry, and the core's own append path refusing to overwrite its
committed prefix.

Those are not implementation bugs. They are the predicted consequence of breaking
a documented assumption, used as a control.

## Chaos and linearizability

The simulator proves the *algorithm* under adversarial scheduling by reading each
node's internal state. It says nothing about the *implementation*: it replaces
gRPC with an in-memory queue, bbolt with a map, and the Go scheduler with a
single-threaded loop. This closes that gap.

Five containers, `tc`/`netem` applying 60ms ±40ms delay, 3% loss and 5%
reordering, three `iptables` partitions and two `SIGKILL`s on a schedule, while
ten concurrent clients drive the store. Every operation's invocation, completion
and result is recorded, and the history is checked with
[Porcupine](https://github.com/anishathalye/porcupine): *could a single machine,
executing these one at a time, have produced these answers?*

```bash
make chaos-up      # five-node cluster in containers
make chaos-run     # faults and workload together, then check the history
make chaos-down
```

| Run | Clients | Operations | Unknown outcome | Result |
| --- | --- | --- | --- | --- |
| 1 | 10 | 3,835 | 326 (8.5%) | undecided |
| 2 | 10 | 3,471 | 325 (9.4%) | linearizable |
| 3 | 10 | 3,327 | 309 (9.3%) | linearizable |
| 4 | 5 | 341 | 116 (34%) | linearizable |

Recorded histories and the full account are in
[docs/chaos-runs/](docs/chaos-runs/).

**Operations whose outcome the client never learned are kept**, not discarded. A
write that timed out may have committed anyway — the *answer* was lost, not
necessarily the command — so it is recorded as invoked-and-never-returned and the
checker is free to place it either way. Dropping them would remove exactly the
operations most likely to be involved in a violation.

**An inconclusive search is not a pass.** Porcupine's search is exponential in
concurrent operations; when it runs out of time the result is `UNKNOWN` and
`chaosctl` exits non-zero. A history that could not be decided has not been shown
to be legal.

**The first runs failed, and the harness was at fault.** Reads were returning
values absent from that run's history — written by the *previous* run, because
container volumes persist. The checker was right; the history was not
self-contained. Fixed by namespacing each run's keys, and written up in full
rather than quietly corrected.

## Measured

Core-only numbers — no disk, no network, no goroutines. They say how much
headroom the algorithm leaves, not what a cluster sustains; a real deployment is
bounded by fsync and network round trips. Apple M4 Pro, Go 1.25.1, median of 3
runs. Reproduce with `make bench` and `make mutation`.

| | |
| --- | --- |
| Commit round, 3-node cluster | 1.55 µs — **647k commits/sec** |
| Commit round, 5-node cluster | 3.13 µs — **320k commits/sec** |
| Steady-state heartbeat, 3-node | 595 ns |
| Leader election from cold start | **12.1 logical ticks** median |
| Seeded bugs caught by the test suite | **22 / 22** |
| Statement coverage of the core | 76.5% |
| Test-to-code ratio | 0.98 : 1 |

### What the conflict-hint optimization is worth

Repairing a follower whose log diverged by 200 entries across 2 terms, measured
both ways:

| | Round trips | Wall time |
| --- | --- | --- |
| With the §5.3 conflict hint | **6** | 12 µs |
| Backing up one index at a time | 204 | 208 µs |

34× fewer round trips. Those are *network* round trips in a real cluster: at
1 ms RTT it is the difference between a 6 ms rejoin and a 204 ms one.

### Mutation testing

`make mutation` seeds twenty-two known Raft bugs — the classic ones, including Figure 8
and index-first log comparison — and checks that the test named after each one
actually fails. It runs in 2.2 s and gates CI.

A passing test proves nothing until you have watched it fail. This has caught
**three decorative tests** in this repository: tests that passed even with the
bug they were named after present. One example: the pre-vote lease test left
`LastLogIndex` at zero, so the election restriction was doing the refusing and
the lease check could be deleted without the test noticing.

## Quickstart

```bash
git clone https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing.git raftkv
cd raftkv
make check      # fmt, vet, determinism guards, lint, tests, mutation check
```

Go 1.25+ is the only requirement for building and testing. The rest is needed
only to regenerate protobuf code or run the linter.

```bash
brew install protobuf golangci-lint
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Commands

Everything runs through `make`; `make help` lists every target.

### Build and test

```bash
make build                     # all binaries into ./bin
make build VERSION=v0.1.0      # stamp a version into the binaries
make test                      # full suite, race detector on
make test-short                # fast tests only
make cover                     # coverage report -> coverage.html
make clean

go test ./raft/...                             # one package
go test -v -run TestElectionRestriction ./raft # one test, verbose
go test -race -count=10 ./raft                 # repeat, to shake out flakes
```

### Static analysis

```bash
make check          # everything CI runs: fmt, vet, determinism, lint, tests, mutation
make bench          # consensus-core throughput benchmarks
make mutation       # verify the tests can detect the bugs they are named after
make fmt            # format
make fmt-check      # fail if anything is unformatted
make vet
make lint
make determinism    # assert the core has no clock, goroutines or global rand
```

### Code generation

```bash
make proto          # regenerate Go code from proto/*.proto
make proto-check    # fail if the checked-in generated code is stale
make tidy           # sync go.mod / go.sum
```

### Simulation

```bash
make fuzz                       # 1,000 seeds (the CI gate)
make fuzz FUZZ_SEEDS=10000      # the full campaign
make replay SEED=12345          # reproduce one exact execution

bin/simctl run  --seed=12345 --verbose
bin/simctl fuzz --count=10000 --workers=8

# tell an algorithm bug from a fault-handling one
bin/simctl run --seed=12345 --no-faults

# the negative control: break Raft's durability assumption on purpose
bin/simctl fuzz --count=200 --disk-loss=0.5
```

### Running a cluster

```bash
# Peer traffic and client traffic get separate ports, so a flood of client
# requests cannot starve the heartbeats that keep the leader in office.
for i in 1 2 3; do
  bin/raftd --id=$i --listen=127.0.0.1:900$i --client-listen=127.0.0.1:800$i \
            --data=./data/$i --tick=50ms \
            --peers=1@127.0.0.1:9001,2@127.0.0.1:9002,3@127.0.0.1:9003 &
done

E=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
bin/kvctl --endpoints=$E status
bin/kvctl --endpoints=$E set greeting "hello world"
bin/kvctl --endpoints=$E get greeting
bin/kvctl --endpoints=$E --stale get greeting   # skips the leadership check
bin/kvctl --endpoints=$E delete greeting

# Membership changes name the complete new voter set, not a delta.
bin/kvctl --endpoints=$E members 1 2
```

`kvctl` is given the whole cluster, not one node. Leadership moves on its own
schedule, so it follows the redirect and remembers where it ended up.

```text
ENDPOINT         NODE  STATE     TERM  LEADER  COMMIT  APPLIED  LAG
127.0.0.1:8001   1     follower  1     3       1       1        -
127.0.0.1:8002   2     follower  1     3       1       1        -
127.0.0.1:8003   3     leader    1     3       1       1        0
```

Membership changes pass through a **joint configuration** that requires
majorities of *both* the old and new voter sets. That overlap is what makes it
impossible for the two configurations to elect separate leaders mid-change — the
failure a single-step change allows, and the part most from-scratch Raft
implementations skip.

## Project layout

```text
raft/         the consensus core — pure, deterministic, no I/O and no goroutines
sim/          deterministic simulator, fault injection, safety checker
chaos/        client workload recorder and the Porcupine linearizability model
node/         the driver: one goroutine per node, owning all Raft state
storage/      durable bbolt storage
transport/    peer transport: the interface and its gRPC implementation
clock/        Clock interface, real and fake
kvstore/      the replicated state machine, with client-session deduplication
kvserver/     the client-facing gRPC API and a cluster-aware client
codec/        conversion between core types and protobuf
proto/        protobuf definitions and generated code
cmd/          raftd, kvctl, simctl, chaosctl
deploy/       container compose file and the tc/netem chaos scripts
docs/         architecture notes and decision records
scripts/      determinism guards and the mutation suite
```

## Status

Built in milestones, each with an explicit exit criterion rather than a vibe.

| | Component |
| --- | --- |
| done | Build tooling, protobuf codegen, CI, determinism guards, mutation gate |
| done | Core types, the log, cluster configuration, storage contract |
| done | Leader election — election restriction, pre-vote |
| done | Log replication — log matching and the commit rule |
| done | Deterministic simulator, fault injection, safety checker, `simctl` |
| done | bbolt storage, gRPC transport, node driver, `raftd` |
| done | KV state machine, read-index linearizable reads, client API, `kvctl` |
| done | Snapshotting, log compaction, and `InstallSnapshot` |
| done | Joint-consensus membership changes |
| next | `tc`/`netem` chaos and linearizability checking |
| | Randomized testing at scale, with bugs found and documented |
| | Kubernetes, Terraform, ArgoCD, CI correctness gate |

## Documentation

- [Architecture](docs/architecture.md) — package layout and the pure-core design
- [ADR-0001](docs/adr/0001-pure-deterministic-core.md) — why the consensus core has no clock, I/O or goroutines
- [ADR-0002](docs/adr/0002-synchronous-persistence.md) — what fsync costs, measured, and the ceiling it sets
- [ADR-0003](docs/adr/0003-read-index.md) — why the obvious read is wrong, and why not a leader lease

## References

- Ongaro and Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) — Figure 2 is the specification this implements
- Ongaro, [*Consensus: Bridging Theory and Practice*](https://github.com/ongardie/dissertation) — membership changes and pre-vote
- Will Wilson, [*Testing Distributed Systems w/ Deterministic Simulation*](https://www.youtube.com/watch?v=4fFDFbi3toc) — the FoundationDB approach this simulator follows

## License

[MIT](LICENSE)
