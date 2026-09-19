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

> **Status:** in active development. Leader election works, with pre-vote; log
> replication is next. See [Status](#status) for what is built and what is coming.

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

## Quickstart

```bash
git clone https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing.git raftkv
cd raftkv
make check      # vet + determinism guards + lint + tests
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
make check          # everything CI runs: fmt, vet, determinism, lint, tests
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

*Not built yet.*

```bash
make fuzz                       # 1,000 seeds (the CI gate)
make fuzz FUZZ_SEEDS=10000      # a real run
make replay SEED=12345          # reproduce one exact execution

bin/simctl run  --seed=12345 --verbose
bin/simctl fuzz --count=10000 --workers=8
```

### Running a cluster

*Not built yet.*

```bash
bin/raftd --id=1 --listen=:9001 --data=./data/1 \
          --peers=1@localhost:9001,2@localhost:9002,3@localhost:9003

bin/kvctl --endpoints=localhost:9001,localhost:9002,localhost:9003 set foo bar
bin/kvctl get foo
bin/kvctl status
```

## Project layout

Present today:

```text
raft/         the consensus core — pure, deterministic, no I/O and no goroutines
proto/        protobuf definitions and generated code
docs/         architecture notes and decision records
scripts/      the determinism guards CI runs
```

Arriving with the milestones that need them:

```text
sim/          deterministic simulator, fault injection, safety checker
storage/      the bbolt Storage implementation
transport/    Transport interface and its gRPC implementation
clock/        Clock interface, real and virtual
node/         the driver: one goroutine per node, wiring it all together
kvstore/      the replicated state machine
cmd/          raftd (server), simctl (simulator), kvctl (client)
```

## Status

Built in milestones, each with an explicit exit criterion rather than a vibe.

| | Component |
| --- | --- |
| done | Build tooling, protobuf codegen, CI, determinism guards |
| done | Core types, the log, cluster configuration, storage contract |
| done | Leader election — election restriction, pre-vote |
| next | Log replication — log matching and the commit rule |
| | Deterministic simulator, fault injection, safety checker |
| | bbolt storage, gRPC transport, node driver |
| | KV state machine with read-index linearizable reads |
| | Snapshotting and `InstallSnapshot` |
| | Joint-consensus membership changes |
| | Randomized testing at scale, with bugs found and documented |
| | `tc`/`netem` chaos and linearizability checking |
| | Kubernetes, Terraform, ArgoCD, CI correctness gate |

## Documentation

- [Architecture](docs/architecture.md) — package layout and the pure-core design
- `docs/adr/` — architecture decision records

## References

- Ongaro and Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf) — Figure 2 is the specification this implements
- Ongaro, [*Consensus: Bridging Theory and Practice*](https://github.com/ongardie/dissertation) — membership changes and pre-vote
- Will Wilson, [*Testing Distributed Systems w/ Deterministic Simulation*](https://www.youtube.com/watch?v=4fFDFbi3toc) — the FoundationDB approach this simulator follows

## License

[MIT](LICENSE)
