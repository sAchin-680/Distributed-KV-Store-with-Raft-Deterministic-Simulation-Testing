# Architecture

## Package layout

```text
proto/            protobuf definitions (peer RPCs + client KV API) and generated code
raft/             the consensus core — pure, deterministic, no I/O, no clock, no goroutines
                    types.go           the vocabulary: Term, Index, LogEntry, Message
                    config.go          membership and quorum arithmetic
                    log.go             the log: matching property, election restriction
                    storage.go         the durability contract
                    memory_storage.go  in-memory Storage, used by tests and the simulator
storage/          the bbolt Storage implementation (production)
kvstore/          the replicated state machine: a KV map fed by committed log entries
transport/        Transport interface + gRPC implementation
clock/            Clock interface + real and virtual implementations
node/             the driver: one goroutine per node wiring core + clock + transport + storage
sim/              deterministic simulator, fault injection, safety checker
cmd/raftd/        the server binary
cmd/simctl/       the simulation CLI
cmd/kvctl/        a client CLI
```

`MemoryStorage` lives in `raft` rather than in `storage` for a mundane reason: the
core's own tests need it, and a separate package that imports `raft` cannot be
imported back by `raft`'s internal tests without a cycle. etcd puts its
`MemoryStorage` in the same place for the same reason. The bbolt implementation
has no such constraint and stays in `storage/`.

## The central design decision: a pure core

`raft.RawNode` is a **synchronous state machine**. It has no goroutines, reads no
clock, performs no network I/O, and never blocks. Its entire surface is:

```go
func (r *RawNode) Tick()                          // one logical time step elapsed
func (r *RawNode) Step(m Message) error           // an incoming message arrived
func (r *RawNode) Propose(cmd []byte) (Index, error)
func (r *RawNode) Messages() []Message            // drain outbound messages
func (r *RawNode) Applied() []LogEntry            // drain newly committed entries
```

Everything nondeterministic lives *outside* the core:

| Concern       | Production                 | Simulation                        |
|---------------|----------------------------|-----------------------------------|
| Time          | `clockx.Real` (`time.Now`) | virtual clock the simulator advances |
| Network       | gRPC client/server         | in-memory queue with fault injection |
| Disk          | `storage.Bolt`             | `storage.Memory`                  |
| Scheduling    | one goroutine per node     | one single-threaded event loop     |

This is why Phase 2 is possible at all. A goroutine-and-channel Raft cannot be
replayed byte-for-byte from a seed, because the Go scheduler is not under our
control. A pure core driven by a single-threaded event loop can.

`node.Node` is the production driver: it owns exactly one goroutine per node, and
every mutation of Raft state happens inside that goroutine's select loop. No Raft
state is ever guarded by a mutex shared across goroutines, because no Raft state is
ever touched by more than one goroutine. Most "my Raft is broken" bugs are data
races, not algorithm errors; this structure makes them unrepresentable.

## Persistence ordering

The core writes to `Storage` synchronously inside `Step`/`Tick`, before returning the
messages those steps produced. That is exactly the ordering Raft requires — a vote or
an appended entry must be durable *before* the corresponding response leaves the node
— and it makes the core trivially auditable. It gives up batching and pipelining;
that trade is deliberate, and documented in ADR-0003.
