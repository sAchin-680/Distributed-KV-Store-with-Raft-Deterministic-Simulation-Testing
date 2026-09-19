# 0002 — Synchronous persistence in the core, and what it costs

## Status

Accepted, with a measured ceiling and a planned revisit.

## Context

Raft's safety argument depends on an ordering: a vote or an appended entry must
be durable **before** the response admitting to it leaves the node. Reply first
and crash, and a node can come back having forgotten it voted, vote a second
time in the same term, and elect a second leader. Election Safety — and
everything built on it — is gone.

There are two ways to satisfy that ordering.

The **synchronous** design has the core call `Storage` inside `Step`/`Tick`,
before returning the messages those steps produced. The ordering is then
structural: there is no way to emit a response without having persisted first,
because the persist happens earlier in the same function.

The **asynchronous** design, which etcd/raft uses, has the core return a `Ready`
struct describing what to persist and what to send, and makes the *driver*
responsible for doing them in the right order. That allows batching many entries
into one write and pipelining the write against the network round trip. It also
moves a safety-critical ordering constraint out of the algorithm and into every
caller.

## Decision

The core persists synchronously. `persistHardState` runs before `send`, always,
and `raftLog.append` completes before the acknowledging message is produced.

## Consequences

**What this buys.** The ordering cannot be got wrong by a caller, because callers
have no say in it. The core is auditable by reading it top to bottom: every path
that emits a response has a persist above it. For a project whose central claim
is verified correctness, that is worth a great deal.

**What it costs — measured, not guessed.**

`BenchmarkBoltAppend`, Apple M4 Pro, bbolt with fsync on commit:

| | Per operation | Throughput |
| --- | --- | --- |
| Consensus core, full 3-node commit round | 1.55 µs | 647,000/sec |
| One durable append (fsync) | 7,974 µs | **125/sec** |
| 128 entries in one durable append | 8,200 µs | **15,611 entries/sec** |

Three things follow.

**fsync is roughly 5,000× more expensive than the consensus algorithm itself.**
Any statement about this system's throughput is a statement about disk and
network, never about the algorithm. The core benchmarks describe headroom, not
capacity, and must not be quoted as system throughput.

**Batching is nearly free.** Appending 128 entries costs 3% more latency than
appending one, for 124× the throughput. The cost is dominated entirely by the
fsync, not by the bytes.

**Therefore the synchronous design has a hard ceiling of about 125 writes per
second**, because each `Propose` triggers its own append and therefore its own
fsync. That number is fine for configuration data, cluster metadata, service
discovery, leader election — the workloads Raft is usually deployed for. It is
not fine for a high-throughput store, and it would be dishonest to present it as
such.

## Alternatives considered

**etcd-style `Ready`/`Advance`.** Strictly better for throughput. Rejected for
now because it relocates the safety-critical ordering into the driver, and the
driver is the part of this system that is *not* verified by the simulator — the
simulator drives the core directly. Trading a verified constraint for an
unverified one, in exchange for throughput this project does not yet need, is a
bad trade at this stage.

**`NoSync`.** Available in `storage.Options`, named to discourage it, documented
as unsafe. It makes appends 194× faster (41 µs versus 7,974 µs) by giving up
exactly the guarantee Raft depends on. The simulator can demonstrate what that
costs: running with `--disk-loss` produces genuine safety violations. It exists
for benchmarks, never for production.

## Revisit when

A measurement shows write throughput is the binding constraint. The fix does not
require abandoning synchronous persistence — batching at the driver level, where
proposals arriving close together are accumulated into one append, captures most
of the 124× without moving the ordering constraint out of the core. That is the
first thing to try, and the numbers above say what it is worth.
