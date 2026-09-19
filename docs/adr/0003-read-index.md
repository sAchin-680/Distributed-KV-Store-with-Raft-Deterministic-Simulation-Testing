# 0003 — Read-index, and why the obvious read is wrong

## Status

Accepted.

## Context

A client wants to read a key. The obvious implementation is one line:

```go
// Wrong.
if node.IsLeader() {
    return stateMachine.Get(key)
}
```

It is wrong, and it is wrong in the worst available way: it returns plausible
data, with no error, and the failure is invisible to everyone involved.

### The failure

Five nodes. Node 1 is leader. A network partition puts nodes 1 and 2 on one
side, nodes 3, 4 and 5 on the other.

```
   ┌─ node 1 (leader) ─┐            ┌─ node 3 ─ node 4 ─ node 5 ─┐
   │  node 2           │     ╳      │  elects node 4 at term 8   │
   └───────────────────┘            │  commits x=2, x=3, x=4     │
                                    └────────────────────────────┘
```

Node 1 still believes it is the leader, and *nothing tells it otherwise*. From
the inside, a partition is indistinguishable from an idle cluster: in both cases
no messages arrive. Its election timeout will eventually fire, but until then —
hundreds of milliseconds, and longer if its own heartbeats are still succeeding
against node 2 — it is a leader by its own reckoning.

Meanwhile node 4 has been elected and has committed three writes. A client
reading from node 1 gets `x=1`, the value from before the partition. Not an
error. Not a timeout. A stale value returned as though it were current.

This is a linearizability violation. A read that begins after a write completes
must observe that write, and this one does not.

### Why "am I the leader?" cannot fix it

The problem is not that the check is implemented badly. The problem is that
leadership is a fact about the *cluster*, and the node is checking a variable in
its own memory. That variable was last correct at some point in the past, and
nothing has updated it since, because the thing that would have updated it is
exactly what the partition removed.

A node can only learn whether it is still the leader by asking other nodes.

## Decision

Reads go through **read-index** (Raft paper §8):

1. **Record the commit index.** Call it the read index. This is the point the
   read must observe.
2. **Confirm leadership with a quorum.** Exchange heartbeats; wait for a
   majority to reply acknowledging this term. A majority cannot simultaneously
   be acknowledging a different leader in a later term — two majorities of the
   same set always intersect — so this is proof, not a guess.
3. **Wait for the state machine to apply the read index**, then read.

A read that cannot complete step 2 returns an error.

### The precondition nobody mentions

Step 1 assumes the commit index means what it appears to mean. It does not,
until the leader has committed an entry from its **own term**.

A freshly elected leader may hold entries from a previous term that were
committed by that leader but whose commit it has not yet learned. Its commit
index therefore *understates* what the cluster has committed, and a read against
it could miss a write that had already completed.

The no-op entry a leader appends the moment it is elected exists partly for this.
Until it commits, `ReadIndex` returns `ErrReadIndexUnavailable` and the caller
retries. The window is one heartbeat interval.

### Why the index is taken at request time, not confirmation time

The barrier records the commit index when the read *begins*. Writes that commit
while the confirmation is in flight are not included, and do not need to be:
they began after this read began, so a linearizable history is free to order the
read before them.

## Consequences

**What this buys.** Reads are linearizable. A partitioned leader returns an error
instead of stale data, which is the honest answer — it genuinely does not know.
And no entry is appended to the log, so reads do not grow the log or pay an
fsync.

**What it costs.**

- One round trip of heartbeats per read, so read latency is at least one network
  round trip rather than a memory lookup.
- Reads are leader-only. Followers cannot serve them, so read throughput does not
  scale with cluster size. (The paper's follower-read extension — a follower asks
  the leader for a read index and then serves locally — is the fix, and is not
  implemented here.)
- Currently one confirmation round *per read*. A read-heavy workload should batch
  them: a single quorum acknowledgement confirms leadership for every barrier
  recorded before it. Noted in the code; worth doing when a measurement asks.

**Testing.** `TestPartitionedLeaderCannotConfirmARead` in the core and
`TestPartitionedLeaderRefusesReads` over a real network both construct the
scenario above and assert the read is refused. The simulator exercises it
continuously, since partitions are one of its standard faults.

## Alternatives considered

**Naive leader read.** Rejected: the failure above, silently.

**Leader lease.** The leader assumes it is still leader for a bounded period
after its last successful heartbeat round, and serves reads from memory with no
round trip. Much faster, and used in production systems.

Rejected here because it trades a safety property for latency, and pays for it
with an assumption about clocks. It is only correct if no node's clock drifts
faster than the lease margin — which is usually true, and is not guaranteed by
anything in the protocol. A VM paused by its hypervisor for longer than the lease
breaks it, and the resulting stale read looks exactly like the failure this ADR
is about. Read-index needs no clock assumption at all.

Worth revisiting if read latency is ever measured to be the binding constraint,
and worth being explicit about the assumption if it is.

**Route every read through the log as a normal entry.** Trivially correct: the
read is ordered with the writes by the same mechanism. Rejected because it makes
every read an append and an fsync — about 8ms each, measured in ADR-0002 — and
grows the log without bound in proportion to read traffic.

**Serve reads from any follower.** Rejected outright: a follower's state machine
can be arbitrarily far behind, with no bound and no way for the client to know.
Offered as an explicit opt-in (`READ_CONSISTENCY_STALE`) so that callers who
genuinely do not care can say so, rather than getting it by accident.
