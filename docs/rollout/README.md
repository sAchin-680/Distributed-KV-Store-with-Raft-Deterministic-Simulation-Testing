# Rolling upgrade under load

Every node in a five-node cluster was replaced while eight clients drove reads, writes and deletes against it without pausing. The question is what the clients saw.

Reproduce with `make rollout` against a running cluster, or `./deploy/rollout.sh run`.

## Result

```
138129 operations over 2m0.003s (69106 get, 58631 put, 10392 delete) across 5 keys
138113 completed, 16 outcome unknown
linearizable — checked in 156ms
```

Five pods replaced, one at a time, in 161 seconds. **16 operations out of 138,129 did not return an answer — 0.0116%.** Nothing failed a linearizability check, and the cluster came back with every node at the same term, commit index and applied index.

## The 16

Not zero, and it is worth being exact about why rather than rounding it away.

An operation is recorded as *unknown* when the client never learned its outcome — the connection went away before an answer came back. It is deliberately not recorded as a failure, because a write whose answer was lost may still have committed. That distinction is the whole reason the checker can be trusted here: it places those 16 operations wherever a valid linearization needs them, including "it took effect", rather than dropping them and checking a history with holes in it.

Sixteen across five pod replacements is roughly three per replacement, and that is the expected shape. When the pod holding leadership is terminated, the operations already in flight on it are exactly what is lost — the client sent them, the leader went away, and no answer is coming. The client retries against the next endpoint and continues; what it cannot do is find out what happened to the request already on the wire.

So the unavailability window is bounded by a single leader election, which is 1–2 seconds by construction (`tick` × `electionTicks`, randomised up to double). At roughly 1,150 operations per second, a couple of seconds of election would cost thousands of operations if the cluster genuinely stopped serving. It cost 16, because it does not stop: the other four nodes keep a majority throughout, a new leader is elected from among them, and only the requests physically in flight on the departing node are lost.

## Why it holds

Three mechanisms, each of which can be removed to break it.

**The StatefulSet replaces one pod at a time, in descending ordinal order, waiting for each to report Ready before touching the next.** Readiness here requires a known leader, so that wait is load-bearing rather than cosmetic — it is what stops the next node going down before the cluster has re-formed around the last one. Four of five nodes are up at every instant, which is a majority.

**Identity survives the restart.** `kv-raftkv-3` is node 4 before and after, with the same disk. It rejoins as the voter the configuration already expects, catches up from the leader, and counts toward quorum again. A returning node that came back as a *new* node would leave the old one permanently unreachable in the configuration, and five such restarts would put the cluster below quorum against members that no longer exist. This is why it is a StatefulSet and not a Deployment — see [ADR-0004](../adr/0004-statefulset-not-deployment.md).

**The client follows the leader.** A write to a follower is refused with a hint naming the leader, and the client library retries there. The workload addresses all five pods individually rather than through the load-balanced Service, so when one refuses, there is somewhere else to go immediately.

The PodDisruptionBudget does not show up in this run, because nothing else was competing for the nodes. It matters when something is: the StatefulSet's one-at-a-time behaviour protects against *itself*, not against a concurrent node drain, and `minAvailable: 3` is what stops an upgrade and a drain together taking the cluster below a majority.

## Reading the report

`rollout-<timestamp>.log` records the pod UIDs before and after. The names are identical on both sides — that is the point of a StatefulSet — so the UID is the evidence that anything was actually replaced.

The workload runs inside the cluster rather than through a port-forward. A port-forward terminates when its target pod does, and the resulting errors would be the harness failing rather than the store, which would make the error rate meaningless.

## What this does not show

The restart replaces each pod with an identical one. That isolates the mechanism being measured: a real version bump also changes the binary, and a failure would then be ambiguous between "rolling is unsafe" and "the new binary is broken". Kubernetes treats the two identically, so this is the honest test of the rollout, and says nothing about compatibility between two different versions of the code.

It is also a single run on a local kind cluster with no competing load and no injected faults. The [chaos runs](../chaos-runs/) cover a damaged network; the [simulator](../../sim/) covers the algorithm under interleavings neither of these can reach.
