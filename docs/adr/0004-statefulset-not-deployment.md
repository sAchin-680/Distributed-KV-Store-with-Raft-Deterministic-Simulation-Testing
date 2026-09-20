# Run the cluster as a StatefulSet, not a Deployment

Status: accepted

## Context

Raft nodes are not interchangeable. A Deployment assumes they are, and every one of its conveniences turns into a correctness problem here.

Three things about a Raft node are durable facts rather than runtime details. It has an **identity** — a node ID that appears in the replicated configuration, and that every peer votes for and replicates to by name. It has an **address** other nodes must reach it at individually, because `AppendEntries` goes to a specific follower and "whichever pod answers" is meaningless. And it has **state on disk** — the log and the term and vote it persisted before answering anything — which is the only reason it may participate again after a restart.

A Deployment guarantees none of these. Its pods get random name suffixes, so identity would have to come from somewhere else. Its pods are behind a load-balanced Service, so they have no individual addresses. Its pods share one volume claim or none, so a restarted pod comes back either empty or fighting another pod for the same disk.

## Decision

The cluster runs as a StatefulSet with a headless Service and per-pod volume claims.

The node ID is derived from the pod ordinal in the container's start command: `ORDINAL="${HOSTNAME##*-}"; exec raftd --id="$((ORDINAL + 1))"`. Pod `kv-raftkv-3` is always node 4 and always will be, across restarts, reschedules and full cluster recreation. The peer list is generated the same way, so it is identical on every pod without any coordination step.

## What breaks with a Deployment

Each of these is a distinct failure, and they compound.

**Identity is lost on every restart.** A pod named `raftkv-7d9f8b-x4k2p` has no stable ID to derive. Generating one from the hostname means a restarted node rejoins as a *different* node — so a five-node cluster that restarts twice has, as far as the replicated configuration is concerned, up to fifteen members, of which ten are permanently unreachable. Quorum is a majority of the configuration, not of the running pods, so the cluster stops committing and cannot recover without manual surgery on the member list.

**Peers cannot be addressed.** A Deployment's Service is a single virtual IP that load-balances. A leader sending `AppendEntries` to that IP reaches an arbitrary follower, so the per-follower `nextIndex` and `matchIndex` the leader maintains — the entire replication state machine — is tracking a peer that does not exist. Replication silently converges to nonsense. The headless Service exists precisely to turn off load balancing: `clusterIP: None` makes a lookup return the pods' own addresses.

**Persisted state is lost or corrupted.** Raft's safety argument rests on durability: a node that voted in term 5 must never vote differently in term 5, and a node that acknowledged an entry must still have it. Both facts live on disk. With no volume, a restarted pod votes twice in the same term and two leaders can be elected — the split-brain the term-and-vote persistence exists to prevent. With a shared `ReadWriteMany` volume, two pods open the same bbolt file and corrupt it. `volumeClaimTemplates` gives each ordinal its own disk, bound to that ordinal forever.

**Restarts are unbounded.** A Deployment's rolling update honours `maxUnavailable`, which is a *percentage of replicas* and defaults to 25%. That says nothing about quorum. Worse, a Deployment will start new pods before old ones terminate, so a five-node cluster can briefly have eight pods, of which three hold no data. The StatefulSet replaces pods one at a time in descending ordinal order, waiting for each to report Ready before touching the next, and readiness here requires a known leader — so the wait is load-bearing rather than cosmetic.

## Consequences

Two settings that look like defaults to override are required, and both are for the same reason.

`podManagementPolicy: Parallel` is not an optimisation. The default, `OrderedReady`, starts pod 0 and waits for it to become Ready before starting pod 1 — but pod 0 cannot become Ready until the cluster elects a leader, and a leader needs a majority, which needs pods 1 and 2. The cluster deadlocks at boot and never starts. Parallel startup is what breaks the cycle.

`publishNotReadyAddresses: true` on the headless Service is required for the same cycle at the DNS layer: peers must resolve before a pod is Ready, and a pod is not Ready until there is a leader. Without it, no pod can find any other and the cluster cannot bootstrap.

The cost is that scaling is no longer automatic. Changing `replicaCount` adds or removes a pod, but the Raft configuration lives in the replicated log and only changes through joint consensus. Scaling out is therefore two steps — scale the StatefulSet so the pod exists and is reachable, then run `kvctl members add` so it counts toward quorum — and scaling in is the same in reverse, removing the member *before* deleting the pod. This is documented in `values.yaml` next to `replicaCount`, because the sequence is easy to get backwards and getting it backwards removes a voter the cluster still expects to hear from.

The PodDisruptionBudget carries the rest of the weight. `minAvailable` is set to quorum, `replicas/2 + 1`, which is what stops a node drain, an upgrade and a rolling restart from *together* taking the cluster below a majority. The StatefulSet's own one-at-a-time behaviour does not protect against a concurrent drain; the budget does.

See `deploy/rollout.sh` and `docs/rollout/` for the measured result of a rolling restart under a live client workload.
