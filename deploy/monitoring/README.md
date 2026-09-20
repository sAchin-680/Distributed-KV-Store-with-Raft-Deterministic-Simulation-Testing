# Monitoring

Prometheus scraping every node individually, alert rules for the ways a consensus cluster actually fails, an Alertmanager routing them, and a Grafana dashboard provisioned from a file in this repository.

```bash
make monitoring-up       # install alongside a running cluster
make monitoring-open     # Grafana on :3000, Prometheus on :9090, Alertmanager on :9093
make monitoring-demo     # break quorum, watch the alert fire, heal, watch it clear
make monitoring-down
```

## The alerts were fired on purpose

An alert rule that has never fired is an assertion, not a safeguard. The expression may not match the metric names, the threshold may be unreachable, the routing may silently drop it — and none of that surfaces until an incident, which is the worst possible time to discover it.

So `monitoring.sh demo` takes quorum away and checks the alert arrives at Alertmanager. Measured against the live five-node cluster:

```
17:27:45  cluster has 5 nodes, quorum is 3
17:27:45  scaling down to 2 — one short of a majority
17:28:15  alerts firing: RaftKVBelowQuorum RaftKVNoLeader
17:28:37  healing: scaling back to 5
17:28:51  alerts cleared; the cluster recovered on its own
```

**Both critical alerts fired within 30 seconds of quorum loss and cleared 10 seconds after recovery**, without intervention.

There is a detail in that demo worth not skipping past. Scaling the StatefulSet from five to two does *not* remove anything from the Raft configuration — membership lives in the replicated log and only changes through joint consensus. The cluster still believed it had five voters, so the two survivors could not assemble a majority and correctly refused to serve. Had the three nodes been removed properly with `kvctl members`, two would have been a legitimate cluster and nothing would have fired. The alert is measuring the right thing: a majority of the *configuration*, not a count of running pods.

## Every rule is evaluated per environment

One Prometheus scrapes both staging and production, and that makes the obvious expression wrong in a way that is silent.

`max(raftkv_raft_has_leader) == 0` asks whether *any node anywhere* has a leader. A perfectly healthy production holds it at 1, so staging could be completely down — no leader, nothing committing — and not a single alert would fire. Grouping `by (namespace)` asks the question once per cluster, which is the only version of it that means anything.

Verified by breaking staging while production ran:

```
every alert that fired, with its namespace:
  RaftKVBelowQuorum   namespace=raftkv-staging   severity=critical
  RaftKVNoLeader      namespace=raftkv-staging   severity=critical

production pods running: 5     (never implicated)
```

The quorum threshold is derived per environment too, from each cluster's own voter count — staging needs 2 of 3, production 3 of 5 — so neither is hardcoded and both survive a membership change.

## Self-heal and this demo want opposite things

On a GitOps-managed cluster the alert demo cannot work as written, and the reason is worth understanding rather than working around.

ArgoCD puts a hand-scaled cluster back in about a second. The below-quorum alert deliberately waits fifteen, because a leaderless instant is normal. So the drift is gone long before the alert could fire, and the alert can never be exercised while reconciliation runs.

Both behaviours are correct. The alert still has to be tested, because **self-heal only repairs drift from the repository** — it does nothing about the failures the alert actually exists for. A partition, a lost node, a disk that stopped answering: the manifests still match, there is no drift, and ArgoCD has no opinion. Those are exactly the cases where the alert is the only thing that notices.

So `monitoring.sh demo` pauses reconciliation for the duration and restores it afterwards, including on a ctrl-c partway through.

## Why the rules are written this way

**`RaftKVNoLeader` is the one that matters**, and it is expressed as "no node can name a leader" rather than as a count of running pods, because those are different questions. Five pods can be `Running` and perfectly healthy while a partition leaves none of them able to reach a majority — every pod fine, the cluster down. Kubernetes cannot see the difference; this metric can.

It waits 15 seconds, not zero. A leaderless instant is *normal* — every election has one — and an alert that fires on each routine failover trains people to ignore it. Fifteen seconds is roughly ten election timeouts, which distinguishes "failing to elect" from "in the middle of electing".

**`RaftKVBelowQuorum`** reads the voter count from the cluster rather than hardcoding five, so it stays correct across membership changes.

**`RaftKVLeaderFlapping` took two corrections, and both were bugs before they were decisions.**

First the aggregator. Every node observes the same sequence of leadership changes, so the per-node counters are N redundant views of one sequence rather than N independent events. Summing them multiplied each change by however many nodes saw it — on five nodes a *single* leadership change scored 5 and tripped a threshold of 4. It also made the value depend on how many pods happened to be reporting, so identical behaviour alerted differently after a scale-up. That is why it fired spuriously. Now `max`.

Then the threshold, which is now the cluster's own size rather than a constant. That is derived, not chosen: a rolling upgrade replaces every pod one at a time, and each replacement costs **at most one** leadership change, because only the pod that currently leads causes an election. A full rollout of an N-node cluster therefore cannot exceed N changes, and any fixed number below N fires on a routine upgrade.

Measured against a real five-node rollout, sampling every 15 seconds:

```
  21:52:08  ready=5/5  prod=0.0
  21:52:23  ready=4/5  prod=0.0      one pod down at a time, never below quorum
  21:53:23  ready=4/5  prod=1.1
  21:54:54  ready=5/5  prod=1.0      all five replaced
```

**One leadership change for a complete five-node rollout.** The bound is 5, the observed cost is 1, and the old constant of 4 sat between them — it would not have fired on this rollout and would have fired on an unlucky one. Reading the rule aloud now: *more leadership changes in ten minutes than there are nodes to be leader.* That is not an upgrade; it is a cluster that cannot keep one.

**`RaftKVNodeDown` is a warning, not a page.** One node down on a five-node cluster is survivable by construction. It becomes urgent only when a second follows, and that is what the quorum alert is for.

**`RaftKVScrapeFailing` alerts on the monitoring, not the cluster.** If the exporter cannot read a node's state, the node may be perfectly healthy. Keeping these distinct matters, because treating a broken exporter as a broken cluster is exactly how a healthy node gets restarted during an incident.

**`RaftKVStuckInJointConfiguration`** catches a membership change that never completed. The joint state requires majorities of *both* the old and new voter sets, so a cluster stuck there is strictly more fragile than it would be in either configuration alone — and nothing else would notice.

Alertmanager inhibits `RaftKVNodeDown` when `RaftKVBelowQuorum` is already firing, and groups by alert name rather than by pod: during a rolling upgrade every pod goes down in turn, and per-pod grouping would send five notifications for one expected event.

## Scraping

Pod discovery, not the Service. The client Service load-balances, so scraping it would return one arbitrary node per scrape and the series would flip between nodes with nothing to distinguish them. Consensus metrics are only meaningful per node — *what does this node believe the term is* — so every pod is scraped individually and keeps its pod name as a label. The pod name is stable across replacement, which is what makes a dashboard survive a rolling upgrade.

The scrape interval is 5s. Failover takes 1–2s, so a conventional 60s interval would miss most elections entirely and the leader-change counter would be the only evidence any had happened.

## The dashboard

`dashboards/raftkv.json` is a file, mounted into Grafana by the install script. A dashboard that exists only in Grafana's database is lost when the pod is replaced and cannot be reviewed in a pull request.

Three rows, in the order you would actually ask the questions:

- **Can the cluster commit?** — leader, nodes up against quorum, term, and a plot of how many nodes can name a leader. The gaps in that plot are elections, and their width is the unavailability window a client sees. This is plotted rather than summarised because a counter tells you an election happened, not what it cost.
- **Replication** — commit throughput, per-follower replication lag, and commit index minus applied index. Only the leader reports per-peer lag, so a follower shows no series rather than zero; zero would read as "caught up" instead of "not the leader".
- **Stability** — elections started against leadership changes. Elections rising while leadership changes stay flat means nodes are repeatedly campaigning without winning, which is what a partition looks like from the inside.

Grafana runs with anonymous viewer access because this is a local cluster reached through a port-forward. A real deployment puts an identity provider in front; no credentials are shipped in these manifests, and Alertmanager's receivers are deliberately empty for the same reason — wiring a real destination means a credential, which belongs in a Secret managed outside version control.
