# GitOps

ArgoCD follows `deploy/helm/raftkv` on `main`. Nothing in the cluster is applied by hand; the desired state is a commit.

```bash
make argocd-up      # install ArgoCD, hand the release over, create the Application
make argocd-demo    # break the cluster by hand, watch it corrected
make argocd-open    # the UI, with the admin password
make argocd-down
```

## It was broken on purpose

"Self-healing" is a checkbox in a YAML file until something has drifted and been corrected. The failure modes are quiet: a project that forbids the resource, a sync policy that detects drift but never acts, an Application stuck `Progressing` because a StatefulSet never goes Ready. None of that is visible from the manifest.

So the demo does the realistic damage — scaling the cluster below quorum by hand, which looks like a reasonable thing to do during an incident and quietly leaves the cluster unable to commit anything.

```
17:51:49  the chart says 5 nodes; quorum is 3
17:51:49  scaling to 2 by hand — below quorum, exactly the mistake that hurts
17:51:49  detected as OutOfSync after 0s
17:51:51  replica count back to 5 after 2s
          final state: Synced / Healthy

17:51:54  deleting the PodDisruptionBudget by hand
17:51:56  PodDisruptionBudget restored after 2s
```

**Drift detected immediately and corrected in 2 seconds, in both directions** — a resource changed, and a resource deleted.

Scaling rather than deleting a pod, deliberately. Kubernetes already replaces a deleted pod on its own, so that would demonstrate the StatefulSet controller rather than ArgoCD. A replica count that disagrees with the repository is drift only a GitOps controller notices.

## No alert fired, and that is the result

The monitoring stack was running throughout, with `RaftKVBelowQuorum` armed. It did not fire.

That is not a gap — it is the two systems in the right relationship. The alert waits 15 seconds before firing, because a leaderless instant is normal. ArgoCD corrected the drift in 2. Nobody should be paged for something that repaired itself in less time than it takes to decide whether it is real.

It also means the numbers are consistent rather than coincidental: if self-heal had taken longer than 15 seconds, the alert would have fired and that would have been correct too. The thresholds were chosen independently and happen to compose.

## The log survived changing deployment tools

The release was installed by Helm before this, and two controllers reconciling the same resources disagree sooner or later, so ownership was handed over rather than shared: `helm uninstall`, then ArgoCD creates everything from the chart.

That deletes the StatefulSet and therefore every pod. The data survived anyway — a StatefulSet's PVCs are deliberately not deleted with it, so each node's log, term and vote were still on disk when the pods came back. Commit index before and after the handover: **344,703**. The cluster did not notice that the thing managing it had changed.

## Why an AppProject rather than `default`

An Application with `selfHeal` is an agent holding write access to the cluster. If the repository it follows is compromised, whatever it is permitted to create is what an attacker can create. The project lists three kinds — `Service`, `StatefulSet`, `PodDisruptionBudget` — and an empty cluster-scoped whitelist, so "someone edited a chart" cannot become "someone created a ClusterRoleBinding".

## Why the image tag is pinned

`image.tag: "0.1.0"`, not `latest`. A floating tag means the deployed version is whatever was last pushed to the registry — unknowable from the repository, and unrevertible by reverting a commit. Those two properties are the entire reason for this arrangement, and a floating tag quietly removes both.

## What this does not do yet

Sync is automated from `main` with no staging gate, so a merge reaches this cluster directly. The continuous-deployment work adds the part that matters for a real system: an image built and tagged by commit SHA, staging synced automatically, and production behind a manual approval — all of it gated on the fuzz suite, so a commit that breaks consensus under simulated faults never reaches a cluster at all.
