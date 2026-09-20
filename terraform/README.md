# Infrastructure

Terraform provisions the platform this cluster runs on: the Kubernetes cluster itself, the GitOps controller that deploys to it, and the monitoring that watches it.

```bash
make tf-bootstrap    # the cluster, and the backend everything else stores state in
make tf-platform     # ArgoCD and the monitoring stack
make tf-staging      # what staging runs
make tf-production   # what production runs
make tf-lock-demo    # show the lock refusing concurrent operations
make tf-destroy
```

Four layers, applied in that order. Each is a separate state, so applying
staging cannot reach production and neither can touch the control plane.

## Why there are two layers

`bootstrap/` uses **local state**; everything else uses a **remote backend that lives inside the cluster**. That is not an inconsistency, it is the ordering problem that every real setup has and usually hides.

State has to be stored somewhere before Terraform can store state. On AWS the answer is an S3 bucket and a lock table that somebody created first — often by hand, often years ago, often undocumented. Here the backend is a Kubernetes Secret with a Lease for locking, so the thing holding the state lives in the cluster that `bootstrap/` creates. The cluster therefore cannot be tracked in it.

Making that explicit is the point. `bootstrap/` is small, rarely run, and its state file is disposable — the cluster can be recreated from the config in seconds. Everything with interesting state is in the layer above.

## Remote state and locking are real

The `kubernetes` backend stores state in a Secret and takes a `coordination.k8s.io/Lease` for the duration of an apply. That is genuine distributed locking, not a local file with a `.lock` next to it: a second `terraform apply` blocks until the first releases, including from another machine.

Demonstrated rather than claimed. `make tf-lock-demo` starts four plans at once:

```
  plan 1  acquired the lock and ran
  plan 2  refused by the lock
  plan 3  refused by the lock
  plan 4  refused by the lock

1 ran, 3 refused
```

What a refused run reports is the Lease being contended, not a file that happened to be present:

```
Error acquiring the state lock
Operation cannot be fulfilled on leases.coordination.k8s.io
"lock-tfstate-default-staging": the object has been modified
```

The demo fails if anything other than exactly one run acquires the lock. Zero means nothing was tested; more than one means the lock is not working, which is the failure it exists to catch.

## Why not AWS

Nothing here needs a cloud account, and that is deliberate. An EKS module that was written but never applied would be the only thing in this repository without a measured result behind it. Every `apply` documented here was run.

The trade is AWS-specific resource names. The structure — modules with variables and outputs, a separate bootstrap layer, remote state with locking, one configuration per environment — is the part that transfers.

## Three things this taught, by failing

**A CRD cannot be installed and used in the same plan.** `kubernetes_manifest` resolves the API schema at *plan* time, so declaring an `AppProject` in the run that installs ArgoCD fails before anything is applied. Moving it into the Helm release's `extraObjects` fails too, for a different reason: Helm builds and validates every object in a manifest before applying any of them, so it rejects the resource against a CRD it is about to install itself. The `kubectl` provider defers both to apply time, which is the only point the CRD is guaranteed to exist.

**Deleting a namespace does not delete what it created.** Removing the `argocd` and `monitoring` namespaces left behind three CRDs and nine ClusterRoles and bindings, none of which a namespace owns. Every one of them then blocked the Terraform apply with "already exists and cannot be imported into the current release", one at a time. A namespace is not a blast radius.

**An apply should not inherit the operator's Helm config.** The provider walks every chart repository configured globally and fails the whole apply if any one has no cached index — including repositories this code never references. A mistyped entry somebody added months ago broke an unrelated deployment here, with an error naming a chart that appears nowhere in this repository. Terraform now keeps its own repository config and cache.
