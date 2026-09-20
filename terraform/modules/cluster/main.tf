# The Kubernetes cluster itself.
#
# kind rather than a cloud provider, and the reason is in the top-level README:
# everything in this repository has been applied, and a module that was only
# ever type-checked would be the single exception.
#
# What transfers to a cloud module is the shape — a cluster with a control
# plane and a set of workers, variables for the things that differ between
# environments, outputs that the layer above consumes without knowing how the
# cluster was made.

terraform {
  required_version = ">= 1.5"
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.8"
    }
  }
}

resource "kind_cluster" "this" {
  name = var.name

  # Terraform writes the kubeconfig where the providers above can find it,
  # rather than merging into the caller's ~/.kube/config. A module that edits
  # a file outside its own state is a module that cannot be destroyed cleanly.
  kubeconfig_path = var.kubeconfig_path

  # Blocks until every node reports Ready. Without it the apply returns while
  # the API server is still coming up, and the next module in the same run
  # fails against a cluster that exists but cannot answer.
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      # The control plane is tainted against ordinary workloads by default in
      # most clusters; kind does not taint it, and this deliberately leaves it
      # that way. Three machines is already the minimum for the anti-affinity
      # rules to place five Raft nodes without piling a majority onto one host.
      kubeadm_config_patches = [
        <<-PATCH
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
        PATCH
      ]
    }

    dynamic "node" {
      for_each = range(var.worker_count)
      content {
        role = "worker"
      }
    }
  }
}
