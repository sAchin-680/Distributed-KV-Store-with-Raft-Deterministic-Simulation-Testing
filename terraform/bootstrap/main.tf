# Bootstrap: the cluster, and the backend every other configuration stores its
# state in.
#
# This layer uses LOCAL state, and that is the point rather than an oversight.
#
# State has to be stored somewhere before Terraform can store state. On AWS the
# answer is an S3 bucket and a lock table that somebody created first — usually
# by hand, usually years ago, usually undocumented. The same ordering problem
# exists here and is made explicit instead: the backend is a Secret inside the
# cluster, so the cluster cannot be tracked in it.
#
# What makes that acceptable is that this layer is disposable. It creates a
# cluster and a namespace and nothing else; losing its state file costs a
# `kind delete cluster` and a re-apply. Everything with state worth protecting
# lives in the layer above, in the backend this creates.

terraform {
  required_version = ">= 1.5"

  backend "local" {
    path = "terraform.tfstate"
  }

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.8"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

module "cluster" {
  source = "../modules/cluster"

  name            = var.cluster_name
  worker_count    = var.worker_count
  kubeconfig_path = var.kubeconfig_path
}

provider "kubernetes" {
  host           = module.cluster.endpoint
  config_path    = module.cluster.kubeconfig_path
  config_context = module.cluster.context
}

# The namespace the remote state lives in.
#
# Separate from every workload namespace on purpose. State is the one object
# whose loss cannot be recovered by re-running anything, so it does not share a
# namespace with things that get deleted during a demo.
resource "kubernetes_namespace" "state" {
  metadata {
    name = var.state_namespace

    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
      "raftkv.io/purpose"            = "terraform-state"
    }
  }

  depends_on = [module.cluster]
}

# A service account the state backend authenticates as, with permission to read
# and write exactly the Secrets and Leases it needs — not cluster-admin, which
# is what a backend usually ends up with by default.
resource "kubernetes_service_account" "terraform" {
  metadata {
    name      = "terraform"
    namespace = kubernetes_namespace.state.metadata[0].name
  }
}

resource "kubernetes_role" "state" {
  metadata {
    name      = "terraform-state"
    namespace = kubernetes_namespace.state.metadata[0].name
  }

  # Secrets hold the state. Leases are the lock: the backend takes one for the
  # duration of an apply, which is what makes a second apply wait rather than
  # write over the first.
  rule {
    api_groups = [""]
    resources  = ["secrets"]
    verbs      = ["get", "list", "create", "update", "delete"]
  }

  rule {
    api_groups = ["coordination.k8s.io"]
    resources  = ["leases"]
    verbs      = ["get", "create", "update", "delete"]
  }
}

resource "kubernetes_role_binding" "state" {
  metadata {
    name      = "terraform-state"
    namespace = kubernetes_namespace.state.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.state.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.terraform.metadata[0].name
    namespace = kubernetes_namespace.state.metadata[0].name
  }
}
