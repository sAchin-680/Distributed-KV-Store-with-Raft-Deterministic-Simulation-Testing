# The platform: the controller that deploys, and the stack that watches.
#
# Applied once. What each environment runs lives in environments/, each with its
# own state, so a staging deployment cannot touch the control plane or
# production.
#
# This is the first layer to use the remote backend, because it is the first
# layer that can — the Secret and Lease it stores state in live in the cluster
# that bootstrap/ created.

terraform {
  required_version = ">= 1.5"

  backend "kubernetes" {
    secret_suffix = "platform"
    namespace     = "terraform-state"
    config_path   = "~/.kube/config"
  }

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "~> 1.14"
    }
  }
}

provider "kubectl" {
  config_path      = var.kubeconfig_path
  config_context   = var.cluster_context
  load_config_file = true
}

provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.cluster_context
}

provider "helm" {
  kubernetes {
    config_path    = var.kubeconfig_path
    config_context = var.cluster_context
  }

  # Terraform keeps its own chart repository config and cache, rather than
  # reading the one in the operator's home directory.
  #
  # Not tidiness. The provider walks every repository configured globally and
  # fails the whole apply if any one of them has no cached index — including
  # repositories this configuration never references. A stale or mistyped entry
  # somebody added months ago then breaks an unrelated deployment, with an error
  # naming a chart that appears nowhere in this code. Isolating it means an
  # apply depends only on what is in this repository.
  repository_config_path = "${path.module}/.helm/repositories.yaml"
  repository_cache       = "${path.module}/.helm/cache"
}

module "gitops" {
  source = "../modules/gitops"

  repo_url = var.repo_url

  # A project must name every destination it permits up front, because it is
  # the thing that bounds them. The Applications themselves are declared in
  # environments/, each with its own state.
  environments = [
    { name = "raftkv-staging", namespace = "raftkv-staging" },
    { name = "raftkv-production", namespace = "raftkv-production" },
  ]
}

module "observability" {
  source = "../modules/observability"

  manifests_path = "${path.module}/../../deploy/monitoring"
}
