# The staging environment: one Application, its own state.
#
# Separate state from the platform and from the other environment, so applying
# this cannot touch either. The Lease that guards it is per-secret, so two
# environments can be applied concurrently while two applies of *this* one
# cannot.

terraform {
  required_version = ">= 1.5"

  backend "kubernetes" {
    secret_suffix = "staging"
    namespace     = "terraform-state"
    config_path   = "~/.kube/config"
  }

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.cluster_context
}

module "application" {
  source = "../../modules/application"

  name        = "raftkv-staging"
  namespace   = "raftkv-staging"
  values_file = "values-staging.yaml"

  repo_url        = var.repo_url
  target_revision = var.target_revision
  self_heal       = var.self_heal
}
