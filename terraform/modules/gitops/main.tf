# ArgoCD, and the Applications it reconciles.
#
# Terraform installs the controller and declares what it should watch; it does
# not deploy the application itself. That division is deliberate and is the
# whole reason both tools are here.
#
# Terraform is good at things that change rarely and have dependencies between
# them — a cluster, a controller, a set of permissions. It is poor at things
# that change on every commit, because that would mean an apply per deployment
# and a state file that churns.
#
# ArgoCD is the opposite: it watches one repository and reconciles continuously,
# which is exactly wrong for provisioning and exactly right for deployment.
#
# So Terraform provisions the thing that deploys, and stops. What each
# environment runs is a commit in the chart, not a variable in here.

terraform {
  required_version = ">= 1.5"
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

resource "kubernetes_namespace" "argocd" {
  metadata {
    name = var.namespace
  }
}

resource "helm_release" "argocd" {
  name       = "argocd"
  namespace  = kubernetes_namespace.argocd.metadata[0].name
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  version    = var.chart_version

  # Long, because this pulls several images on a cold cluster and a timeout
  # part-way through leaves a half-installed controller that the next apply has
  # to reconcile rather than create.
  timeout = 900
  wait    = true

  values = [yamlencode({
    # A single replica of everything. This is a platform for one cluster, and
    # high availability for the deployment controller is not on the critical
    # path: if ArgoCD is down, the workloads it deployed keep running and the
    # only thing lost is reconciliation.
    controller = { replicas = 1 }
    repoServer = { replicas = 1 }

    server = {
      replicas  = 1
      extraArgs = var.insecure_server ? ["--insecure"] : []
    }

    configs = {
      params = {
        # Polling interval for the tracked repository. The default is three
        # minutes; drift to a *live resource* is caught by a watch and
        # corrected in about a second regardless, so this only bounds how long
        # a new commit waits.
        "timeout.reconciliation" = var.reconciliation_interval
      }
    }
  })]
}

# The AppProject, which bounds what the Applications may create.
#
# Applied with kubectl_manifest rather than kubernetes_manifest, and the reason
# is a genuine ordering problem rather than a preference.
#
# An AppProject is a custom resource whose CRD is installed by the Helm release
# immediately above. Both of the obvious approaches fail on that:
#
#   - kubernetes_manifest resolves the API schema at *plan* time, so planning a
#     fresh cluster fails before anything is applied: the CRD does not exist yet
#     and cannot, because the release that installs it has not run.
#   - putting it in the release's extraObjects fails too, because Helm builds
#     and validates every object in the manifest before applying any of them,
#     so it rejects the AppProject against a CRD it is about to install itself.
#
# kubectl_manifest defers both parsing and validation to apply time, which is
# the only point at which the CRD is guaranteed to exist. depends_on makes that
# ordering explicit rather than incidental.
resource "kubectl_manifest" "project" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "AppProject"
    metadata = {
      name      = var.project_name
      namespace = var.namespace
    }
    spec = {
      description = "The replicated key/value store"
      sourceRepos = [var.repo_url]

      destinations = [
        for env in var.environments : {
          server    = "https://kubernetes.default.svc"
          namespace = env.namespace
        }
      ]

      # Namespace is the one cluster-scoped exception, and it is required
      # rather than convenient: the Applications set CreateNamespace, and
      # creating a namespace is a cluster-scoped operation. With an empty list
      # every sync fails with "resource :Namespace is not permitted" before a
      # single workload exists.
      clusterResourceWhitelist = [
        { group = "", kind = "Namespace" },
      ]

      # An Application with selfHeal is an agent holding write access to the
      # cluster: whatever it may create is what a compromised repository may
      # create. This is the narrowest list that works.
      namespaceResourceWhitelist = [
        { group = "", kind = "Service" },
        { group = "apps", kind = "StatefulSet" },
        { group = "policy", kind = "PodDisruptionBudget" },
      ]
    }
  })

  depends_on = [helm_release.argocd]
}
