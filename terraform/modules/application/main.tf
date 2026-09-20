# One ArgoCD Application: what a single environment should be running.
#
# Deliberately separate from the module that installs ArgoCD. The controller is
# installed once and changes rarely; what each environment runs changes on every
# deployment. Keeping them in one module would mean an apply that touches the
# control plane every time an environment moved, and a single state file whose
# loss took both with it.
#
# Split, each environment has its own state, and applying staging cannot reach
# production.

terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

resource "kubernetes_manifest" "application" {
  manifest = {
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = var.name
      namespace = var.argocd_namespace

      # Without this, deleting the Application orphans everything it created
      # and leaves a cluster nothing is managing.
      #
      # Note the ordering trap. The finalizer needs the AppProject to still
      # exist in order to work out what it may delete, so destroying both at
      # once wedges it: the finalizer never completes and its workloads are
      # stranded. Terraform destroys in reverse dependency order and this
      # module is applied after the one holding the project, so it is removed
      # first — which is the order that works. Doing it by hand with a single
      # kubectl delete is what breaks.
      finalizers = ["resources-finalizer.argocd.argoproj.io"]
    }

    spec = {
      project = var.project

      source = {
        repoURL        = var.repo_url
        targetRevision = var.target_revision
        path           = var.chart_path
        helm = {
          releaseName = var.release_name
          valueFiles  = [var.values_file]
        }
      }

      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = var.namespace
      }

      syncPolicy = {
        automated = {
          # Revert drift without being asked. A change made by hand to a
          # consensus cluster is usually incident response, and the dangerous
          # ones are silent — someone scales the StatefulSet down to quiet an
          # alert and leaves it below quorum.
          selfHeal = var.self_heal
          # Delete resources removed from the chart, or the repository stops
          # describing what is running.
          prune = true
        }

        syncOptions = [
          "CreateNamespace=true",
          # volumeClaimTemplates are immutable, so a chart change that touches
          # them fails loudly rather than silently doing nothing.
          "ApplyOutOfSyncOnly=true",
        ]

        retry = {
          limit   = 5
          backoff = { duration = "5s", factor = 2, maxDuration = "3m" }
        }
      }

      revisionHistoryLimit = 10
    }
  }
}
