# Prometheus, Alertmanager and Grafana.
#
# The manifests are the ones in deploy/monitoring, applied by Terraform rather
# than by a shell script. That is the only change: the alert rules, the scrape
# configuration and the dashboard stay files in the repository, reviewable in a
# pull request, and this module is responsible for their existence rather than
# their content.

terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = var.namespace
  }
}

# The dashboard is a file, not a string inside a manifest. A dashboard that
# exists only in Grafana's database vanishes with the pod and cannot be
# reviewed; this one is JSON in the repository that happens to be mounted.
resource "kubernetes_config_map" "dashboards" {
  metadata {
    name      = "grafana-dashboards"
    namespace = kubernetes_namespace.monitoring.metadata[0].name
  }

  data = {
    for f in fileset("${var.manifests_path}/dashboards", "*.json") :
    f => file("${var.manifests_path}/dashboards/${f}")
  }
}

# The stack itself, split into the documents each file contains.
#
# kubernetes_manifest resolves the API schema at plan time, which means the
# cluster has to exist and serve the types before a plan can even be produced.
# That is fine here and would not be everywhere: every resource in these files
# is a built-in kind, and this module only ever runs after bootstrap/ has
# created the cluster. The same approach applied to a CRD in the run that
# installs it fails, which is why the AppProject is rendered by Helm instead.
resource "kubernetes_manifest" "stack" {
  for_each = {
    for doc in local.documents :
    "${doc.kind}/${try(doc.metadata.name, "unnamed")}" => doc
  }

  manifest = each.value

  depends_on = [
    kubernetes_namespace.monitoring,
    kubernetes_config_map.dashboards,
  ]
}

locals {
  files = [for f in var.manifest_files : "${var.manifests_path}/${f}"]

  # Split each file on its document separator, drop anything empty or a pure
  # comment block, and decode the rest.
  documents = flatten([
    for path in local.files : [
      for doc in split("\n---\n", file(path)) :
      yamldecode(doc)
      if length(trimspace(replace(doc, "/(?m)^#.*$/", ""))) > 0
    ]
  ])
}
