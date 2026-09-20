# The layer above consumes these without knowing the cluster is kind, which is
# the property that would let a cloud module be substituted here.

output "name" {
  description = "Cluster name."
  value       = kind_cluster.this.name
}

output "kubeconfig_path" {
  description = "Path to the kubeconfig for this cluster."
  value       = var.kubeconfig_path
}

output "endpoint" {
  description = "API server endpoint."
  value       = kind_cluster.this.endpoint
}

output "context" {
  description = "kubeconfig context name."
  value       = "kind-${kind_cluster.this.name}"
}
