output "cluster_name" {
  value       = module.cluster.name
  description = "Name of the created cluster."
}

output "cluster_context" {
  value       = module.cluster.context
  description = "kubeconfig context for the cluster."
}

output "state_namespace" {
  value       = kubernetes_namespace.state.metadata[0].name
  description = "Namespace the remote state is stored in."
}

output "backend_configuration" {
  description = "What the layer above puts in its backend block."
  value = {
    secret_suffix = "state"
    namespace     = kubernetes_namespace.state.metadata[0].name
    config_path   = var.kubeconfig_path
  }
}
