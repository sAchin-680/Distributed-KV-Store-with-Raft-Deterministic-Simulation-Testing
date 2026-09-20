output "argocd_namespace" {
  description = "Namespace the ArgoCD control plane runs in."
  value       = module.gitops.namespace
}

output "argocd_project" {
  description = "AppProject the environments deploy under."
  value       = module.gitops.project
}

output "monitoring_namespace" {
  description = "Namespace the monitoring stack runs in."
  value       = module.observability.namespace
}

output "reach_it" {
  description = "How to reach each component."
  value = merge(
    module.observability.port_forward_commands,
    { argocd_password = module.gitops.admin_password_command },
  )
}
