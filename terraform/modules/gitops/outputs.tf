output "namespace" {
  description = "Namespace the ArgoCD control plane runs in."
  value       = kubernetes_namespace.argocd.metadata[0].name
}

output "project" {
  description = "AppProject bounding what the Applications may create."
  value       = var.project_name
}

output "admin_password_command" {
  description = "How to read the initial admin password."
  value = join(" ", [
    "kubectl -n ${kubernetes_namespace.argocd.metadata[0].name}",
    "get secret argocd-initial-admin-secret",
    "-o jsonpath='{.data.password}' | base64 -d",
  ])
}
