output "name" {
  description = "Application name."
  value       = kubernetes_manifest.application.manifest.metadata.name
}

output "namespace" {
  description = "Namespace the workload is deployed into."
  value       = var.namespace
}
