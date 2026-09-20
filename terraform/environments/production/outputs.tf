output "application" {
  description = "Application reconciling this environment."
  value       = module.application.name
}

output "namespace" {
  description = "Namespace the workload runs in."
  value       = module.application.namespace
}
