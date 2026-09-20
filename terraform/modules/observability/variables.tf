variable "namespace" {
  description = "Namespace for the monitoring stack."
  type        = string
  default     = "monitoring"
}

variable "manifests_path" {
  description = "Directory holding the monitoring manifests and dashboards."
  type        = string
}

variable "manifest_files" {
  description = <<-DESC
    Manifests to apply, in order.

    Prometheus first because it owns the RBAC the others assume, then
    Alertmanager so it exists before Prometheus tries to route to it, then
    Grafana which only reads.
  DESC
  type        = list(string)
  default     = ["prometheus.yaml", "alertmanager.yaml", "grafana.yaml"]
}
