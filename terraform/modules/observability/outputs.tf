output "namespace" {
  description = "Namespace the monitoring stack runs in."
  value       = kubernetes_namespace.monitoring.metadata[0].name
}

output "port_forward_commands" {
  description = "How to reach each component."
  value = {
    grafana      = "kubectl -n ${var.namespace} port-forward svc/grafana 3000:3000"
    prometheus   = "kubectl -n ${var.namespace} port-forward svc/prometheus 9090:9090"
    alertmanager = "kubectl -n ${var.namespace} port-forward svc/alertmanager 9093:9093"
  }
}
