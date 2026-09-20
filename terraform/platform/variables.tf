variable "kubeconfig_path" {
  description = "Path to the kubeconfig written by the bootstrap layer."
  type        = string
  default     = "~/.kube/config"
}

variable "cluster_context" {
  description = "kubeconfig context to act against."
  type        = string
  default     = "kind-raftkv"
}

variable "repo_url" {
  description = "Repository ArgoCD is permitted to deploy from."
  type        = string
  default     = "https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing.git"
}
