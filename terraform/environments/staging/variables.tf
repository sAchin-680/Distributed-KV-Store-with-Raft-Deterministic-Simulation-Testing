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
  description = "Repository ArgoCD tracks."
  type        = string
  default     = "https://github.com/sAchin-680/Distributed-KV-Store-with-Raft-Deterministic-Simulation-Testing.git"
}

variable "target_revision" {
  description = "Branch or tag ArgoCD follows for this environment."
  type        = string
  default     = "main"
}

variable "self_heal" {
  description = "Whether ArgoCD corrects drift here without being asked."
  type        = bool
  default     = true
}
