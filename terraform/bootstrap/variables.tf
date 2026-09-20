variable "cluster_name" {
  description = "Name of the cluster to create."
  type        = string
  default     = "raftkv"
}

variable "worker_count" {
  description = "Worker nodes, excluding the control plane."
  type        = number
  default     = 2
}

variable "kubeconfig_path" {
  description = "Where to write the kubeconfig."
  type        = string
  default     = "~/.kube/config"
}

variable "state_namespace" {
  description = <<-DESC
    Namespace holding the remote state.

    Separate from every workload namespace deliberately: state is the one
    object whose loss cannot be recovered by re-running something, so it does
    not sit alongside things that get deleted during a demo.
  DESC
  type        = string
  default     = "terraform-state"
}
