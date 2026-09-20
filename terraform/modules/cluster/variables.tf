variable "name" {
  description = "Cluster name. Also the kind context name, as kind-<name>."
  type        = string

  validation {
    # kind builds container and network names from this, and Docker rejects
    # anything with uppercase or underscores. Caught here rather than as a
    # confusing failure several minutes into an apply.
    condition     = can(regex("^[a-z0-9][a-z0-9-]*$", var.name))
    error_message = "Cluster name must be lowercase alphanumeric with hyphens."
  }
}

variable "worker_count" {
  description = <<-DESC
    Worker nodes, excluding the control plane.

    Two is the practical minimum here. The Raft nodes are spread with soft
    anti-affinity, so fewer machines than that means a majority of the cluster
    can land on one host and a single machine failure takes quorum with it —
    which is the failure the whole arrangement exists to survive.
  DESC
  type        = number
  default     = 2

  validation {
    condition     = var.worker_count >= 2
    error_message = "At least two workers, or a single host can hold a majority of the Raft cluster."
  }
}

variable "kubeconfig_path" {
  description = "Where to write the kubeconfig for this cluster."
  type        = string
}
