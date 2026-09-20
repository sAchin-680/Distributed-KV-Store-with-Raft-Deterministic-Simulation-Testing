variable "namespace" {
  description = "Namespace for the ArgoCD control plane."
  type        = string
  default     = "argocd"
}

variable "chart_version" {
  description = <<-DESC
    Pinned, not "latest". A floating chart version makes the controller that
    deploys everything else the one component whose version is unknowable — and
    it is the component that would silently change what every other one runs.
  DESC
  type        = string
  default     = "7.7.5"
}

variable "project_name" {
  description = "AppProject that bounds what the Applications may create."
  type        = string
  default     = "raftkv"
}

variable "repo_url" {
  description = "Repository the Applications track."
  type        = string
}



variable "environments" {
  description = <<-DESC
    Namespaces the project is allowed to deploy into.

    The Applications themselves are declared per environment, each with its own
    state, so that applying staging cannot touch production. This module only
    needs to know which destinations the project should permit — a project has
    to name them all up front, because it is the thing that bounds them.
  DESC
  type = list(object({
    name      = string
    namespace = string
  }))

  validation {
    condition     = length(var.environments) > 0
    error_message = "At least one environment is required."
  }
}

variable "reconciliation_interval" {
  description = <<-DESC
    How often the tracked repository is polled for new commits.

    This does not govern drift correction. A change to a live resource is seen
    by a watch and corrected in about a second regardless; this only bounds how
    long a new commit waits before being noticed.
  DESC
  type        = string
  default     = "60s"
}

variable "insecure_server" {
  description = <<-DESC
    Serve the UI over plain HTTP.

    True for a local cluster reached through a port-forward, where the
    alternative is a self-signed certificate that every client has to be told
    to ignore. A real deployment terminates TLS at an ingress and leaves this
    false.
  DESC
  type        = bool
  default     = true
}
