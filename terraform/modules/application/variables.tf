variable "name" {
  description = "Application name."
  type        = string
}

variable "namespace" {
  description = "Namespace the workload is deployed into."
  type        = string
}

variable "argocd_namespace" {
  description = "Namespace the ArgoCD control plane runs in."
  type        = string
  default     = "argocd"
}

variable "project" {
  description = "AppProject that bounds what this Application may create."
  type        = string
  default     = "raftkv"
}

variable "repo_url" {
  description = "Repository to track."
  type        = string
}

variable "target_revision" {
  description = "Branch or tag to follow."
  type        = string
  default     = "main"
}

variable "chart_path" {
  description = "Path to the chart within the repository."
  type        = string
  default     = "deploy/helm/raftkv"
}

variable "values_file" {
  description = <<-DESC
    Values file for this environment, relative to the chart.

    The chart is the same for every environment. If staging rendered different
    manifests it would stop being a rehearsal, and the bugs it exists to catch
    are exactly the ones that only appear on a real cluster.
  DESC
  type        = string
}

variable "release_name" {
  description = "Helm release name used when rendering the chart."
  type        = string
  default     = "kv"
}

variable "self_heal" {
  description = <<-DESC
    Whether drift is corrected automatically.

    True everywhere here. It is exposed as a variable because testing an alert
    that fires on a broken cluster requires turning it off: self-heal repairs
    drift in about a second and the below-quorum alert deliberately waits
    fifteen, so the drift is gone before the rule can fire.
  DESC
  type        = bool
  default     = true
}
