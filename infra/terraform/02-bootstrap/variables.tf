variable "gitops_repo_url" {
  description = "HTTPS URL of the Git repository ArgoCD tracks (e.g. https://github.com/you/shopsphere.git)"
  type        = string
}

variable "gitops_revision" {
  description = "Branch ArgoCD tracks"
  type        = string
  default     = "main"
}

variable "git_username" {
  description = "Only needed if the repository is private"
  type        = string
  default     = ""
}

variable "git_token" {
  description = "GitHub PAT with read access (only needed if the repository is private)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "app_namespace" {
  type    = string
  default = "ecommerce"
}
