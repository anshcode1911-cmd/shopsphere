variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Primary region for all regional resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the (zonal, free-tier-management-fee) GKE cluster"
  type        = string
  default     = "us-central1-a"
}

variable "firestore_location" {
  description = "Firestore location (nam5 = US multi-region; use asia-south1 for Mumbai)"
  type        = string
  default     = "nam5"
}

variable "github_repo" {
  description = "GitHub repository in OWNER/NAME form (used for CI Workload Identity Federation)"
  type        = string
}

variable "gke_node_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "gke_min_nodes" {
  type    = number
  default = 2
}

variable "gke_max_nodes" {
  type    = number
  default = 5
}

variable "sql_tier" {
  type    = string
  default = "db-custom-2-7680"
}

variable "app_namespace" {
  description = "Kubernetes namespace of the microservices (Workload Identity binding)"
  type        = string
  default     = "ecommerce"
}

variable "app_ksa" {
  description = "Kubernetes service account used by the microservices"
  type        = string
  default     = "shopsphere-app"
}
