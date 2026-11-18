terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.14"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }

  # Local state keeps the assignment self-contained. For a team, switch to a GCS backend:
  # backend "gcs" { bucket = "<project>-tfstate" prefix = "01-platform" }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
