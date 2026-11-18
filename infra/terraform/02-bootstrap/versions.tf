terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.14"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.16"
    }
  }
}

# Outputs of stage 01 (cluster endpoint, DB host, Kafka bootstrap, secrets ...)
data "terraform_remote_state" "platform" {
  backend = "local"
  config = {
    path = "${path.module}/../01-platform/terraform.tfstate"
  }
}

locals {
  p = data.terraform_remote_state.platform.outputs
}

provider "google" {
  project = local.p.project_id
  region  = local.p.region
}

data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${local.p.gke_endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(local.p.gke_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${local.p.gke_endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(local.p.gke_ca_certificate)
  }
}
