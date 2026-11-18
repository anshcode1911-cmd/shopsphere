# ---------------------------------------------------------------------------
# Storage product #2: Object storage — Cloud Storage
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "invoices" {
  name                        = "${var.project_id}-shopsphere-invoices"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# Flink job jars, Flink checkpoints/savepoints, Dataproc staging
resource "google_storage_bucket" "flink" {
  name                        = "${var.project_id}-shopsphere-flink"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Cloud Function source archives
resource "google_storage_bucket" "functions_src" {
  name                        = "${var.project_id}-shopsphere-fn-src"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}
