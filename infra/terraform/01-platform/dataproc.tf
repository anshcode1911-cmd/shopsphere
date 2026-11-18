# ---------------------------------------------------------------------------
# Managed cluster for stream processing: Dataproc (image 2.2) with the Flink
# optional component. Single-node to keep the student budget low.
# ---------------------------------------------------------------------------
resource "google_service_account" "dataproc" {
  account_id   = "shopsphere-dataproc"
  display_name = "Dataproc / Flink job identity"
}

resource "google_project_iam_member" "dataproc" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/managedkafka.client",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_flink_bucket" {
  bucket = google_storage_bucket.flink.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_dataproc_cluster" "flink" {
  name   = "shopsphere-flink"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.flink.name

    gce_cluster_config {
      subnetwork       = google_compute_subnetwork.main.id
      internal_ip_only = true
      service_account  = google_service_account.dataproc.email
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
      ]
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version       = "2.2-debian12"
      optional_components = ["FLINK"]
      override_properties = {
        "dataproc:dataproc.allow.zero.workers"                     = "true"
        "dataproc:dataproc.logging.stackdriver.job.driver.enable"  = "true"
        "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable" = "true"
        "flink:taskmanager.numberOfTaskSlots"                      = "4"
      }
    }

    endpoint_config {
      enable_http_port_access = true # Flink / YARN UIs via Component Gateway
    }
  }

  depends_on = [
    google_project_iam_member.dataproc,
    google_storage_bucket_iam_member.dataproc_flink_bucket,
    google_compute_router_nat.nat,
  ]
}
