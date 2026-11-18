# ---------------------------------------------------------------------------
# Centralised logging: one Cloud Logging bucket that aggregates logs from
#   * every microservice pod on GKE        (resource.type = k8s_container)
#   * the Flink analytics job on Dataproc  (resource.type = cloud_dataproc_cluster / _job)
#   * the invoice Cloud Function           (resource.type = cloud_run_revision)
# (Loki in-cluster provides the same pod logs inside Grafana.)
# ---------------------------------------------------------------------------
resource "google_logging_project_bucket_config" "central" {
  project        = var.project_id
  location       = "global"
  bucket_id      = "shopsphere-central"
  retention_days = 30
  description    = "Aggregated ShopSphere logs (GKE services + Flink job + Cloud Function)"
}

locals {
  central_log_filter = <<-EOT
    (resource.type="k8s_container" AND resource.labels.namespace_name="${var.app_namespace}")
    OR resource.type="cloud_dataproc_cluster"
    OR resource.type="cloud_dataproc_job"
    OR (resource.type="cloud_run_revision" AND resource.labels.service_name="invoice-function")
    OR (resource.type="cloud_function" AND resource.labels.function_name="invoice-function")
  EOT
}

resource "google_logging_project_sink" "central" {
  name                   = "shopsphere-central-sink"
  destination            = "logging.googleapis.com/${google_logging_project_bucket_config.central.id}"
  filter                 = local.central_log_filter
  unique_writer_identity = true
}

resource "google_project_iam_member" "sink_writer" {
  project = var.project_id
  role    = "roles/logging.bucketWriter"
  member  = google_logging_project_sink.central.writer_identity
}

# Log-based metric: count of ERROR logs across all services (usable for alerting)
resource "google_logging_metric" "errors" {
  name   = "shopsphere_error_logs"
  filter = "(${trimspace(local.central_log_filter)}) AND severity>=ERROR"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}
