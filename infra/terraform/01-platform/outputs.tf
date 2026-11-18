output "project_id" { value = var.project_id }
output "region" { value = var.region }
output "zone" { value = var.zone }

output "gke_cluster_name" { value = google_container_cluster.gke.name }
output "gke_endpoint" {
  value     = google_container_cluster.gke.endpoint
  sensitive = true
}
output "gke_ca_certificate" {
  value     = google_container_cluster.gke.master_auth[0].cluster_ca_certificate
  sensitive = true
}

output "db_host" { value = google_sql_database_instance.pg.private_ip_address }
output "db_user" { value = google_sql_user.app.name }
output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "firestore_database" { value = google_firestore_database.shopsphere.name }
output "pubsub_order_topic" { value = google_pubsub_topic.order_completed.name }

output "kafka_bootstrap" { value = local.kafka_bootstrap }
output "kafka_sasl_username" { value = google_service_account.kafka_client.email }
output "kafka_sasl_password" {
  value     = google_service_account_key.kafka_client.private_key
  sensitive = true
}
output "kafka_password_secret" { value = google_secret_manager_secret.kafka_password.secret_id }

output "app_gsa_email" { value = google_service_account.app.email }
output "artifact_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}
output "invoice_bucket" { value = google_storage_bucket.invoices.name }
output "flink_bucket" { value = google_storage_bucket.flink.name }
output "dataproc_cluster" { value = google_dataproc_cluster.flink.name }

output "ci_service_account" { value = google_service_account.ci.email }
output "ci_workload_identity_provider" { value = google_iam_workload_identity_pool_provider.github.name }
