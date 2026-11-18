# ---------------------------------------------------------------------------
# Message queue #2: Google Cloud Managed Service for Apache Kafka
#   shop-events        : gateway + order-service  -> Flink
#   analytics-results  : Flink -> analytics-service
# ---------------------------------------------------------------------------
resource "google_managed_kafka_cluster" "kafka" {
  cluster_id = "shopsphere-kafka"
  location   = var.region

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472 # 3 GiB (minimum: 1 GiB per vCPU)
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = google_compute_subnetwork.main.id
      }
    }
  }

  rebalance_config {
    mode = "AUTO_REBALANCE_ON_SCALE_UP"
  }

  depends_on = [google_project_service.apis]
}

resource "google_managed_kafka_topic" "shop_events" {
  topic_id           = "shop-events"
  cluster            = google_managed_kafka_cluster.kafka.cluster_id
  location           = var.region
  partition_count    = 6
  replication_factor = 3
  configs = {
    "retention.ms" = "86400000"
  }
}

resource "google_managed_kafka_topic" "analytics_results" {
  topic_id           = "analytics-results"
  cluster            = google_managed_kafka_cluster.kafka.cluster_id
  location           = var.region
  partition_count    = 3
  replication_factor = 3
  configs = {
    "retention.ms" = "604800000"
  }
}

# Identity used by every Kafka client (pods + Flink) via SASL/PLAIN.
resource "google_service_account" "kafka_client" {
  account_id   = "shopsphere-kafka-client"
  display_name = "Kafka SASL client identity"
}

resource "google_project_iam_member" "kafka_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.kafka_client.email}"
}

resource "google_service_account_key" "kafka_client" {
  service_account_id = google_service_account.kafka_client.name
}

# private_key is already the base64-encoded key JSON == the SASL/PLAIN password.
resource "google_secret_manager_secret" "kafka_password" {
  secret_id = "shopsphere-kafka-sasl-password"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "kafka_password" {
  secret      = google_secret_manager_secret.kafka_password.id
  secret_data = google_service_account_key.kafka_client.private_key
}

locals {
  kafka_bootstrap = "bootstrap.${google_managed_kafka_cluster.kafka.cluster_id}.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092"
}
