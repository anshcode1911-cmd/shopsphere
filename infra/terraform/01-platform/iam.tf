# ---------------------------------------------------------------------------
# Workload Identity: pods in ns/ecommerce running as KSA `shopsphere-app`
# impersonate this GSA -> no JSON keys for Firestore / Pub/Sub / GCS in pods.
# ---------------------------------------------------------------------------
resource "google_service_account" "app" {
  account_id   = "shopsphere-app"
  display_name = "ShopSphere microservices (GKE Workload Identity)"
}

resource "google_project_iam_member" "app" {
  for_each = toset([
    "roles/datastore.user",      # Firestore
    "roles/pubsub.publisher",    # order-completed topic
    "roles/logging.logWriter",
    "roles/cloudsql.client",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.app.email}"
}

resource "google_service_account_iam_member" "app_wi" {
  service_account_id = google_service_account.app.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.app_namespace}/${var.app_ksa}]"
  depends_on         = [google_container_cluster.gke]
}
