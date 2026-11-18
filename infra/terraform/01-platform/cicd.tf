# ---------------------------------------------------------------------------
# Keyless CI: GitHub Actions -> Workload Identity Federation -> GSA
# CI builds images (Artifact Registry), bumps tags in gitops/ (ArgoCD syncs),
# and builds/submits the Flink job to Dataproc.
# ---------------------------------------------------------------------------
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  attribute_condition = "assertion.repository == \"${var.github_repo}\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "ci" {
  account_id   = "shopsphere-ci"
  display_name = "GitHub Actions CI"
}

resource "google_service_account_iam_member" "ci_wif" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

resource "google_project_iam_member" "ci" {
  for_each = toset([
    "roles/artifactregistry.writer",
    "roles/dataproc.editor",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_storage_bucket_iam_member" "ci_flink_bucket" {
  bucket = google_storage_bucket.flink.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_secret_manager_secret_iam_member" "ci_kafka_secret" {
  secret_id = google_secret_manager_secret.kafka_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ci.email}"
}

# CI submits Dataproc jobs that run as the Dataproc SA -> needs actAs on it.
resource "google_service_account_iam_member" "ci_act_as_dataproc" {
  service_account_id = google_service_account.dataproc.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.ci.email}"
}
