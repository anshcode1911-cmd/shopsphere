# ---------------------------------------------------------------------------
# Serverless: Cloud Functions (2nd gen) — invoice generation, triggered
# asynchronously by Pub/Sub `order-completed` via Eventarc.
# ---------------------------------------------------------------------------
data "archive_file" "invoice_fn" {
  type        = "zip"
  source_dir  = "${path.module}/../../../functions/invoice-function"
  output_path = "${path.module}/.build/invoice-function.zip"
}

resource "google_storage_bucket_object" "invoice_fn" {
  name   = "invoice-function-${data.archive_file.invoice_fn.output_md5}.zip"
  bucket = google_storage_bucket.functions_src.name
  source = data.archive_file.invoice_fn.output_path
}

resource "google_service_account" "invoice_fn" {
  account_id   = "shopsphere-invoice-fn"
  display_name = "Invoice Cloud Function"
}

resource "google_project_iam_member" "invoice_fn" {
  for_each = toset([
    "roles/datastore.user",
    "roles/logging.logWriter",
    "roles/run.invoker",              # Eventarc -> Cloud Run (function) invocation
    "roles/eventarc.eventReceiver",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.invoice_fn.email}"
}

resource "google_storage_bucket_iam_member" "invoice_fn_bucket" {
  bucket = google_storage_bucket.invoices.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.invoice_fn.email}"
}

# Pub/Sub service agent must mint tokens for push subscriptions created by Eventarc.
resource "google_project_iam_member" "pubsub_token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  depends_on = [google_project_service.apis]
}

# Dedicated Cloud Build identity for building the function (newer projects no longer
# grant the default compute SA build permissions).
resource "google_service_account" "fn_build" {
  account_id   = "shopsphere-fn-build"
  display_name = "Cloud Functions build"
}

resource "google_project_iam_member" "fn_build" {
  for_each = toset([
    "roles/cloudbuild.builds.builder",
    "roles/logging.logWriter",
    "roles/artifactregistry.writer",
    "roles/storage.objectViewer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.fn_build.email}"
}

resource "google_cloudfunctions2_function" "invoice" {
  name     = "invoice-function"
  location = var.region

  build_config {
    runtime         = "python312"
    entry_point     = "handle_order_completed"
    service_account = google_service_account.fn_build.id
    source {
      storage_source {
        bucket = google_storage_bucket.functions_src.name
        object = google_storage_bucket_object.invoice_fn.name
      }
    }
  }

  service_config {
    min_instance_count    = 0 # scale to zero
    max_instance_count    = 10
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.invoice_fn.email
    environment_variables = {
      INVOICE_BUCKET     = google_storage_bucket.invoices.name
      FIRESTORE_DATABASE = google_firestore_database.shopsphere.name
      LOG_EXECUTION_ID   = "true"
    }
  }

  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic          = google_pubsub_topic.order_completed.id
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.invoice_fn.email
  }

  depends_on = [
    google_project_iam_member.invoice_fn,
    google_project_iam_member.fn_build,
    google_project_iam_member.pubsub_token_creator,
  ]
}
