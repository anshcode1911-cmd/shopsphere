# ---------------------------------------------------------------------------
# Message queue #1: Pub/Sub — order-service -> invoice Cloud Function
# ---------------------------------------------------------------------------
resource "google_pubsub_topic" "order_completed" {
  name                       = "order-completed"
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_topic" "order_completed_dlq" {
  name = "order-completed-dlq"
}
