# ---------------------------------------------------------------------------
# Storage product #3: Managed NoSQL — Firestore (Native mode, named database)
# collections: payments, analytics_windows, invoices, notifications
# ---------------------------------------------------------------------------
resource "google_firestore_database" "shopsphere" {
  name                    = "shopsphere"
  location_id             = var.firestore_location
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = "DELETE_PROTECTION_DISABLED"
  deletion_policy         = "DELETE"
  depends_on              = [google_project_service.apis]
}
