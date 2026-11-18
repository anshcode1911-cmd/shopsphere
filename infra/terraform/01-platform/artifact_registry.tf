resource "google_artifact_registry_repository" "images" {
  repository_id = "shopsphere"
  location      = var.region
  format        = "DOCKER"
  description   = "ShopSphere microservice images (pushed by GitHub Actions)"
  depends_on    = [google_project_service.apis]
}
