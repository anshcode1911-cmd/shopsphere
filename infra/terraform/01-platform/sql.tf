# ---------------------------------------------------------------------------
# Storage product #1: Managed SQL — Cloud SQL for PostgreSQL (private IP only)
# Database-per-service: users (user-service), catalog (catalog-service), orders (order-service)
# ---------------------------------------------------------------------------
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "google_sql_database_instance" "pg" {
  name                = "shopsphere-pg"
  database_version    = "POSTGRES_15"
  region              = var.region
  deletion_protection = false

  settings {
    tier              = var.sql_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_size         = 20
    disk_autoresize   = true

    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = google_compute_network.vpc.id
      enable_private_path_for_google_cloud_services = true
    }

    backup_configuration {
      enabled = true
    }

    database_flags {
      name  = "max_connections"
      value = "300"
    }

    insights_config {
      query_insights_enabled = true
    }
  }

  depends_on = [google_service_networking_connection.private_vpc]
}

resource "google_sql_database" "dbs" {
  for_each = toset(["users", "catalog", "orders"])
  name     = each.value
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_user" "app" {
  name     = "app"
  instance = google_sql_database_instance.pg.name
  password = random_password.db.result
}
