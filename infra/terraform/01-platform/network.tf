# ---------------------------------------------------------------------------
# VPC shared by GKE, Cloud SQL (private IP), Managed Kafka and Dataproc
# ---------------------------------------------------------------------------
resource "google_compute_network" "vpc" {
  name                    = "shopsphere-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "main" {
  name                     = "shopsphere-subnet"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = "10.10.0.0/20"
  private_ip_google_access = true # Dataproc (internal IPs only) -> GCS / APIs

  secondary_ip_range {
    range_name    = "gke-pods"
    ip_cidr_range = "10.20.0.0/16"
  }
  secondary_ip_range {
    range_name    = "gke-services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

resource "google_compute_firewall" "internal" {
  name    = "shopsphere-allow-internal"
  network = google_compute_network.vpc.name
  allow { protocol = "tcp" }
  allow { protocol = "udp" }
  allow { protocol = "icmp" }
  source_ranges = ["10.10.0.0/20", "10.20.0.0/16", "10.30.0.0/20"]
}

resource "google_compute_firewall" "iap_ssh" {
  name          = "shopsphere-allow-iap-ssh"
  network       = google_compute_network.vpc.name
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["35.235.240.0/20"] # IAP TCP forwarding range
}

# Outbound internet for private Dataproc VMs
resource "google_compute_router" "router" {
  name    = "shopsphere-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "shopsphere-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Private Service Access -> Cloud SQL gets a private IP inside our VPC
resource "google_compute_global_address" "private_services" {
  name          = "shopsphere-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_vpc" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}
