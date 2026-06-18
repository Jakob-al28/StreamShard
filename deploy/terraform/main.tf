terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

locals {
  # With a single node, force RF=1 regardless of the variable.
  effective_rf = var.node_count == 1 ? 1 : var.rf

  node_names   = [for i in range(var.node_count) : "streamshard-node-${i}"]
  node_port    = 8080
  swim_base    = 9080
  gw_swim_port = 9070
  gw_port      = 7070
  cp_port      = 6060
}

# Network

resource "google_compute_network" "streamshard" {
  name                    = "streamshard"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "internal" {
  name    = "streamshard-internal"
  network = google_compute_network.streamshard.name

  allow {
    protocol = "tcp"
    ports    = ["6060", "7070", "8080"]
  }

  allow {
    protocol = "udp"
    ports    = ["9070-9085"]
  }

  source_tags = ["streamshard"]
  target_tags = ["streamshard"]
}

resource "google_compute_firewall" "gateway_ingress" {
  name    = "streamshard-gateway-ingress"
  network = google_compute_network.streamshard.name

  allow {
    protocol = "tcp"
    ports    = ["7070"]
  }

  # Allow external traffic and GCP TCP-LB health check probes.
  source_ranges = ["0.0.0.0/0", "35.191.0.0/16", "130.211.0.0/22"]
  target_tags   = ["streamshard-gateway"]
}

resource "google_compute_firewall" "ssh" {
  name    = "streamshard-ssh"
  network = google_compute_network.streamshard.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["streamshard"]
}

#################
# Control plane #
#################

resource "google_compute_instance" "controlplane" {
  name                      = "streamshard-controlplane"
  machine_type              = var.controlplane_machine_type
  tags                      = ["streamshard"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  network_interface {
    network = google_compute_network.streamshard.name
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/controlplane.sh", {
    repo_url = var.repo_url
    cp_port  = local.cp_port
  })

  service_account {
    scopes = ["cloud-platform"]
  }
}

###################
# Partition nodes #
###################

resource "google_compute_address" "node" {
  count        = var.node_count
  name         = "streamshard-node-${count.index}"
  address_type = "INTERNAL"
  subnetwork   = "default"
  region       = var.region
}

resource "google_compute_instance" "node" {
  count                     = var.node_count
  name                      = local.node_names[count.index]
  machine_type              = var.machine_type
  tags                      = ["streamshard"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  network_interface {
    network    = google_compute_network.streamshard.name
    network_ip = google_compute_address.node[count.index].address
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh", {
    repo_url   = var.repo_url
    node_port  = local.node_port
    data_dir   = "/var/lib/streamshard"
    swim_port  = local.swim_base + count.index
    queue_cap  = var.queue_cap
    node_addrs = join(",", [for i in range(var.node_count) : "${google_compute_address.node[i].address}:${local.node_port}"])
    rf         = local.effective_rf
    w          = local.effective_rf == 1 ? 1 : 2
    primary_replication = var.primary_replication
    enable_swim = var.enable_swim
    wal_batch   = var.wal_batch
    swim_seeds = join(",", [
      for i in range(var.node_count) :
      "${google_compute_address.node[i].address}:${local.swim_base + i}"
      if i != count.index
    ])
  })

  service_account {
    scopes = ["cloud-platform"]
  }
}

############
# Gateways #
############

resource "google_compute_instance" "gateway" {
  count                     = var.gateway_count
  name                      = "streamshard-gateway-${count.index}"
  machine_type              = var.gateway_machine_type
  tags                      = ["streamshard", "streamshard-gateway"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20
    }
  }

  network_interface {
    network = google_compute_network.streamshard.name
    access_config {}
  }

  metadata_startup_script = templatefile("${path.module}/startup/gateway.sh", {
    repo_url   = var.repo_url
    gw_port    = local.gw_port
    cp_addr    = "${google_compute_instance.controlplane.network_interface[0].network_ip}:${local.cp_port}"
    node_addrs = join(",", [for i in range(var.node_count) : "${google_compute_address.node[i].address}:${local.node_port}"])
    rf         = local.effective_rf
    w          = local.effective_rf == 1 ? 1 : 2
    swim_addr  = "0.0.0.0:${local.gw_swim_port + count.index}"
    swim_seeds = join(",", [for i in range(var.node_count) : "${google_compute_address.node[i].address}:${local.swim_base + i}"])
    gw_rate                  = var.gw_rate
    gw_burst                 = var.gw_burst
    breaker_threshold        = var.breaker_threshold
    disable_ratelimit_flag   = var.disable_ratelimit ? "--disable-ratelimit" : ""
    primary_replication_flag = var.primary_replication ? "--primary-replication" : ""
  })

  service_account {
    scopes = ["cloud-platform"]
  }

  depends_on = [
    google_compute_instance.node,
    google_compute_instance.controlplane,
  ]
}

###############
# GKE cluster #
###############

# The k6 load-generation cluster is created manually (gcloud container clusters create),
# not here. A terraform apply on a GKE cluster + node pool took too long and we kept
# tearing it down between benchmark runs, so the churn wasn't worth managing as IaC.
# If you do want it in Terraform, it's roughly:
#   resource "google_project_service" "container" { ... }
#   resource "google_container_cluster" "bench" { ... }
#   resource "google_container_node_pool" "bench_nodes" { ... }

######################
# TCP load balancer  #
######################

# Legacy HTTP health checkm required by target pools
resource "google_compute_http_health_check" "gateway" {
  name                = "streamshard-gw-health"
  request_path        = "/health"
  port                = local.gw_port
  check_interval_sec  = 5
  timeout_sec         = 3
  healthy_threshold   = 1
  unhealthy_threshold = 2
}

# Target pool distributes TCP connections across all gateway instances
resource "google_compute_target_pool" "gateways" {
  name          = "streamshard-gw-pool"
  region        = var.region
  instances     = [for gw in google_compute_instance.gateway : gw.self_link]
  health_checks = [google_compute_http_health_check.gateway.self_link]
}

# Regional forwarding rule, single stable IP on gw_port that k6 targets.
resource "google_compute_forwarding_rule" "gateway_lb" {
  name                  = "streamshard-gw-lb"
  region                = var.region
  target                = google_compute_target_pool.gateways.self_link
  port_range            = tostring(local.gw_port)
  load_balancing_scheme = "EXTERNAL"
}

# Optional second entry IP onto the same gateway pool
resource "google_compute_forwarding_rule" "gateway_lb2" {
  count                 = var.second_lb ? 1 : 0
  name                  = "streamshard-gw-lb2"
  region                = var.region
  target                = google_compute_target_pool.gateways.self_link
  port_range            = tostring(local.gw_port)
  load_balancing_scheme = "EXTERNAL"
}
