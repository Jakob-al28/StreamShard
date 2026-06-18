output "gateway_ips" {
  description = "External IPs of gateways"
  value       = [for gw in google_compute_instance.gateway : gw.network_interface[0].access_config[0].nat_ip]
}

output "node_internal_ips" {
  description = "Internal IPs of partition nodes"
  value       = [for n in google_compute_instance.node : n.network_interface[0].network_ip]
}

output "controlplane_ip" {
  description = "External IP of the control plane"
  value       = google_compute_instance.controlplane.network_interface[0].access_config[0].nat_ip
}

output "lb_ip" {
  description = "TCP load balancer IP"
  value       = google_compute_forwarding_rule.gateway_lb.ip_address
}

output "k6_base_url" {
  description = "BASE_URL for k6"
  value       = "http://${google_compute_forwarding_rule.gateway_lb.ip_address}:${local.gw_port}"
}

output "lb_ip2" {
  description = "Second TCP load balancer IP (only when second_lb=true)"
  value       = var.second_lb ? google_compute_forwarding_rule.gateway_lb2[0].ip_address : ""
}

output "zone" {
  description = "GCP zone used for all instances"
  value       = var.zone
}

output "gke_cluster" {
  description = "GKE cluster name for kubectl / run_benchmark.sh --k8s"
  value       = "streamshard-bench"
}
