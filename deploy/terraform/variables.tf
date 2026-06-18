variable "project" {
  description = "GCP project ID"
  type        = string
  default     = "se-streamshard"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west3"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "europe-west3-a"
}

variable "node_count" {
  description = "Number of partition nodes (1, 3, or 5)"
  type        = number
  default     = 3

  validation {
    condition     = contains([1, 3, 5], var.node_count)
    error_message = "node_count must be 1, 3, or 5."
  }
}

variable "machine_type" {
  description = "GCE machine type for all nodes"
  type        = string
  default     = "e2-micro"
}

variable "gateway_machine_type" {
  description = "GCE machine type for the gateways"
  type        = string
  default     = "e2-micro"
}

variable "controlplane_machine_type" {
  description = "GCE machine type for the control plane (off the write path, keep small)"
  type        = string
  default     = "e2-small"
}

variable "gateway_count" {
  description = "Number of stateless gateways"
  type        = number
  default     = 1
}

variable "rf" {
  description = "Replication factor (set to 1 for single-node, 3 otherwise)"
  type        = number
  default     = 3
}

variable "repo_url" {
  description = "Git repository URL to clone on startup"
  type        = string
  default     = "https://github.com/Jakob-al28/StreamShard"
}

variable "k6_workers" {
  description = "Number of pre-provisioned k6 worker nodes in the GKE bench cluster"
  type        = number
  default     = 8
}

variable "gw_rate" {
  description = "Gateway token bucket rate (requests/sec)"
  type        = number
  default     = 1000000
}

variable "gw_burst" {
  description = "Gateway token bucket burst size"
  type        = number
  default     = 100000
}

variable "breaker_threshold" {
  description = "Gateway circuit breaker failure threshold before opening"
  type        = number
  default     = 999999
}

variable "queue_cap" {
  description = "Partition node apply-queue capacity (events in-flight before 429 shedding)"
  type        = number
  default     = 8192
}

variable "disable_ratelimit" {
  description = "Pass --disable-ratelimit to the gateway (for benchmarking)"
  type        = bool
  default     = true
}

variable "primary_replication" {
  description = "Primary node fans out replication instead of the gateway"
  type        = bool
  default     = false
}

variable "enable_swim" {
  description = "Enable SWIM gossip membership. Off for benchmarks: replication uses the static --peers ring."
  type        = bool
  default     = false
}

variable "wal_batch" {
  description = "Node --wal-batch: max writes coalesced into one WAL write syscall (1 = off)."
  type        = number
  default     = 1
}

variable "second_lb" {
  description = "Create a second forwarding rule (entry IP) onto the same gateway pool"
  type        = bool
  default     = false
}
