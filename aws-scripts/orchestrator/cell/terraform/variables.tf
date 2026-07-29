variable "region" {
  type    = string
  default = "us-west-1"
}

# ---- Inputs sourced from infra.env (no defaults; required) ----
variable "vpc_id" { type = string }
variable "subnet_id" { type = string }
variable "sg_id" { type = string }
variable "placement_group" { type = string }
variable "key_name" { type = string }
variable "ubuntu_ami" { type = string }
variable "az" { type = string }

# ---- Per-cell parameters ----
variable "cell_id" {
  description = "Stable string identifying this cell (e.g. E1-n5-v4096-metronome-gp3-c500). Tagged on every resource."
  type        = string
}

variable "n_servers" {
  description = "Number of etcd server nodes in the cluster."
  type        = number
  default     = 5
}

variable "instance_type" {
  description = "EC2 instance type for both servers and driver."
  type        = string
  default     = "c6in.2xlarge"
}

variable "disk_tier" {
  description = "EBS data-volume tier. One of: gp3-baseline, gp3-200, gp3-300, gp3-provisioned, st1-HDD, io2-fast, io2-extreme."
  type        = string
  default     = "gp3-baseline"
  validation {
    condition     = contains(["gp3-baseline", "gp3-200", "gp3-300", "gp3-provisioned", "st1-HDD", "io2-fast", "io2-extreme"], var.disk_tier)
    error_message = "disk_tier must be one of: gp3-baseline, gp3-200, gp3-300, gp3-provisioned, st1-HDD, io2-fast, io2-extreme."
  }
}

# ---- Workload-shaping (consumed downstream by lib/run-workload.sh ----
variable "etcd_mode" {
  description = "etcd build to run: vanilla | inmem | metronome. Only used as a tag on the cell."
  type        = string
  default     = "metronome"
}

variable "value_size" {
  description = "Workload value size in bytes (tag only)."
  type        = number
  default     = 4096
}

variable "metronome_quorum_offset" {
  description = <<-EOT
    Offset added to the default persist-set size for metronome mode.
      0 -> K = f+1  (etcd default; passes no --metronome-quorum-size flag)
      1 -> K = f+2
      2 -> K = f+3
      ...
    Tag only; the actual --metronome-quorum-size value is computed in
    lib/run-workload.sh from n_servers + this offset. Ignored when
    etcd_mode != "metronome".
  EOT
  type        = number
  default     = 0
}

variable "n_clients" {
  description = "Driver concurrency (tag only; consumed by workloads/etcd-bench-put.sh)."
  type        = number
  default     = 300
}

variable "n_drivers" {
  description = <<-EOT
    Number of load-generating driver VMs. Default 1. Used > 1 in E0's
    2-driver validation step to confirm a single driver isn't a hidden
    bottleneck. Each driver runs the same workload with n_clients
    concurrency, so the cluster sees total concurrency = n_drivers ×
    n_clients.
  EOT
  type        = number
  default     = 1
}

# ---- Cross-AZ deployment (E7) ----
variable "cross_az" {
  description = <<-EOT
    When true, distribute server instances round-robin across var.azs /
    var.subnet_ids instead of pinning every server to var.az / var.subnet_id.
    Placement group is dropped (it's single-AZ only). Used by experiments/E7-cross-az.sh
    to measure metronome's benefit at moderate (1-3ms) cluster RTT.
    Driver always stays in var.az regardless of this flag.
  EOT
  type        = bool
  default     = false
}

variable "azs" {
  description = "AZ list for cross-AZ mode. Required (length >= 2) when cross_az=true; ignored otherwise."
  type        = list(string)
  default     = []
}

variable "subnet_ids" {
  description = "Subnet list aligned 1:1 with var.azs. Required when cross_az=true; ignored otherwise."
  type        = list(string)
  default     = []
}
