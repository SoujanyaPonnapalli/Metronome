terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.60" }
  }
}

provider "aws" {
  region = var.region
}

locals {
  project = "metronome-eval"
  tags = {
    Project   = local.project
    ManagedBy = "terraform"
    Tier      = "cell"
    CellId    = var.cell_id
    Mode      = var.etcd_mode
    DiskTier  = var.disk_tier
    NServers  = tostring(var.n_servers)
    ValueSize = tostring(var.value_size)
  }

  # Disk-tier decoder: maps a friendly tier name to the AWS EBS knobs.
  # Sized to fit etcd raft log + snapshot + headroom at ~10k tx/s for
  # several minutes (the bench window). 200 GB gp3 / 500 GB st1.
  disk_specs = {
    "gp3-baseline" = {
      volume_type = "gp3"
      volume_size = 200
      iops        = 3000 # gp3 baseline
      throughput  = 125  # MB/s, gp3 baseline
    }
    "gp3-provisioned" = {
      volume_type = "gp3"
      volume_size = 200
      iops        = 16000 # provisioned
      throughput  = 1000  # MB/s, near gp3 cap
    }
    # Throughput sweep between gp3-baseline (125) and gp3-provisioned (1000).
    # IOPS held at baseline (3000) so we isolate the throughput axis.
    "gp3-200" = {
      volume_type = "gp3"
      volume_size = 200
      iops        = 3000
      throughput  = 200
    }
    "gp3-300" = {
      volume_type = "gp3"
      volume_size = 200
      iops        = 3000
      throughput  = 300
    }
    "st1-HDD" = {
      volume_type = "st1"
      volume_size = 500 # st1 min for sustained throughput
      iops        = null
      throughput  = null
    }
    # io2 (regular): up to 64k IOPS, ~1000 MB/s. 4x IOPS over gp3-provisioned
    # at the same nominal bandwidth — isolates the IOPS axis.
    "io2-fast" = {
      volume_type = "io2"
      volume_size = 200
      iops        = 64000
      throughput  = null # not applicable for io2
    }
    # io2 Block Express: up to 256k IOPS, 4000 MB/s. Activated automatically
    # by AWS when iops > 64k on a Nitro instance (c6in.2xlarge qualifies).
    # 16x IOPS, 4x bandwidth vs gp3-provisioned.
    "io2-extreme" = {
      volume_type = "io2"
      volume_size = 200
      iops        = 100000
      throughput  = null
    }
  }
  disk = local.disk_specs[var.disk_tier]

  # Effective AZ / subnet lists used by servers.tf. In single-AZ mode
  # they collapse to a one-element list pinned to var.az / var.subnet_id;
  # in cross-AZ mode they're the user-supplied var.azs / var.subnet_ids.
  # Servers are distributed round-robin: server[i] -> effective_azs[i mod len].
  effective_azs     = var.cross_az ? var.azs : [var.az]
  effective_subnets = var.cross_az ? var.subnet_ids : [var.subnet_id]
}
