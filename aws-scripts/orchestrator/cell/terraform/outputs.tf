output "cell_id" {
  value = var.cell_id
}

output "n_servers" {
  value = var.n_servers
}

output "n_drivers" {
  value = var.n_drivers
}

# Public IPs for SSH from the orchestrator.
output "server_public_ips" {
  value = aws_instance.server[*].public_ip
}

output "server_private_ips" {
  value = aws_instance.server[*].private_ip
}

output "server_ids" {
  value = aws_instance.server[*].id
}

output "driver_public_ips" {
  value = aws_instance.driver[*].public_ip
}

output "driver_private_ips" {
  value = aws_instance.driver[*].private_ip
}

output "driver_ids" {
  value = aws_instance.driver[*].id
}

# Back-compat singular fields — point at driver[0] when n_drivers >= 1.
output "driver_public_ip" {
  value = var.n_drivers > 0 ? aws_instance.driver[0].public_ip : ""
}

output "driver_private_ip" {
  value = var.n_drivers > 0 ? aws_instance.driver[0].private_ip : ""
}

output "driver_id" {
  value = var.n_drivers > 0 ? aws_instance.driver[0].id : ""
}

# topology.json blob ready to drop into results/<cell>/topology.json.
#
# `drivers` is the new authoritative array (length = n_drivers).
# `driver` (singular) is kept for backward compatibility with older
# parsers — it always points at drivers[0].
output "topology_json" {
  value = jsonencode({
    cell_id                 = var.cell_id
    n_servers               = var.n_servers
    n_drivers               = var.n_drivers
    disk_tier               = var.disk_tier
    etcd_mode               = var.etcd_mode
    value_size              = var.value_size
    metronome_quorum_offset = var.metronome_quorum_offset
    n_clients               = var.n_clients
    az                      = var.az
    servers = [for i in range(var.n_servers) : {
      index       = i + 1
      public_ip   = aws_instance.server[i].public_ip
      private_ip  = aws_instance.server[i].private_ip
      instance_id = aws_instance.server[i].id
    }]
    drivers = [for i in range(var.n_drivers) : {
      index       = i + 1
      public_ip   = aws_instance.driver[i].public_ip
      private_ip  = aws_instance.driver[i].private_ip
      instance_id = aws_instance.driver[i].id
    }]
    driver = {
      public_ip   = aws_instance.driver[0].public_ip
      private_ip  = aws_instance.driver[0].private_ip
      instance_id = aws_instance.driver[0].id
    }
  })
}
