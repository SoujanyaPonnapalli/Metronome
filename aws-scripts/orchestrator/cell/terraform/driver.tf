# Load-generating driver VM(s). Same instance type as the servers so each
# driver has plenty of CPU + network to push the cluster. No persistent
# data volume needed.
#
# count = var.n_drivers — default 1. Used > 1 in the E0 2-driver
# validation step to confirm a single driver isn't a hidden bottleneck.
resource "aws_instance" "driver" {
  count                  = var.n_drivers
  ami                    = var.ubuntu_ami
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.sg_id]
  key_name               = var.key_name
  availability_zone      = var.az
  placement_group        = try(aws_placement_group.cell[0].name, null)

  associate_public_ip_address = true

  # On-demand for the canonical E19 rerun (was spot) — see servers.tf.

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -ux
    exec > >(tee -a /var/log/driver-bootstrap.log) 2>&1
    export DEBIAN_FRONTEND=noninteractive

    finish() {
      printf 'status=%s reason=%s @ %s\n' "$1" "$2" "$(date -u +%FT%TZ)" \
        > /home/ubuntu/READY
      chown ubuntu:ubuntu /home/ubuntu/READY
      exit 0
    }

    apt-get update || finish failed apt-update
    apt-get install -y chrony sysstat jq curl || finish failed apt-install
    systemctl enable --now sysstat || true
    finish ok done
  EOF

  tags = merge(local.tags, {
    Name = "${var.cell_id}-driver-${count.index + 1}"
    Role = "driver"
  })
  volume_tags = merge(local.tags, {
    Name = "${var.cell_id}-driver-${count.index + 1}-root"
  })
}
