# Server EBS data volumes. One per server, attached as /dev/sdf.
# Decoded from var.disk_tier via the local.disk map in main.tf.
resource "aws_ebs_volume" "server_data" {
  count             = var.n_servers
  availability_zone = var.az
  type              = local.disk.volume_type
  size              = local.disk.volume_size
  iops              = local.disk.iops
  throughput        = local.disk.throughput
  tags = merge(local.tags, {
    Name = "${var.cell_id}-server-${count.index + 1}-data"
    Role = "server-data"
  })
}

# Server instances. Spot, instance-type from variables, public IP for
# convenience SSH from the orchestrator (still SG-restricted).
resource "aws_instance" "server" {
  count                  = var.n_servers
  ami                    = var.ubuntu_ami
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.sg_id]
  key_name               = var.key_name
  availability_zone      = var.az
  placement_group        = try(aws_placement_group.cell[0].name, null)

  associate_public_ip_address = true

  # On-demand for the canonical E19 rerun (was spot). On-demand gets priority
  # capacity, can't be reclaimed mid-benchmark, and surfaces capacity errors
  # fast instead of hanging on spot fulfillment. Re-add
  # instance_market_options{ market_type="spot" ... } to revert.

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # cloud-init: format + mount the data volume; install only what etcd
  # itself needs at runtime. The actual etcd binary, systemd unit, and
  # bench code is scp'd by the orchestrator after the VM is SSH-ready.
  # Always write /home/ubuntu/READY at the end (success or failed); the
  # orchestrator's wait_ready check distinguishes them.
  user_data = <<-EOF
    #!/bin/bash
    # Don't `set -e`: we want READY=failed instead of an absent file if any
    # step fails. Log every step to a dedicated file the orchestrator can
    # scp on failure.
    set -ux
    exec > >(tee -a /var/log/server-bootstrap.log) 2>&1
    export DEBIAN_FRONTEND=noninteractive

    finish() {
      local status="$1" reason="$2"
      printf 'status=%s reason=%s @ %s\n' "$status" "$reason" \
        "$(date -u +%FT%TZ)" > /home/ubuntu/READY
      chown ubuntu:ubuntu /home/ubuntu/READY
      exit 0
    }

    apt-get update || finish failed apt-update
    apt-get install -y chrony sysstat jq curl || finish failed apt-install

    # Wait for the EBS data volume (bounded: ~3 min).
    DEV=""
    for _ in $(seq 1 90); do
      for d in /dev/nvme1n1 /dev/sdf /dev/xvdf; do
        if [ -b "$d" ]; then DEV="$d"; break; fi
      done
      [ -n "$DEV" ] && break
      sleep 2
    done
    [ -n "$DEV" ] || finish failed no-data-volume

    mkfs.ext4 -F "$DEV"      || finish failed mkfs
    mkdir -p /var/lib/etcd
    mount "$DEV" /var/lib/etcd || finish failed mount
    echo "$DEV /var/lib/etcd ext4 defaults,noatime 0 0" >> /etc/fstab
    chown -R ubuntu:ubuntu /var/lib/etcd

    systemctl enable --now sysstat || true
    finish ok done
  EOF

  tags = merge(local.tags, {
    Name        = "${var.cell_id}-server-${count.index + 1}"
    Role        = "server"
    ServerIndex = tostring(count.index + 1)
  })

  # Tag attached volumes too.
  volume_tags = merge(local.tags, {
    Name = "${var.cell_id}-server-${count.index + 1}-root"
  })
}

resource "aws_volume_attachment" "server_data" {
  count       = var.n_servers
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.server_data[count.index].id
  instance_id = aws_instance.server[count.index].id
}
