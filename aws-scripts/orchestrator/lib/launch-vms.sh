#!/usr/bin/env bash
# Single entrypoint to bring up one cell's VMs (N servers + 1 driver).
# Sources ../infra.env for the shared VPC/SG/etc. and runs terraform
# against ../cell/terraform with a per-cell state file under
# ../cell-states/<cell_id>.tfstate so cells don't collide.
#
# Writes ../results/<cell_id>/topology.json on success.
#
# Usage:
#   launch-vms.sh <cell_id> n_servers=<N> disk_tier=<tier> [k=v ...]
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
CELL_ID="${1:?cell_id required as first arg}"
shift

source "$PROJECT_ROOT/infra.env"

# Parse remaining k=v args into terraform -var options.
TF_VARS=(
  -var "cell_id=$CELL_ID"
  -var "region=$AWS_REGION"
  -var "vpc_id=$VPC_ID"
  -var "subnet_id=$SUBNET_ID"
  -var "sg_id=$SG_ID"
  -var "placement_group=$PLACEMENT_GROUP"
  -var "key_name=$KEY_NAME"
  -var "ubuntu_ami=$UBUNTU_AMI"
  -var "az=$AZ"
)
for kv in "$@"; do
  TF_VARS+=(-var "$kv")
done

STATE_DIR="$PROJECT_ROOT/cell-states"
STATE_FILE="$STATE_DIR/$CELL_ID.tfstate"
mkdir -p "$STATE_DIR"

RESULTS_DIR="$PROJECT_ROOT/results/$CELL_ID"
mkdir -p "$RESULTS_DIR"

cd "$PROJECT_ROOT/cell/terraform"
terraform init -input=false >/dev/null

# Apply with cell-specific state. -lock-timeout because multiple cells
# may run concurrently in the future and could race on state init.
terraform apply -auto-approve -input=false -lock-timeout=120s \
  -state="$STATE_FILE" "${TF_VARS[@]}"

# Capture topology.json for downstream consumers.
terraform output -state="$STATE_FILE" -raw topology_json > "$RESULTS_DIR/topology.json"
echo "wrote $RESULTS_DIR/topology.json"

# Print a quick summary so the operator can see what came up.
echo
echo "=== $CELL_ID up ==="
jq -r '
  "servers:",
  (.servers[] | "  [\(.index)] \(.public_ip)  (\(.private_ip))"),
  "driver:",
  "  \(.driver.public_ip)  (\(.driver.private_ip))"
' "$RESULTS_DIR/topology.json"
