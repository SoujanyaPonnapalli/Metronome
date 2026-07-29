#!/usr/bin/env bash
# Tears down a single cell's VMs (idempotent). Removes the per-cell
# state file on success.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
CELL_ID="${1:?cell_id required as first arg}"

source "$PROJECT_ROOT/infra.env"

STATE_FILE="$PROJECT_ROOT/cell-states/$CELL_ID.tfstate"
if [[ ! -f "$STATE_FILE" ]]; then
  echo "(no state file for $CELL_ID; nothing to destroy)"
  exit 0
fi

# We still need to pass the input vars terraform validated on apply;
# values don't matter for destroy but the variable block requires them.
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

cd "$PROJECT_ROOT/cell/terraform"
terraform init -input=false >/dev/null
terraform destroy -auto-approve -input=false -lock-timeout=120s \
  -state="$STATE_FILE" "${TF_VARS[@]}"

# Archive then remove state so we keep a record but don't accumulate.
ARCHIVE_DIR="$PROJECT_ROOT/cell-states/archive"
mkdir -p "$ARCHIVE_DIR"
mv "$STATE_FILE"           "$ARCHIVE_DIR/$CELL_ID.tfstate.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
mv "${STATE_FILE}.backup"  "$ARCHIVE_DIR/" 2>/dev/null || true

echo "destroyed $CELL_ID"
