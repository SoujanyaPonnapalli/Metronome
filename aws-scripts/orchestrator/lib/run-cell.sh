#!/usr/bin/env bash
# Composes launch-vms -> run-workload -> collect-results -> destroy-vms.
# This is the only entry point experiments/*.sh ever calls.
#
# Usage:
#   run-cell.sh <cell_id> <workload_path> -- <k=v terraform vars>
#
# Example:
#   run-cell.sh E1-n3-v4096-vanilla-gp3 workloads/etcd-bench-put.sh \
#       n_servers=3 disk_tier=gp3-baseline etcd_mode=vanilla value_size=4096
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

CELL_ID="${1:?cell_id required}"
WORKLOAD="${2:?workload path required}"
shift 2

# Default: re-run on transient failure once. Set RETRY=0 to disable.
RETRY="${RETRY:-1}"

run_once() {
  # launch-vms.sh consumes the k=v pairs as terraform vars.
  # run-workload.sh also needs them so the driver workload script gets
  # workload-shaping args (value_size, n_clients, ...). Earlier bug: we
  # only passed them to launch-vms, so the driver used VALUE_SIZE default
  # of 4096 even when the cell was supposed to be v=256.
  "$HERE/launch-vms.sh"   "$CELL_ID" "$@"
  "$HERE/run-workload.sh" "$CELL_ID" "$WORKLOAD" "$@"
  "$HERE/collect-results.sh" "$CELL_ID"
}

cleanup() {
  set +e
  "$HERE/destroy-vms.sh" "$CELL_ID" || true
}

trap cleanup EXIT

if run_once "$@"; then
  :
elif [[ "$RETRY" -gt 0 ]]; then
  echo "[run-cell] first attempt failed; tearing down and retrying once"
  "$HERE/destroy-vms.sh" "$CELL_ID" || true
  RETRY=0 run_once "$@"
else
  exit 1
fi
