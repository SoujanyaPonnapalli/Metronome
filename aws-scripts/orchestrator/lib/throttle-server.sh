#!/usr/bin/env bash
# Apply or remove a cgroup-v2 write-bandwidth throttle on one server's
# running etcd process. Used by E3 (stragglers) to simulate a slow disk
# on a single follower while the rest of the cluster runs at full speed.
#
# Usage:
#   throttle-server.sh apply  <cell_id> <server_index 1-based> <wbps>
#   throttle-server.sh remove <cell_id> <server_index 1-based>
#
# Implementation: creates a custom cgroup at /sys/fs/cgroup/etcd-throttle
# with io.max="<major>:<minor> wbps=<N>" for the data-volume device, then
# moves the etcd PID into it. Idempotent (re-applying updates the limit).
# Cleanup is best-effort; the VM is destroyed at cell-end anyway.
set -euo pipefail

ACTION="${1:?action required: apply|remove}"
CELL_ID="${2:?cell_id required}"
SRV_IDX="${3:?server_index required (1-based)}"
WBPS="${4:-0}"

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

TOPO="$PROJECT_ROOT/results/$CELL_ID/topology.json"
[[ -f "$TOPO" ]] || { echo "throttle-server: no topology.json for $CELL_ID" >&2; exit 2; }

PRV=$(jq -r --argjson i $((SRV_IDX-1)) '.servers[$i].private_ip // empty' "$TOPO")
[[ -n "$PRV" && "$PRV" != "null" ]] || \
  { echo "throttle-server: no server #$SRV_IDX in $TOPO" >&2; exit 2; }

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o LogLevel=ERROR -i ~/.ssh/${KEY_NAME}.pem)

case "$ACTION" in
  apply)
    [[ "$WBPS" -gt 0 ]] || { echo "throttle-server: wbps must be > 0 for apply" >&2; exit 2; }
    ssh "${SSH_OPTS[@]}" "ubuntu@$PRV" bash -s "$WBPS" <<'REMOTE'
set -euo pipefail
WBPS="$1"

ETCDPID=$(pgrep -f '/usr/local/bin/etcd --name=' | head -1)
[[ -n "$ETCDPID" ]] || { echo "throttle: no running etcd on $(hostname)" >&2; exit 3; }

# Resolve the data-volume block device backing /var/lib/etcd. Strip any
# partition suffix so we throttle the whole device (e.g. nvme1n1p1 → nvme1n1).
DEV=$(findmnt -no SOURCE /var/lib/etcd | head -1)
[[ -n "$DEV" ]] || { echo "throttle: /var/lib/etcd not mounted" >&2; exit 3; }
BASE=$(basename "$DEV")
BASE=${BASE%p[0-9]*}
DEV_MM=$(cat "/sys/class/block/$BASE/dev" 2>/dev/null || true)
[[ -n "$DEV_MM" ]] || { echo "throttle: no major:minor for $BASE" >&2; exit 3; }

# cgroup v2: the io controller must be delegated to children via
# cgroup.subtree_control. On systemd Ubuntu this is usually already set.
if ! grep -qw io /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null; then
  echo "+io" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null \
    || { echo "throttle: could not enable io controller" >&2; exit 3; }
fi

sudo mkdir -p /sys/fs/cgroup/etcd-throttle
echo "$DEV_MM wbps=$WBPS" | sudo tee /sys/fs/cgroup/etcd-throttle/io.max >/dev/null
echo "$ETCDPID" | sudo tee /sys/fs/cgroup/etcd-throttle/cgroup.procs >/dev/null
echo "throttle APPLY on $(hostname) dev=$DEV ($DEV_MM) wbps=$WBPS pid=$ETCDPID"
REMOTE
    ;;
  remove)
    ssh "${SSH_OPTS[@]}" "ubuntu@$PRV" bash -s <<'REMOTE'
set -euo pipefail
if [[ -d /sys/fs/cgroup/etcd-throttle ]]; then
  # Move every PID in the throttle cgroup back to the root, then rmdir.
  if [[ -r /sys/fs/cgroup/etcd-throttle/cgroup.procs ]]; then
    while read -r pid; do
      [[ -n "$pid" ]] || continue
      echo "$pid" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true
    done < /sys/fs/cgroup/etcd-throttle/cgroup.procs
  fi
  sudo rmdir /sys/fs/cgroup/etcd-throttle 2>/dev/null || true
  echo "throttle REMOVE on $(hostname)"
else
  echo "throttle REMOVE on $(hostname): nothing to do"
fi
REMOTE
    ;;
  *)
    echo "throttle-server: unknown action '$ACTION' (expected apply|remove)" >&2
    exit 2
    ;;
esac
