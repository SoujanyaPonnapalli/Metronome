#!/usr/bin/env bash
# Pulls all artifacts from one cell's VMs into results/<cell_id>/ then
# runs the parsers to produce results.json.
#
# Idempotent; safe to re-run.
#
# Usage:  collect-results.sh <cell_id>
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
CELL_ID="${1:?cell_id required}"
source "$PROJECT_ROOT/infra.env"

RESULTS="$PROJECT_ROOT/results/$CELL_ID"
TOPO="$RESULTS/topology.json"
[[ -f "$TOPO" ]] || { echo "no topology.json for $CELL_ID"; exit 1; }

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o LogLevel=ERROR
          -i ~/.ssh/${KEY_NAME}.pem)
SSH="ssh ${SSH_OPTS[*]}"

SERVER_PRV_IPS=($(jq -r '.servers[].private_ip' "$TOPO"))
DRIVER_PRV_IPS=($(jq -r '.drivers[].private_ip // empty' "$TOPO"))
if [[ ${#DRIVER_PRV_IPS[@]} -eq 0 ]]; then
  DRIVER_PRV_IPS=($(jq -r '.driver.private_ip' "$TOPO"))
fi

# Use PRIVATE IPs for orchestrator -> cell ssh/scp; public IPs are blocked
# by the SG (only operator-CIDR allowed). See note in run-workload.sh.

# ---- Pull per-server artifacts ----
for i in "${!SERVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  prv=${SERVER_PRV_IPS[$i]}

  # etcd log
  scp ${SSH_OPTS[*]} -q "ubuntu@$prv:/home/ubuntu/etcd.log" \
      "$RESULTS/server-${idx}-etcd.log" 2>/dev/null || true

  # System samplers (started by run-workload.sh, stopped before this script runs).
  for f in iostat vmstat mpstat pidstat; do
    scp ${SSH_OPTS[*]} -q "ubuntu@$prv:/home/ubuntu/${f}.csv" \
        "$RESULTS/server-${idx}-${f}.csv" 2>/dev/null || true
  done
  scp ${SSH_OPTS[*]} -q "ubuntu@$prv:/home/ubuntu/proposals-sampler.csv" "$RESULTS/server-${idx}-proposals-sampler.csv" 2>/dev/null || true

  # Final Prometheus snapshot. run-workload.sh already took a -pre snapshot;
  # this is the -post one. parse-bench.py diffs the two for clean per-cell deltas.
  curl -sf --max-time 5 "http://${prv}:9090/metrics" \
       > "$RESULTS/server-${idx}-metrics-post.prom" 2>/dev/null || true
  cp -f "$RESULTS/server-${idx}-metrics-post.prom" \
        "$RESULTS/server-${idx}-metrics.prom" 2>/dev/null || true
done

# ---- Pull driver artifacts (per-driver) ----
for i in "${!DRIVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  dip=${DRIVER_PRV_IPS[$i]}
  scp ${SSH_OPTS[*]} -q "ubuntu@$dip:/home/ubuntu/workload.log" \
      "$RESULTS/driver-${idx}-bench.log" 2>/dev/null || true
  for f in vmstat mpstat pidstat; do
    scp ${SSH_OPTS[*]} -q "ubuntu@$dip:/home/ubuntu/driver-${f}.csv" \
        "$RESULTS/driver-${idx}-${f}.csv" 2>/dev/null || true
  done
done
# Back-compat: parse-bench.py also looks for the singular driver-bench.log.
cp -f "$RESULTS/driver-1-bench.log" "$RESULTS/driver-bench.log"     2>/dev/null || true
cp -f "$RESULTS/driver-1-vmstat.csv"  "$RESULTS/driver-vmstat.csv"  2>/dev/null || true
cp -f "$RESULTS/driver-1-mpstat.csv"  "$RESULTS/driver-mpstat.csv"  2>/dev/null || true
cp -f "$RESULTS/driver-1-pidstat.csv" "$RESULTS/driver-pidstat.csv" 2>/dev/null || true

# ---- Parse ----
if command -v python3 >/dev/null 2>&1; then
  if [[ -x "$PROJECT_ROOT/analyze/parse-bench.py" ]]; then
    python3 "$PROJECT_ROOT/analyze/parse-bench.py" \
      --cell-id "$CELL_ID" \
      --results-dir "$RESULTS" \
      > "$RESULTS/results.json" \
      || echo "(parse-bench.py exited non-zero; manual inspection needed)"
  fi
fi

echo "collected -> $RESULTS"
ls -1 "$RESULTS"
