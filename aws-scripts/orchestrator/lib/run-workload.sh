#!/usr/bin/env bash
# Single entrypoint to drive one cell's workload. Reads
# results/<cell_id>/topology.json, scps the cluster bootstrap helper +
# the workload script to the cell, starts the etcd cluster on every
# server, and runs the workload on the driver. Captures everything to
# results/<cell_id>/.
#
# Usage:
#   run-workload.sh <cell_id> <workload_path> [extra args passed to workload]
#
# The workload script (e.g. workloads/etcd-bench-put.sh) receives:
#   $1 = comma-separated client endpoints (server private IPs:2379)
#   $2 = cell_id (so it can self-tag output)
#   $3+ = extra k=v from the experiment script (val_size, n_clients, …)
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
CELL_ID="${1:?cell_id required}"
WORKLOAD="${2:?path to workload script required}"
shift 2

source "$PROJECT_ROOT/infra.env"

RESULTS="$PROJECT_ROOT/results/$CELL_ID"
TOPO="$RESULTS/topology.json"
[[ -f "$TOPO" ]] || { echo "missing topology.json for $CELL_ID — was launch-vms.sh run?"; exit 2; }

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o LogLevel=ERROR
          -i ~/.ssh/${KEY_NAME}.pem)
SSH="ssh ${SSH_OPTS[*]}"

# ---- Parse topology ----
N=$(jq -r '.n_servers' "$TOPO")
N_DRIVERS=$(jq -r '.n_drivers // 1' "$TOPO")
ETCD_MODE=$(jq -r '.etcd_mode' "$TOPO")
METRO_OFFSET=$(jq -r '.metronome_quorum_offset // 0' "$TOPO")
SERVER_PUB_IPS=($(jq -r '.servers[].public_ip' "$TOPO"))
SERVER_PRV_IPS=($(jq -r '.servers[].private_ip' "$TOPO"))
# Multi-driver-aware: drivers[] is the authoritative array. Fall back to
# the singleton driver.private_ip for back-compat with older topology.json.
DRIVER_PRV_IPS=($(jq -r '.drivers[].private_ip // empty' "$TOPO"))
if [[ ${#DRIVER_PRV_IPS[@]} -eq 0 ]]; then
  DRIVER_PRV_IPS=($(jq -r '.driver.private_ip' "$TOPO"))
fi
DRIVER_PRV_IP="${DRIVER_PRV_IPS[0]}"   # back-compat alias for code paths that still use the singular

# IMPORTANT: orchestrator <-> cell SSH must use PRIVATE IPs. The SG only
# opens public-IP:22 to the operator laptop CIDR (76.14.5.183/32), not to
# the orchestrator's public IP — so orchestrator -> cell over public IPs
# is blocked by AWS and times out. Private IPs flow through SG-self and
# work. See the audit at the bottom of this file's history.

# ---- Wait for all hosts to be SSH-ready ----
# Cloud-init always writes /home/ubuntu/READY with one of:
#   status=ok reason=done @ <ts>
#   status=failed reason=<which-step> @ <ts>
wait_ready() {
  local ip="$1" name="$2"
  local marker=""
  for i in $(seq 1 144); do        # 144 * 5s = 12 min ceiling
    marker=$($SSH "ubuntu@$ip" 'cat /home/ubuntu/READY 2>/dev/null' 2>/dev/null || true)
    if [[ -n "$marker" ]]; then
      if [[ "$marker" == status=ok* ]]; then
        echo "  $name ($ip) ready -- $marker"
        return 0
      else
        echo "  $name ($ip) FAILED -- $marker"
        echo "  ---- bootstrap log from $name ----"
        $SSH "ubuntu@$ip" 'sudo tail -n 80 /var/log/server-bootstrap.log 2>/dev/null \
                          || sudo tail -n 80 /var/log/driver-bootstrap.log 2>/dev/null \
                          || sudo tail -n 80 /var/log/cloud-init-output.log' \
            2>/dev/null | sed 's/^/    /'
        echo "  ---- end bootstrap log ----"
        exit 3
      fi
    fi
    sleep 5
  done
  echo "  $name ($ip) never wrote READY in 12 min (private-IP SSH timed out)"
  exit 3
}
echo "waiting for cell VMs (via private IPs) ..."
for i in "${!SERVER_PRV_IPS[@]}"; do
  wait_ready "${SERVER_PRV_IPS[$i]}" "server-$((i+1))"
done
for i in "${!DRIVER_PRV_IPS[@]}"; do
  wait_ready "${DRIVER_PRV_IPS[$i]}" "driver-$((i+1))"
done

# ---- Network preflight: explicit IP-visibility + reachability matrix ----
# Catches firewall / routing / DNS regressions early — before we spend any
# time scp'ing binaries. Tests every orchestrator->VM and VM->VM pair on
# the relevant ports.
echo "network preflight: VM-to-VM reachability ..."
PEER_PRV_IPS=("${SERVER_PRV_IPS[@]}" "${DRIVER_PRV_IPS[@]}")
PEER_NAMES=()
for i in "${!SERVER_PRV_IPS[@]}"; do PEER_NAMES+=("server-$((i+1))"); done
for i in "${!DRIVER_PRV_IPS[@]}"; do PEER_NAMES+=("driver-$((i+1))"); done

# 1. Orchestrator -> each VM (port 22). Already proven by wait_ready, but
#    surface it in the log for the audit trail.
echo "  orchestrator -> VM via port 22:"
for i in "${!PEER_PRV_IPS[@]}"; do
  if nc -z -w 3 "${PEER_PRV_IPS[$i]}" 22 2>/dev/null; then
    echo "    OK   ${PEER_NAMES[$i]} (${PEER_PRV_IPS[$i]}):22"
  else
    echo "    FAIL ${PEER_NAMES[$i]} (${PEER_PRV_IPS[$i]}):22"
    exit 6
  fi
done

# 2. Each VM -> every other VM on port 22 (proves SG-self covers intra-cell
#    SSH which etcd's --initial-cluster handshake does NOT use but is a
#    useful canary that VM-to-VM TCP works at all).
echo "  VM-to-VM port 22 matrix (each row = one source VM):"
for i in "${!PEER_PRV_IPS[@]}"; do
  src_ip="${PEER_PRV_IPS[$i]}"
  src_name="${PEER_NAMES[$i]}"
  row=$($SSH "ubuntu@$src_ip" "
    for tgt in ${PEER_PRV_IPS[*]}; do
      if [ \"\$tgt\" = \"$src_ip\" ]; then printf '%s' .; continue; fi
      if nc -z -w 2 \$tgt 22 2>/dev/null; then printf '%s' o; else printf '%s' x; fi
    done; echo
  " 2>/dev/null)
  echo "    $src_name ($src_ip): $row"
  if [[ "$row" =~ x ]]; then
    echo "  network preflight FAILED: $src_name cannot reach some peer on port 22"
    exit 6
  fi
done
echo "  network preflight passed."

# ---- Stage etcd binaries ----
# Etcd binaries are expected in /opt/metronome-eval/bin/ on the
# orchestrator. They're built once by scripts/build-binaries.sh.
ETCD_BIN_LOCAL=""
case "$ETCD_MODE" in
  vanilla)   ETCD_BIN_LOCAL=/opt/metronome-eval/bin/etcd-vanilla ;;
  metronome) ETCD_BIN_LOCAL=/opt/metronome-eval/bin/etcd-metronome ;;
  inmem)     ETCD_BIN_LOCAL=/opt/metronome-eval/bin/etcd-metronome ;;  # same binary; flag toggles
  *) echo "unknown etcd_mode $ETCD_MODE"; exit 4 ;;
esac
[[ -x "$ETCD_BIN_LOCAL" ]] || { echo "missing $ETCD_BIN_LOCAL — run scripts/build-binaries.sh first"; exit 5; }

echo "scp etcd binary to servers (via private IPs) ..."
for ip in "${SERVER_PRV_IPS[@]}"; do
  scp ${SSH_OPTS[*]} -q "$ETCD_BIN_LOCAL" "ubuntu@$ip:/tmp/etcd"
  $SSH "ubuntu@$ip" 'sudo mv /tmp/etcd /usr/local/bin/etcd && sudo chmod +x /usr/local/bin/etcd'
done

# Also scp the etcd benchmark binary to the driver.
[[ -x /opt/metronome-eval/bin/etcd-benchmark ]] \
  || { echo "missing etcd-benchmark — run scripts/build-binaries.sh first"; exit 5; }
echo "scp etcd-benchmark to ${#DRIVER_PRV_IPS[@]} driver(s) ..."
for dip in "${DRIVER_PRV_IPS[@]}"; do
  scp ${SSH_OPTS[*]} -q /opt/metronome-eval/bin/etcd-benchmark "ubuntu@$dip:/tmp/etcd-benchmark"
  $SSH "ubuntu@$dip" 'sudo mv /tmp/etcd-benchmark /usr/local/bin/etcd-benchmark && sudo chmod +x /usr/local/bin/etcd-benchmark'
done

# ---- Start etcd cluster ----
INITIAL_CLUSTER=""
for i in "${!SERVER_PRV_IPS[@]}"; do
  name="n$((i+1))"
  url="http://${SERVER_PRV_IPS[$i]}:2380"
  if [[ -n "$INITIAL_CLUSTER" ]]; then INITIAL_CLUSTER="$INITIAL_CLUSTER,"; fi
  INITIAL_CLUSTER="${INITIAL_CLUSTER}${name}=${url}"
done

# Optional mode flag.
MODE_FLAG=""
case "$ETCD_MODE" in
  metronome)
    MODE_FLAG="--metronome --metronome-work-steal-timeout=300s"
    # K = ceil(N/2) + 1 + offset; offset == 0 means default (f+1), so omit the flag.
    if [[ "${METRO_OFFSET:-0}" -gt 0 ]]; then
      DEFAULT_K=$(( N / 2 + 1 ))                      # ceil for odd N (paper-standard cluster sizes)
      K=$(( DEFAULT_K + METRO_OFFSET ))
      if (( K > N )); then K=$N; fi                   # clamp; should never trip for our matrix
      MODE_FLAG="${MODE_FLAG} --metronome-quorum-size=${K}"
      echo "  K=${K} (N=${N}, offset=${METRO_OFFSET})"
    fi
    ;;
  inmem)     MODE_FLAG="--experimental-in-mem-only" ;;
esac

echo "starting etcd on each server (mode=$ETCD_MODE) ..."
for i in "${!SERVER_PRV_IPS[@]}"; do
  prv=${SERVER_PRV_IPS[$i]}
  name="n$((i+1))"
  $SSH "ubuntu@$prv" bash -s <<EOF
set -e
sudo pkill -9 -f 'etcd --name=' || true
sudo rm -rf /var/lib/etcd/n${i}
sudo mkdir -p /var/lib/etcd/n${i}
sudo chown -R ubuntu:ubuntu /var/lib/etcd
nohup /usr/local/bin/etcd --name=${name} ${MODE_FLAG} \
  --data-dir=/var/lib/etcd/${name} \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://${prv}:2379 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --initial-advertise-peer-urls=http://${prv}:2380 \
  --initial-cluster=${INITIAL_CLUSTER} \
  --listen-metrics-urls=http://0.0.0.0:9090 \
  --log-level=warn > /home/ubuntu/etcd.log 2>&1 &
echo \$! > /home/ubuntu/etcd.pid
EOF
done
sleep $((3 + N))

# Verify cluster has a leader.
ENDPOINTS=$(IFS=,; addrs=("${SERVER_PRV_IPS[@]/%/:2379}"); IFS=,; echo "${addrs[*]/#/http://}")
# (build comma-separated http://ip:2379 endpoints)
ENDPOINTS=$(printf "http://%s:2379," "${SERVER_PRV_IPS[@]}" | sed 's/,$//')

# ---- Start system samplers on each server ----
# We want enough bookkeeping that we can defend the c6in.2xlarge choice:
#   iostat   - per-device read/write throughput + util%  (proves disk is the bottleneck)
#   vmstat   - whole-system CPU + mem + IO at 1s         (proves CPU is NOT the bottleneck)
#   mpstat   - per-CPU breakdown                          (catches a single-core stall)
#   pidstat  - per-process CPU + RSS + IO for etcd        (the bottleneck-of-interest)
# All four are dirt-cheap; sysstat is already installed by cloud-init.
echo "starting system samplers ..."
for ip in "${SERVER_PRV_IPS[@]}"; do
  $SSH "ubuntu@$ip" bash -s <<'SAMPLERS'
set -e
pkill -f 'iostat -xyt' 2>/dev/null || true
pkill -f 'vmstat'      2>/dev/null || true
pkill -f 'mpstat'      2>/dev/null || true
pkill -f 'pidstat'     2>/dev/null || true
rm -f /home/ubuntu/iostat.csv /home/ubuntu/vmstat.csv /home/ubuntu/mpstat.csv /home/ubuntu/pidstat.csv
nohup iostat -xyt 1                                 > /home/ubuntu/iostat.csv  2>&1 &
nohup vmstat -t -n 1                                > /home/ubuntu/vmstat.csv  2>&1 &
nohup mpstat -P ALL 1                               > /home/ubuntu/mpstat.csv  2>&1 &
# proposals_pending sampler @ 200ms (for Little's-law-based wait-for-quorum inference)
pkill -f 'proposals-sampler.sh' 2>/dev/null || true
rm -f /home/ubuntu/proposals-sampler.csv
cat > /tmp/proposals-sampler.sh <<'SAMPLER_BODY'
#!/usr/bin/env bash
echo "ts,pending,committed,applied"
while true; do
  ts=$(date +%s.%N)
  m=$(curl -sf --max-time 1 http://localhost:9090/metrics 2>/dev/null)
  p=$(printf '%s\n' "$m" | awk '/^etcd_server_proposals_pending / {print $2; exit}')
  c=$(printf '%s\n' "$m" | awk '/^etcd_server_proposals_committed_total / {print $2; exit}')
  a=$(printf '%s\n' "$m" | awk '/^etcd_server_proposals_applied_total / {print $2; exit}')
  echo "${ts},${p},${c},${a}"
  sleep 0.2
done
SAMPLER_BODY
chmod +x /tmp/proposals-sampler.sh
nohup /tmp/proposals-sampler.sh > /home/ubuntu/proposals-sampler.csv 2>&1 &

ETCDPID=$(pgrep -f '/usr/local/bin/etcd --name=' | head -1)
if [ -n "$ETCDPID" ]; then
  nohup pidstat -h -u -r -d -p "$ETCDPID" 1         > /home/ubuntu/pidstat.csv 2>&1 &
fi
SAMPLERS
done

# ---- Pre-snapshot of Prometheus (so we can compute exact per-cell deltas) ----
echo "pre-snapshot of Prometheus per server ..."
for i in "${!SERVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  prv=${SERVER_PRV_IPS[$i]}
  curl -sf --max-time 5 "http://${prv}:9090/metrics" \
       > "$RESULTS/server-${idx}-metrics-pre.prom" 2>/dev/null || true
done

# ---- Optional disk-throttle hook (E3 stragglers) ----
# Set STRAGGLER_INDEX=<1-based server index> + STRAGGLER_WBPS=<bytes/sec>
# to throttle one follower's data-volume write bandwidth via cgroup v2.
# Applied AFTER the Prom pre-snapshot so the resulting delta captures
# the throttled period in full. Cleanup is best-effort via EXIT trap; VM
# is destroyed at cell-end regardless.
if [[ -n "${STRAGGLER_INDEX:-}" && -n "${STRAGGLER_WBPS:-}" && "${STRAGGLER_WBPS:-0}" -gt 0 ]]; then
  echo "applying disk throttle: server $STRAGGLER_INDEX wbps=$STRAGGLER_WBPS"
  "$HERE/throttle-server.sh" apply "$CELL_ID" "$STRAGGLER_INDEX" "$STRAGGLER_WBPS"
  trap "'$HERE/throttle-server.sh' remove '$CELL_ID' '$STRAGGLER_INDEX' >/dev/null 2>&1 || true" EXIT
fi

# ---- Start driver-side samplers on every driver ----
# Captures each client's CPU + RAM during the workload. If a driver's CPU
# is pegged we know our throughput numbers are client-limited rather than
# cluster-limited. Each driver writes to its own
# /home/ubuntu/driver-{vmstat,mpstat,pidstat}.csv; collect-results.sh
# renames them to driver-N-* on the orchestrator side.
for dip in "${DRIVER_PRV_IPS[@]}"; do
  $SSH "ubuntu@$dip" bash -s <<'DSAMPLERS'
set -e
pkill -f 'vmstat'  2>/dev/null || true
pkill -f 'mpstat'  2>/dev/null || true
pkill -f 'pidstat' 2>/dev/null || true
rm -f /home/ubuntu/driver-{vmstat,mpstat,pidstat}.csv
nohup vmstat -t -n 1   > /home/ubuntu/driver-vmstat.csv 2>&1 &
nohup mpstat -P ALL 1  > /home/ubuntu/driver-mpstat.csv 2>&1 &
(
  for i in $(seq 1 60); do
    BPID=$(pgrep -x etcd-benchmark | head -1)
    if [ -n "$BPID" ]; then
      pidstat -h -u -r -d -p "$BPID" 1 > /home/ubuntu/driver-pidstat.csv 2>&1
      break
    fi
    sleep 1
  done
) &
DSAMPLERS
done

# ---- Run the workload on every driver in parallel ----
# Each driver runs the same workload independently against the same
# endpoints. parse-bench.py aggregates throughput across drivers.
echo "running workload on ${#DRIVER_PRV_IPS[@]} driver(s) in parallel ..."
for i in "${!DRIVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  dip=${DRIVER_PRV_IPS[$i]}
  scp ${SSH_OPTS[*]} -q "$WORKLOAD" "ubuntu@$dip:/tmp/workload.sh"
done

# Launch all drivers in the background and wait for them to finish.
DRIVER_PIDS=()
for i in "${!DRIVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  dip=${DRIVER_PRV_IPS[$i]}
  (
    $SSH "ubuntu@$dip" "chmod +x /tmp/workload.sh && /tmp/workload.sh '$ENDPOINTS' '$CELL_ID' $@ > /home/ubuntu/workload.log 2>&1"
  ) &
  DRIVER_PIDS+=($!)
done
for pid in "${DRIVER_PIDS[@]}"; do
  wait "$pid" || echo "(driver pid $pid exited non-zero — continuing)"
done

# ---- Pull each driver's workload log ----
for i in "${!DRIVER_PRV_IPS[@]}"; do
  idx=$((i+1))
  dip=${DRIVER_PRV_IPS[$i]}
  scp ${SSH_OPTS[*]} -q "ubuntu@$dip:/home/ubuntu/workload.log" \
      "$RESULTS/driver-${idx}-bench.log" 2>/dev/null || true
done
# Back-compat: parse-bench.py still expects driver-bench.log if there's
# only one driver. Hard-link to the multi-driver name.
cp -f "$RESULTS/driver-1-bench.log" "$RESULTS/driver-bench.log" 2>/dev/null || true

# ---- Stop samplers on every VM (so collect-results.sh sees finalized files) ----
echo "stopping system samplers ..."
for ip in "${SERVER_PRV_IPS[@]}"; do
  $SSH "ubuntu@$ip" "pkill -f 'iostat -xyt' 2>/dev/null; pkill -f 'vmstat' 2>/dev/null; pkill -f 'mpstat' 2>/dev/null; pkill -f 'pidstat' 2>/dev/null; pkill -f 'proposals-sampler.sh' 2>/dev/null; true"
done
for dip in "${DRIVER_PRV_IPS[@]}"; do
  $SSH "ubuntu@$dip" "pkill -f 'vmstat' 2>/dev/null; pkill -f 'mpstat' 2>/dev/null; pkill -f 'pidstat' 2>/dev/null; true"
done

# Remove the throttle on the straggler (if any). VM is destroyed at
# cell-end so this is purely cosmetic; on success it lets samplers
# capture a few seconds of post-throttle baseline if desired.
if [[ -n "${STRAGGLER_INDEX:-}" ]]; then
  "$HERE/throttle-server.sh" remove "$CELL_ID" "$STRAGGLER_INDEX" >/dev/null 2>&1 || true
  trap - EXIT
fi

echo "workload complete; driver-bench.log saved"
