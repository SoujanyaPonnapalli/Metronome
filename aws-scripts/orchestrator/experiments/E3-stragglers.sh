#!/usr/bin/env bash
# E3 — Disk stragglers / work-stealing.
#
# Thesis: a metronome cluster tolerates a slow follower better than
# vanilla because (a) the persist-set rotation lets the cluster commit
# without waiting on the straggler, and (b) when the straggler holds
# uncommitted entries long enough, peers with matching memory state
# steal the write and persist on its behalf.
#
# Method: launch a homogeneous gp3-baseline cluster, then artificially
# throttle ONE follower's data-volume write bandwidth via cgroup v2
# io.max. Compare vanilla vs metronome_kf1 at the same workload.
#
# Cells:
#   modes:           vanilla, metronome_kf1                (2)
#   throttle levels: none, 50 MB/s, 25 MB/s, 10 MB/s        (4)
#   runs:                                                   2
#   Total:                                                  2 × 4 × 2 = 16
# Wall: ~1 h on Spot. Cost: ~$2.
#
# Headline observations to look for:
#   - Throughput cliff for vanilla as throttle tightens.
#   - Metronome holds throughput much further down.
#   - etcd_metronome_work_steals_triggered_total > 0 on at least one
#     peer at the tighter throttle levels (workload longer than the
#     1s WS arming timeout).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

N=3
VAL=4096
DISK=gp3-baseline
C=200            # E0 knee for v=4096 / gp3-baseline
STRAGGLER_IDX=2  # 1-based; servers are n1 (leader bias), n2, n3 → pick n2
RUNS_PER_CELL=2

# Throttle levels in bytes/sec. "0" means no throttle (baseline).
LEVELS=(0 50000000 25000000 10000000)
LEVEL_LABEL() {
  case "$1" in
    0)        echo "none" ;;
    10000000) echo "10mbs" ;;
    25000000) echo "25mbs" ;;
    50000000) echo "50mbs" ;;
    *)        echo "${1}bps" ;;
  esac
}

MODES=(vanilla metronome_kf1)
mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

refresh_e3() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E3 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

TOTAL=$(( ${#MODES[@]} * ${#LEVELS[@]} * RUNS_PER_CELL ))
T0=$(date +%s)
CELL_INDEX=0
FAILED=()

echo "==== E3 stragglers: $TOTAL cells (N=$N val=$VAL c=$C disk=$DISK straggler=n${STRAGGLER_IDX}) ===="
for level in "${LEVELS[@]}"; do
  label=$(LEVEL_LABEL "$level")
  for mode in "${MODES[@]}"; do
    for run in $(seq 1 "$RUNS_PER_CELL"); do
      CELL_INDEX=$((CELL_INDEX+1))
      CELL_ID="E3-n${N}-v${VAL}-${DISK}-c${C}-straggler-${label}-${mode}-run${run}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [${CELL_INDEX}/${TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "#   straggler: server #${STRAGGLER_IDX} wbps=${level} (${label})"
      echo "###############################################"
      VARS=$(mode_to_vars "$mode")

      if [[ "$level" == "0" ]]; then
        # Baseline: no throttle env vars.
        if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
              "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
              n_servers="$N" \
              disk_tier="$DISK" \
              value_size="$VAL" \
              $VARS \
              n_clients="$C"; then
          echo ">>> cell FAILED ($CELL_ID) — continuing"
          FAILED+=("$CELL_ID")
        fi
      else
        # Throttle one follower via env vars consumed by run-workload.sh.
        if ! STRAGGLER_INDEX="$STRAGGLER_IDX" STRAGGLER_WBPS="$level" \
              "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
              "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
              n_servers="$N" \
              disk_tier="$DISK" \
              value_size="$VAL" \
              $VARS \
              n_clients="$C"; then
          echo ">>> cell FAILED ($CELL_ID) — continuing"
          FAILED+=("$CELL_ID")
        fi
      fi
      refresh_e3
    done
  done
done

refresh_e3
echo
echo "==== E3 stragglers complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED[@]} > 0 )); then
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
echo
echo "Inspect:"
echo "  results/E3-aggregate.csv     - throughput / p99 / fsync delta by mode×throttle"
echo "  grep etcd_metronome_work_steals_triggered_total results/E3-*/server-*-metrics-post.prom"
