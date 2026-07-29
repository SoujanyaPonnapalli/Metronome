#!/usr/bin/env bash
# E0 — per-(N, value_size) concurrency calibration, with finer-grained
# sweep around the knee and a 2-driver validation phase to rule out
# client-side bottleneck.
#
# Phases:
#   1. Sweep n_clients ∈ per-value-size sets (denser near the prior knee)
#      with metronome_kf1, single driver. → find a per-(N, val) knee.
#   2. 2-driver validation at each knee: each driver runs n_clients =
#      knee/2 so total cluster concurrency stays = knee but the client
#      load is split across two VMs. If the 2-driver throughput is
#      materially higher than the 1-driver knee, the single driver was
#      bottlenecked and the knee is wrong.
#   3. Comparison cells at each (validated) knee: one vanilla + one
#      inmem so we have apples-to-apples baselines.
#
# Output:
#   results/E0-peak-loads.json    — { "3": {"1024": 800, ...}, ... }
#   results/E0-aggregate.csv      — every E0 cell row
#
# Cells: ~87 calibration + 12 validation + 24 comparison ≈ 123 cells.
# Wall: ~6 h at ~3 min/cell. Cost: ~$15 on Spot.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

NS=(3 5 7)
VALS=(1024 4096 8192 16384)
DISK=gp3-baseline

# Per-value sweep ranges — chosen to bracket the knees discovered in the
# previous E0 (v=1024→c=800, v=4096→c=200, v=8192→c=50, v=16384→c=50)
# with samples on BOTH sides of each.
sweep_for_val() {
  case "$1" in
    1024)  echo  200 400 600 800 1000 1200 1600 ;;
    4096)  echo  50  100 150 200 250  300  400 600 ;;
    8192)  echo  10  25  35  50  75   100  150 ;;
    16384) echo  10  25  35  50  75   100  150 ;;
    *) echo "bad val $1" >&2; exit 2 ;;
  esac
}

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    inmem)          echo "etcd_mode=inmem" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

refresh_e0() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E0 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)
CELL_INDEX=0

# Compute total calibration cell count for progress reporting.
CALIBRATION_TOTAL=0
for n in "${NS[@]}"; do
  for val in "${VALS[@]}"; do
    for _ in $(sweep_for_val "$val"); do
      CALIBRATION_TOTAL=$((CALIBRATION_TOTAL+1))
    done
  done
done

# ---------- Phase 1: calibration sweep ----------
echo "==== E0 phase 1: concurrency sweep ($CALIBRATION_TOTAL cells) ===="
for n in "${NS[@]}"; do
  for val in "${VALS[@]}"; do
    for c in $(sweep_for_val "$val"); do
      CELL_INDEX=$((CELL_INDEX+1))
      CELL_ID="E0-n${n}-v${val}-c${c}-metronome_kf1"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [${CELL_INDEX}/${CALIBRATION_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS=$(mode_to_vars metronome_kf1)
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$n" \
            disk_tier="$DISK" \
            value_size="$val" \
            $VARS \
            n_clients="$c"; then
        echo ">>> calibration cell FAILED ($CELL_ID) — continuing"
      fi
      refresh_e0
    done
  done
done

# ---------- Phase 2: find knees ----------
echo
echo "==== E0 phase 2: knee analysis ===="
"$PROJECT_ROOT/analyze/find-knee.py" \
  --results-dir "$PROJECT_ROOT/results" \
  --out "$PROJECT_ROOT/results/E0-peak-loads.json"

PEAK_JSON="$PROJECT_ROOT/results/E0-peak-loads.json"
[[ -f "$PEAK_JSON" ]] || { echo "missing $PEAK_JSON"; exit 4; }

# ---------- Phase 3: 2-driver validation at each knee ----------
# Each driver runs n_clients = knee/2 so total cluster concurrency stays
# at the knee. If the resulting throughput exceeds the 1-driver knee by
# more than ~10%, the single driver was the bottleneck.
echo
echo "==== E0 phase 3: 2-driver validation at each knee ===="
VAL_INDEX=0
VAL_TOTAL=$(( ${#NS[@]} * ${#VALS[@]} ))
for n in "${NS[@]}"; do
  for val in "${VALS[@]}"; do
    PEAK=$(jq -r --arg n "$n" --arg v "$val" '.[$n][$v] // empty' "$PEAK_JSON")
    if [[ -z "$PEAK" || "$PEAK" == "null" ]]; then
      echo ">>> no knee for N=$n val=$val — skipping validation"
      continue
    fi
    HALF=$(( PEAK / 2 ))
    if (( HALF < 1 )); then HALF=1; fi
    VAL_INDEX=$((VAL_INDEX+1))
    CELL_ID="E0-n${n}-v${val}-c${PEAK}-2drv-metronome_kf1"
    ELAPSED=$(( $(date +%s) - T0 ))
    echo
    echo "###############################################"
    echo "# [validation ${VAL_INDEX}/${VAL_TOTAL}, +${ELAPSED}s] $CELL_ID"
    echo "#   knee=c=${PEAK} → each of 2 drivers runs c=${HALF}"
    echo "###############################################"
    VARS=$(mode_to_vars metronome_kf1)
    if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
          "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
          n_servers="$n" \
          n_drivers=2 \
          disk_tier="$DISK" \
          value_size="$val" \
          $VARS \
          n_clients="$HALF"; then
      echo ">>> validation cell FAILED ($CELL_ID) — continuing"
    fi
    refresh_e0
  done
done

# ---------- Phase 4: comparison cells (vanilla + inmem) at each knee ----------
echo
echo "==== E0 phase 4: vanilla + inmem at each knee ===="
COMP_INDEX=0
COMP_TOTAL=$(( ${#NS[@]} * ${#VALS[@]} * 2 ))
for n in "${NS[@]}"; do
  for val in "${VALS[@]}"; do
    PEAK=$(jq -r --arg n "$n" --arg v "$val" '.[$n][$v] // empty' "$PEAK_JSON")
    [[ -z "$PEAK" || "$PEAK" == "null" ]] && continue
    for mode in vanilla inmem; do
      COMP_INDEX=$((COMP_INDEX+1))
      CELL_ID="E0-n${n}-v${val}-c${PEAK}-${mode}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [comparison ${COMP_INDEX}/${COMP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS=$(mode_to_vars "$mode")
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$n" \
            disk_tier="$DISK" \
            value_size="$val" \
            $VARS \
            n_clients="$PEAK"; then
        echo ">>> comparison cell FAILED ($CELL_ID) — continuing"
      fi
      refresh_e0
    done
  done
done

echo
echo "==== E0 sweep complete in $(( $(date +%s) - T0 ))s ===="
echo "==== peak loads ===="
cat "$PEAK_JSON"
echo
echo "==== 2-driver vs 1-driver comparison (delta > 10% = client bottleneck) ===="
"$PROJECT_ROOT/analyze/check-2drv-validation.py" \
  --results-dir "$PROJECT_ROOT/results" || true
echo
echo "Now run experiments/E1-failure-free-perf.sh"
