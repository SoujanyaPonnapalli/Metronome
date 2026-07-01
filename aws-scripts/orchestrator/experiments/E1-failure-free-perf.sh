#!/usr/bin/env bash
# E1 - Failure-free performance: full K sweep at per-(N, value_size)
# saturation concurrency.
#
# Matrix:
#   N           ∈ {3, 5, 7}
#   value_size  ∈ {1024, 4096, 8192, 16384}  bytes
#   modes       ∈ {vanilla, inmem} + every valid K ∈ [f+1, N]:
#     N=3, f=1: kf1 (K=2), kf2 (K=3)              -> 2 metronome modes
#     N=5, f=2: kf1 (K=3), kf2 (K=4), kf3 (K=5)    -> 3 metronome modes
#     N=7, f=3: kf1 (K=4), kf2 (K=5), kf3 (K=6), kf4 (K=7) -> 4 metronome modes
#   disk_tier   = gp3-baseline
#   n_clients   = per-(N, val) saturation concurrency from results/E0-peak-loads.json
#   runs/cell   = 3
#
# Cells:
#   N=3 -> 4 modes × 4 val × 3 runs = 48
#   N=5 -> 5 modes × 4 val × 3 runs = 60
#   N=7 -> 6 modes × 4 val × 3 runs = 72
#   Total: 180
# Wall: ~9 h. Cost: ~$26 on Spot.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

NS=(3 5 7)
VALS=(1024 4096 8192 16384)
DISK=gp3-baseline
RUNS_PER_CELL=3

PEAK_JSON="$PROJECT_ROOT/results/E0-peak-loads.json"
[[ -f "$PEAK_JSON" ]] || {
  echo "missing $PEAK_JSON — run experiments/E0-concurrency-calibration.sh first" >&2
  exit 2
}

# Friendly mode -> terraform vars.
# metronome_kf<i>: K = f+i. We pass metronome_quorum_offset = i-1 (default
# selects f+1 when offset=0). For kf<i> where i == f+1 (i.e. K=N), this is
# equivalent to vanilla persistence patterns but exercises the metronome
# code path — useful as a sanity / overhead check.
mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    inmem)          echo "etcd_mode=inmem" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    metronome_kf2)  echo "etcd_mode=metronome metronome_quorum_offset=1" ;;
    metronome_kf3)  echo "etcd_mode=metronome metronome_quorum_offset=2" ;;
    metronome_kf4)  echo "etcd_mode=metronome metronome_quorum_offset=3" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

# Build the per-N mode list: vanilla + every valid K ∈ [f+1, N].
# Inmem is dropped from v2: we already know from v1 that inmem ≈ disk-free
# upper bound (+70..+170% over vanilla); the K-sweep + vanilla comparison
# is what we actually need for paper claims.
modes_for_n() {
  local n="$1"
  local f=$(( n / 2 ))      # f = floor(n/2)
  local out=(vanilla)
  local i
  for (( i=1; i<=f+1; i++ )); do        # i from 1 to f+1: K = f+i ∈ [f+1, 2f+1]
    local k=$(( f + i ))
    if (( k > n )); then break; fi
    out+=("metronome_kf${i}")
  done
  echo "${out[@]}"
}

refresh_rollup() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E1 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
  "$PROJECT_ROOT/analyze/plot-results.py" \
    --experiment E1 --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

# Pre-flight: print the sweep we're about to run.
TOTAL_CELLS=0
for n in "${NS[@]}"; do
  modes=( $(modes_for_n "$n") )
  for val in "${VALS[@]}"; do
    PEAK=$(jq -r --arg n "$n" --arg v "$val" '.[$n][$v] // empty' "$PEAK_JSON")
    if [[ -z "$PEAK" || "$PEAK" == "null" ]]; then
      echo "WARN: no E0 knee for N=$n val=$val — those cells will be SKIPPED" >&2
      continue
    fi
    for mode in "${modes[@]}"; do
      for run in $(seq 1 "$RUNS_PER_CELL"); do
        TOTAL_CELLS=$((TOTAL_CELLS+1))
      done
    done
  done
done
echo "==== E1 planning: $TOTAL_CELLS cells across modes ===="
for n in "${NS[@]}"; do
  echo "  N=$n  modes: $(modes_for_n "$n")"
done

# Main sweep.
T0=$(date +%s)
CELL_INDEX=0
FAILED_CELLS=()
CONSECUTIVE_FAILS=0
MAX_CONSECUTIVE_FAILS=5

for n in "${NS[@]}"; do
  modes=( $(modes_for_n "$n") )
  for val in "${VALS[@]}"; do
    PEAK=$(jq -r --arg n "$n" --arg v "$val" '.[$n][$v] // empty' "$PEAK_JSON")
    if [[ -z "$PEAK" || "$PEAK" == "null" ]]; then
      continue
    fi
    for mode in "${modes[@]}"; do
      for run in $(seq 1 "$RUNS_PER_CELL"); do
        CELL_INDEX=$((CELL_INDEX+1))
        CELL_ID="E1-n${n}-v${val}-${mode}-${DISK}-c${PEAK}-run${run}"
        ELAPSED=$(( $(date +%s) - T0 ))
        echo
        echo "###############################################"
        echo "# [${CELL_INDEX}/${TOTAL_CELLS}, +${ELAPSED}s] $CELL_ID"
        echo "###############################################"
        VARS=$(mode_to_vars "$mode")
        if "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
              "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
              n_servers="$n" \
              disk_tier="$DISK" \
              value_size="$val" \
              $VARS \
              n_clients="$PEAK"; then
          CONSECUTIVE_FAILS=0
        else
          rc=$?
          echo ">>> CELL FAILED ($CELL_ID) rc=$rc — continuing"
          FAILED_CELLS+=("$CELL_ID")
          CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS+1))
          if (( CONSECUTIVE_FAILS >= MAX_CONSECUTIVE_FAILS )); then
            echo ">>> $MAX_CONSECUTIVE_FAILS consecutive failures — aborting sweep"
            break 4
          fi
        fi
        refresh_rollup
      done
    done
  done
done

refresh_rollup
echo
echo "==== E1 sweep complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED_CELLS[@]} > 0 )); then
  echo ">>> sweep completed with ${#FAILED_CELLS[@]} failed cells:"
  printf '    - %s\n' "${FAILED_CELLS[@]}"
fi
