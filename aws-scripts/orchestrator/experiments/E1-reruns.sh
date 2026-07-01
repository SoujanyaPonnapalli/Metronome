#!/usr/bin/env bash
# E1 re-run sweep — tightens cells flagged by the variance-monitor.
#
# Reads ~/v2-rerun-candidates.txt (one cell-group per line). For each
# flagged group, runs 3 additional cells (run4..run6) so the original
# 3-run mean+σ becomes a 6-run mean+σ. With 6 samples we expect σ/μ
# to fall to ~ orig/sqrt(2) ≈ 0.71× for cells whose variance is real
# (and even lower if the single-bad-run outlier diluted the std).
#
# Reuses lib/run-cell.sh with the same vars as the original E1 cells.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

CANDIDATES="${HOME}/v2-rerun-candidates.txt"
[[ -f "$CANDIDATES" ]] || { echo "missing $CANDIDATES"; exit 2; }

EXTRA_RUNS="${EXTRA_RUNS:-3}"   # number of additional runs per flagged group
DISK=gp3-baseline               # all E1 cells are gp3-baseline

# Map mode name to terraform vars (mirror of E1-failure-free-perf.sh).
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

refresh_rollup() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E1 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

# Discover the highest existing run-number for a cell-pattern so we don't
# clobber. Pattern: results/<pattern>-run<N>.
next_run() {
  local pattern="$1"
  local max=0
  for d in "$PROJECT_ROOT/results/${pattern}"-run*; do
    [[ -d "$d" ]] || continue
    local n="${d##*-run}"
    [[ "$n" =~ ^[0-9]+$ ]] && (( n > max )) && max=$n
  done
  echo $((max + 1))
}

T0=$(date +%s)

# Parse candidates.txt — each line is tab-separated; the sample-cell-id
# at the end gives us a known-good cell pattern to mirror.
mapfile -t LINES < <(grep -E '^E1-' "$CANDIDATES")
TOTAL=$(( ${#LINES[@]} * EXTRA_RUNS ))
echo "==== E1 re-runs: ${#LINES[@]} flagged cell-groups × $EXTRA_RUNS additional runs = $TOTAL cells ===="
echo

INDEX=0
FAILED=()
for line in "${LINES[@]}"; do
  # Example line:
  # E1-n3-v4096-c200-vanilla-gp3-baseline\ttput_mean=...\tsample=E1-n3-v4096-vanilla-gp3-baseline-c200-run1
  sample=$(awk -F'\t' '{for(i=1;i<=NF;i++) if($i~/^sample=/) {sub(/^sample=/,"",$i); print $i; exit}}' <<<"$line")
  [[ -z "$sample" ]] && { echo "could not parse sample from: $line"; continue; }

  # Strip the -runN suffix from the sample cell-id to get the pattern.
  pattern="${sample%-run*}"

  # Extract n / val / mode / c from the pattern.
  # Pattern: E1-n<N>-v<VAL>-<MODE>-<DISK>-c<C>
  if ! [[ "$pattern" =~ ^E1-n([0-9]+)-v([0-9]+)-(.+)-${DISK}-c([0-9]+)$ ]]; then
    echo "could not parse fields from: $pattern"; continue
  fi
  n="${BASH_REMATCH[1]}"; val="${BASH_REMATCH[2]}"; mode="${BASH_REMATCH[3]}"; c="${BASH_REMATCH[4]}"

  start_run=$(next_run "$pattern")
  echo "--- cell-group: $pattern (will run runs ${start_run}..$((start_run + EXTRA_RUNS - 1))) ---"

  for ((r=0; r<EXTRA_RUNS; r++)); do
    INDEX=$((INDEX+1))
    runN=$((start_run + r))
    CELL_ID="${pattern}-run${runN}"
    ELAPSED=$(( $(date +%s) - T0 ))
    echo
    echo "###############################################"
    echo "# [${INDEX}/${TOTAL}, +${ELAPSED}s] $CELL_ID"
    echo "###############################################"
    VARS=$(mode_to_vars "$mode")
    if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
          "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
          n_servers="$n" \
          disk_tier="$DISK" \
          value_size="$val" \
          $VARS \
          n_clients="$c"; then
      echo ">>> cell FAILED ($CELL_ID) — continuing"
      FAILED+=("$CELL_ID")
    fi
    refresh_rollup
  done
done

refresh_rollup
echo
echo "==== E1 re-runs complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED[@]} > 0 )); then
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
