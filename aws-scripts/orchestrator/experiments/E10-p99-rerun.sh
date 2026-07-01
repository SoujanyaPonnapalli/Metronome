#!/usr/bin/env bash
# E10 p99 rerun — additional 3 runs (tagged run4/5/6) for the two cells
# that showed unexpected p99 increases in E10:
#   1. N=7 v=4kB intra-AZ  (vanilla p99 36.4ms vs kf1 38.2ms; +4.9%)
#   2. N=5 v=4kB cross-AZ  (vanilla p99 37.9ms vs kf1 42.3ms; +11.4%,
#                           kf1 run3 had p99=50.6ms — looks like cross-AZ blip)
#
# Cells produced (12 total, ~50min):
#   E10-n7-v4096-intraAZ-{vanilla,metronome_kf1}-c200-run{4,5,6}
#   E10-n5-v4096-crossAZ-{vanilla,metronome_kf1}-c200-run{4,5,6}
#
# The new rows merge into the existing E10 aggregate, giving 6-run means
# for those (config × mode) combinations.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

[[ -n "${SUBNET_ID_B:-}" ]] || {
  echo "SUBNET_ID_B not set" >&2; exit 2
}

DISK=gp3-baseline
VAL=4096
C=200
MODES=(vanilla metronome_kf1)
RUNS=(4 5 6)

AZS_JSON='["us-west-1a","us-west-1b"]'
SUBNETS_JSON="[\"$SUBNET_ID\",\"$SUBNET_ID_B\"]"

mode_to_vars() {
  case "$1" in
    vanilla)       echo "etcd_mode=vanilla" ;;
    metronome_kf1) echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
  esac
}

refresh_e10() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E10 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

# (N, deploy, deploy_tag, extra_vars)
declare -a CONFIGS=(
  "7 intra intraAZ"
  "5 cross crossAZ"
)

deploy_to_vars() {
  case "$1" in
    intra) echo "" ;;
    cross) echo "cross_az=true azs=$AZS_JSON subnet_ids=$SUBNETS_JSON" ;;
  esac
}

T0=$(date +%s)
FAILED=()
TOTAL=$(( ${#CONFIGS[@]} * ${#MODES[@]} * ${#RUNS[@]} ))
INDEX=0

echo "==== E10 p99 rerun: $TOTAL cells ===="

for cfg in "${CONFIGS[@]}"; do
  read -r N DEPLOY TAG <<< "$cfg"
  DEPLOY_VARS=$(deploy_to_vars "$DEPLOY")
  for run in "${RUNS[@]}"; do
    for mode in "${MODES[@]}"; do
      INDEX=$((INDEX+1))
      CELL_ID="E10-n${N}-v${VAL}-${TAG}-${mode}-c${C}-run${run}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [${INDEX}/${TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS="$(mode_to_vars "$mode") $DEPLOY_VARS"
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$N" disk_tier="$DISK" value_size="$VAL" \
            $VARS n_clients="$C"; then
        echo ">>> cell FAILED ($CELL_ID) — continuing"
        FAILED+=("$CELL_ID")
      fi
      refresh_e10
    done
  done
done

refresh_e10
echo
echo "==== E10 p99 rerun complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED[@]} > 0 )); then
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
