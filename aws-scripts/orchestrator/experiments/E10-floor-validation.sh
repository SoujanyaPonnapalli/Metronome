#!/usr/bin/env bash
# E10 floor-validation microbench.
#
# Purpose: directly MEASURE the WAL fsync floor cost (syscall + EBS write-
# barrier) by running at val=1B + low concurrency, where per-fsync byte
# content is so small that the bytes-portion of fsync time collapses to
# near zero. The remaining per-call fsync time IS the floor.
#
# Expected result (from linear fit on bigger val sizes):
#   intra-AZ floor ≈ 1.20 ms
#   cross-AZ floor ≈ 1.44 ms
#
# If measured per-call fsync at val=1B matches these within ~10%, the
# floor is empirically grounded (not a fitted artifact).
#
# Cells (4):
#   E10fv-n5-v1-{intraAZ,crossAZ}-{vanilla,metronome_kf1}-c10-run1
# ~5 min each ⇒ ~20 min wall.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

[[ -n "${SUBNET_ID_B:-}" ]] || {
  echo "SUBNET_ID_B not set" >&2; exit 2
}

N=5
DISK=gp3-baseline
VAL=1
C=10
MODES=(vanilla metronome_kf1)
DEPLOYS=(intra cross)

AZS_JSON='["us-west-1a","us-west-1b"]'
SUBNETS_JSON="[\"$SUBNET_ID\",\"$SUBNET_ID_B\"]"

mode_to_vars() {
  case "$1" in
    vanilla)       echo "etcd_mode=vanilla" ;;
    metronome_kf1) echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
  esac
}
deploy_to_vars() {
  case "$1" in
    intra) echo "" ;;
    cross) echo "cross_az=true azs=$AZS_JSON subnet_ids=$SUBNETS_JSON" ;;
  esac
}
deploy_tag() {
  case "$1" in
    intra) echo "intraAZ" ;;
    cross) echo "crossAZ" ;;
  esac
}

refresh() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E10fv \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)
FAILED=()
TOTAL=$(( ${#MODES[@]} * ${#DEPLOYS[@]} ))
INDEX=0

echo "==== E10 floor-validation microbench: $TOTAL cells ===="
echo "==== val=${VAL}B, c=${C}, N=${N}, ${TOTAL_MEASURE:-default} ops per cell ===="

for deploy in "${DEPLOYS[@]}"; do
  tag=$(deploy_tag "$deploy")
  DEPLOY_VARS=$(deploy_to_vars "$deploy")
  for mode in "${MODES[@]}"; do
    INDEX=$((INDEX+1))
    CELL_ID="E10fv-n${N}-v${VAL}-${tag}-${mode}-c${C}-run1"
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
    refresh
  done
done

refresh
echo
echo "==== E10 floor-validation complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED[@]} > 0 )); then
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
