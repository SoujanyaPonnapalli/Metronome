#!/usr/bin/env bash
# E10 — Cross-AZ at N=5 and N=7 with paper-matched cluster sizes.
#
# E9 established the intra-vs-cross story at N=3. This run extends it to
# the paper's headline cluster sizes (N=5 → K=3, N=7 → K=4) to reach
# the larger K/N savings (40% / 43%) the paper exploits. Combining N=5/7
# with cross-AZ should give the closest comparison yet to the paper's
# 18-23% etcd throughput claim.
#
# Knees are REUSED from E1 / E9 (validated invariance across N at 4-16kB
# under gp3-baseline). No knee sweep phase.
#
# Matrix:
#   N       ∈ {5, 7}                              (2)
#   disk    = gp3-baseline (125 MB/s, 3k IOPS)
#   vals    ∈ {4096, 8192, 16384}                 (3)
#   modes   ∈ {vanilla, metronome_kf1}            (2)
#   deploys ∈ {intra-AZ, cross-AZ}                (2)
#   passes  = 3
#   = 72 cells total, ~4.8h wall
#
# Cross-AZ topology with 2 AZs (us-west-1a, us-west-1b), round-robin:
#   N=5: a, b, a, b, a   (3-2 split)
#   N=7: a, b, a, b, a, b, a   (4-3 split)
# Leader will be one of the AZ-a nodes.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

[[ -n "${SUBNET_ID_B:-}" ]] || {
  echo "SUBNET_ID_B not set — run infra/setup-cross-az-subnet.sh first" >&2; exit 2
}

NS=(5 7)
DISK=gp3-baseline
VALS=(4096 8192 16384)
MODES=(vanilla metronome_kf1)
DEPLOYS=(intra cross)
PASSES=3

# Cross-AZ terraform vars
AZS_JSON='["us-west-1a","us-west-1b"]'
SUBNETS_JSON="[\"$SUBNET_ID\",\"$SUBNET_ID_B\"]"

# Baked-in knees (validated across N=3/5/7 in E1/E9 at gp3-baseline).
knee_c() {
  case "$1" in
    4096)  echo 200 ;;
    8192)  echo 100 ;;
    16384) echo  50 ;;
    *) echo "bad val $1" >&2; exit 2 ;;
  esac
}

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

deploy_to_vars() {
  case "$1" in
    intra) echo "" ;;
    cross) echo "cross_az=true azs=$AZS_JSON subnet_ids=$SUBNETS_JSON" ;;
    *) echo "bad deploy $1" >&2; exit 2 ;;
  esac
}

deploy_tag() {
  case "$1" in
    intra) echo "intraAZ" ;;
    cross) echo "crossAZ" ;;
  esac
}

refresh_e10() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E10 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)
FAILED=()

TOTAL=$(( ${#NS[@]} * ${#VALS[@]} * ${#DEPLOYS[@]} * ${#MODES[@]} * PASSES ))
echo "==== E10: $TOTAL comparison cells (no knee sweep — reusing c=200/100/50) ===="

# Order: pass-major, so baseline data exists after pass 1 (~1.6h).
INDEX=0
for pass in $(seq 1 "$PASSES"); do
  for n in "${NS[@]}"; do
    for val in "${VALS[@]}"; do
      C=$(knee_c "$val")
      for deploy in "${DEPLOYS[@]}"; do
        tag=$(deploy_tag "$deploy")
        for mode in "${MODES[@]}"; do
          INDEX=$((INDEX+1))
          CELL_ID="E10-n${n}-v${val}-${tag}-${mode}-c${C}-run${pass}"
          ELAPSED=$(( $(date +%s) - T0 ))
          echo
          echo "###############################################"
          echo "# [pass${pass} ${INDEX}/${TOTAL}, +${ELAPSED}s] $CELL_ID"
          echo "###############################################"
          VARS="$(mode_to_vars "$mode") $(deploy_to_vars "$deploy")"
          if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
                "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
                n_servers="$n" disk_tier="$DISK" value_size="$val" \
                $VARS n_clients="$C"; then
            echo ">>> cell FAILED ($CELL_ID) — continuing"
            FAILED+=("$CELL_ID")
          fi
          refresh_e10
        done
      done
    done
  done
  echo "  ==== pass $pass complete ===="
done

refresh_e10
echo
echo "==== E10 complete in $(( $(date +%s) - T0 ))s ===="
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
