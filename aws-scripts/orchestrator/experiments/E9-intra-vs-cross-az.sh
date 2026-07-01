#!/usr/bin/env bash
# E9 — Intra-AZ vs cross-AZ comparison at gp3-baseline, N=3.
#
# Reduced matrix focused on getting the cross-AZ vs intra-AZ comparison
# clean across value sizes. The hypothesis from our prediction model is
# that cross-AZ should boost metronome's win because RTT (~1-2ms in
# us-west-1) brings follower fsync onto the critical path under vanilla,
# and metronome removes it.
#
# Matrix (per pass):
#   N       = 3
#   disk    = gp3-baseline (125 MB/s, 3k IOPS)
#   vals    ∈ {1024, 4096, 8192, 16384, 32768}      (5)
#   modes   ∈ {vanilla, metronome_kf1}              (2)
#   deploys ∈ {intra-AZ, cross-AZ}                  (2)
#   = 20 cell-configs per pass
#
# Phases:
#   1. Per-(val, deploy) concurrency sweep with metronome_kf1
#      to find each operating point's saturation knee.
#   2. Comparison: vanilla + metronome_kf1 × 3 passes at each knee.
#
# Cells:
#   sweep:     5 vals × 2 deploys × 3 c-points × kf1   = 30
#   compare:   20 configs × 3 runs × 2 modes            = 120 (60 cells × 2 ... see below)
#   Actually:  20 configs × 1 run each, × 3 passes      = 60 cells total
#   Total:                                                90 cells, ~6h wall
#
# Order: knee sweep → pass 1 (all 20 configs) → pass 2 → pass 3, so
# baseline data is available after pass 1 (~3.5h).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

[[ -n "${SUBNET_ID_B:-}" ]] || {
  echo "SUBNET_ID_B not set — run infra/setup-cross-az-subnet.sh first" >&2; exit 2
}

N=3
DISK=gp3-baseline
VALS=(1024 4096 8192 16384 32768)
MODES=(vanilla metronome_kf1)
DEPLOYS=(intra cross)
PASSES=3

# Cross-AZ terraform vars
AZS_JSON='["us-west-1a","us-west-1b"]'
SUBNETS_JSON="[\"$SUBNET_ID\",\"$SUBNET_ID_B\"]"

# Per-(val, deploy) knee-sweep c-points.
sweep_c() {
  local val="$1" deploy="$2"
  case "${val}_${deploy}" in
    1024_intra)    echo  600 1200 2000 ;;
    1024_cross)    echo  800 1600 2400 ;;
    4096_intra)    echo  100  200  400 ;;
    4096_cross)    echo  200  400  800 ;;
    8192_intra)    echo   50  100  200 ;;
    8192_cross)    echo  100  200  400 ;;
    16384_intra)   echo   25   50  100 ;;
    16384_cross)   echo   50  100  200 ;;
    32768_intra)   echo   25   50  100 ;;
    32768_cross)   echo   50  100  200 ;;
    *) echo "bad (val,deploy)=$val,$deploy" >&2; exit 2 ;;
  esac
}

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

# Deploy mode -> terraform var additions
deploy_to_vars() {
  case "$1" in
    intra) echo "" ;;
    cross) echo "cross_az=true azs=$AZS_JSON subnet_ids=$SUBNETS_JSON" ;;
    *) echo "bad deploy $1" >&2; exit 2 ;;
  esac
}

# Compact deployment label for cell ids.
deploy_tag() {
  case "$1" in
    intra) echo "intraAZ" ;;
    cross) echo "crossAZ" ;;
  esac
}

refresh_e9() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E9 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)
FAILED=()

# ---------- Phase 1: knee sweep ----------
SWEEP_TOTAL=0
for val in "${VALS[@]}"; do
  for deploy in "${DEPLOYS[@]}"; do
    for _ in $(sweep_c "$val" "$deploy"); do SWEEP_TOTAL=$((SWEEP_TOTAL+1)); done
  done
done
SWEEP_INDEX=0
echo "==== E9 phase 1: knee sweep ($SWEEP_TOTAL cells, ~$((SWEEP_TOTAL*4)) min) ===="
for val in "${VALS[@]}"; do
  for deploy in "${DEPLOYS[@]}"; do
    tag=$(deploy_tag "$deploy")
    for c in $(sweep_c "$val" "$deploy"); do
      SWEEP_INDEX=$((SWEEP_INDEX+1))
      CELL_ID="E9-n${N}-v${val}-${tag}-c${c}-metronome_kf1-knee"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [sweep ${SWEEP_INDEX}/${SWEEP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS="$(mode_to_vars metronome_kf1) $(deploy_to_vars "$deploy")"
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$N" disk_tier="$DISK" value_size="$val" \
            $VARS n_clients="$c"; then
        echo ">>> sweep cell FAILED ($CELL_ID) — continuing"
        FAILED+=("$CELL_ID")
      fi
      refresh_e9
    done
  done
done

# ---------- Phase 2: knee detection ----------
echo
echo "==== E9 phase 2: knee detection ===="
declare -A KNEE
for val in "${VALS[@]}"; do
  for deploy in "${DEPLOYS[@]}"; do
    tag=$(deploy_tag "$deploy")
    KNEE_C=$(python3 - "$PROJECT_ROOT/results" "$N" "$val" "$tag" <<'PYEOF'
import json, sys, re
from pathlib import Path
root, n, val, tag = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
pat = re.compile(rf"^E9-n{n}-v{val}-{re.escape(tag)}-c(\d+)-metronome_kf1-knee$")
points = []
for d in sorted(Path(root).glob("E9-*")):
    if not d.is_dir(): continue
    m = pat.match(d.name)
    if not m: continue
    rj = d / "results.json"
    if not rj.exists(): continue
    try:
        data = json.loads(rj.read_text())
    except Exception: continue
    h = data.get("headline") or {}; lat = h.get("latency") or {}
    t = h.get("throughput_ops_per_sec"); p = lat.get("p99_s")
    if t is None or p is None: continue
    points.append({"c": int(m.group(1)), "tput": t, "p99": p})
if not points: print(""); sys.exit(0)
points.sort(key=lambda x: x["c"])
TG, PG = 1.30, 1.50
idx = 0
for i in range(len(points)-1):
    lo, hi = points[i], points[i+1]
    tg = hi["tput"]/lo["tput"] if lo["tput"] else 0
    pg = hi["p99"]/lo["p99"] if lo["p99"] else 0
    if tg >= TG and pg <= PG: idx = i+1
print(points[idx]["c"])
PYEOF
    )
    if [[ -z "$KNEE_C" ]]; then
      echo ">>> no knee for v=$val ${tag} — skipping comparison"
      continue
    fi
    KNEE["${val}_${deploy}"]="$KNEE_C"
    echo "  v=$val ${tag} knee: c=$KNEE_C"
  done
done

# Snapshot knees.
python3 - <<PYEOF
import json
d = {}
$(for k in "${!KNEE[@]}"; do
  IFS='_' read -r val deploy <<< "$k"
  echo "d.setdefault('$val', {})['$deploy'] = ${KNEE[$k]}"
done)
open("$PROJECT_ROOT/results/E9-knees.json", "w").write(json.dumps(d, indent=2))
print("wrote E9-knees.json:", d)
PYEOF

# ---------- Phase 3: passes 1..PASSES at each knee ----------
echo
echo "==== E9 phase 3: $PASSES passes × 20 cells = $((PASSES * 20)) comparison cells ===="
for pass in $(seq 1 "$PASSES"); do
  PASS_INDEX=0
  for val in "${VALS[@]}"; do
    for deploy in "${DEPLOYS[@]}"; do
      C="${KNEE[${val}_${deploy}]:-}"
      [[ -z "$C" ]] && continue
      tag=$(deploy_tag "$deploy")
      for mode in "${MODES[@]}"; do
        PASS_INDEX=$((PASS_INDEX+1))
        CELL_ID="E9-n${N}-v${val}-${tag}-${mode}-c${C}-run${pass}"
        ELAPSED=$(( $(date +%s) - T0 ))
        echo
        echo "###############################################"
        echo "# [pass${pass} ${PASS_INDEX}/20, +${ELAPSED}s] $CELL_ID"
        echo "###############################################"
        VARS="$(mode_to_vars "$mode") $(deploy_to_vars "$deploy")"
        if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
              "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
              n_servers="$N" disk_tier="$DISK" value_size="$val" \
              $VARS n_clients="$C"; then
          echo ">>> cell FAILED ($CELL_ID) — continuing"
          FAILED+=("$CELL_ID")
        fi
        refresh_e9
      done
    done
  done
  echo "  ==== pass $pass complete ===="
done

refresh_e9
echo
echo "==== E9 complete in $(( $(date +%s) - T0 ))s ===="
echo "==== knees ===="
cat "$PROJECT_ROOT/results/E9-knees.json" 2>/dev/null
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
