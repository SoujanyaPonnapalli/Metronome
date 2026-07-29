#!/usr/bin/env bash
# E8 — Disk-bandwidth-bound regime, IOPS held constant.
#
# Sweeps gp3 throughput (125/200/300 MB/s) at constant 3000 IOPS across
# 3 value sizes (4kB/16kB/64kB). Tests how metronome's win behaves when
# the leader sits on the disk-bandwidth bottleneck: as throughput rises,
# leader fsync time shrinks, leaving less for K/N savings to recover.
#
# Phases:
#   1. Per-(disk, val) knee-sweep with metronome_kf1 — 3 c-points each
#      to find each operating point's saturation knee.
#   2. Comparison: vanilla vs metronome_kf1, 3 runs each, at each knee.
#
# Cells:
#   sweep:     3 disks × 3 vals × 3 c-points × metronome_kf1   = 27
#   compare:   3 disks × 3 vals × 2 modes × 3 runs              = 54
#   Total:                                                       81 (~5.5h)
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

N=3
TIERS=(gp3-baseline gp3-200 gp3-300)
VALS=(4096 16384 65536)
RUNS_PER_CELL=3

# c-point sweep per (disk MB/s × val). Picked to bracket the expected
# saturation knee. Higher-throughput disks need more concurrency to
# reach saturation (Little's law); larger values need less.
sweep_c() {
  local tier="$1" val="$2"
  case "${tier}_v${val}" in
    gp3-baseline_v4096)    echo 100 200 400 ;;
    gp3-baseline_v16384)   echo  50 100 200 ;;
    gp3-baseline_v65536)   echo  25  50 100 ;;
    gp3-200_v4096)         echo 200 400 800 ;;
    gp3-200_v16384)        echo 100 200 400 ;;
    gp3-200_v65536)        echo  50 100 200 ;;
    gp3-300_v4096)         echo 400 600 1000 ;;
    gp3-300_v16384)        echo 150 300 600 ;;
    gp3-300_v65536)        echo  75 150 300 ;;
    *) echo "bad (tier,val) = ${tier},${val}" >&2; exit 2 ;;
  esac
}

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

refresh_e8() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E8 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)

# ---------- Phase 1: per-(disk, val) knee sweep ----------
SWEEP_TOTAL=0
for tier in "${TIERS[@]}"; do
  for val in "${VALS[@]}"; do
    for _ in $(sweep_c "$tier" "$val"); do SWEEP_TOTAL=$((SWEEP_TOTAL+1)); done
  done
done
SWEEP_INDEX=0
echo "==== E8 phase 1: knee sweep ($SWEEP_TOTAL cells) ===="
for tier in "${TIERS[@]}"; do
  for val in "${VALS[@]}"; do
    for c in $(sweep_c "$tier" "$val"); do
      SWEEP_INDEX=$((SWEEP_INDEX+1))
      CELL_ID="E8-n${N}-v${val}-${tier}-c${c}-metronome_kf1"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [sweep ${SWEEP_INDEX}/${SWEEP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS=$(mode_to_vars metronome_kf1)
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$N" disk_tier="$tier" value_size="$val" \
            $VARS n_clients="$c"; then
        echo ">>> sweep cell FAILED ($CELL_ID) — continuing"
      fi
      refresh_e8
    done
  done
done

# ---------- Phase 2: per-(disk, val) knee detection ----------
echo
echo "==== E8 phase 2: knee detection ===="
declare -A KNEE
for tier in "${TIERS[@]}"; do
  for val in "${VALS[@]}"; do
    KNEE_C=$(python3 - "$PROJECT_ROOT/results" "$tier" "$N" "$val" <<'PYEOF'
import json, sys, re
from pathlib import Path
root, tier, n, val = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
pat = re.compile(rf"^E8-n{n}-v{val}-{re.escape(tier)}-c(\d+)-metronome_kf1$")
points = []
for d in sorted(Path(root).glob("E8-*")):
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
    pg = hi["p99"]/lo["p99"]   if lo["p99"]  else 0
    if tg >= TG and pg <= PG: idx = i+1
print(points[idx]["c"])
PYEOF
    )
    if [[ -z "$KNEE_C" ]]; then
      echo ">>> no knee for $tier v=$val — skipping its comparison"
      continue
    fi
    KNEE["${tier}|${val}"]="$KNEE_C"
    echo "  $tier v=$val knee: c=$KNEE_C"
  done
done

# Snapshot the knees.
python3 - <<PYEOF
import json
d = {}
$(for k in "${!KNEE[@]}"; do
  IFS='|' read -r tier val <<< "$k"
  echo "d.setdefault('$tier', {})['$val'] = ${KNEE[$k]}"
done)
open("$PROJECT_ROOT/results/E8-knees.json", "w").write(json.dumps(d, indent=2))
print("wrote E8-knees.json:", d)
PYEOF

# ---------- Phase 3: comparison ----------
COMP_TOTAL=0
for tier in "${TIERS[@]}"; do
  for val in "${VALS[@]}"; do
    [[ -z "${KNEE[${tier}|${val}]:-}" ]] && continue
    COMP_TOTAL=$(( COMP_TOTAL + 2 * RUNS_PER_CELL ))
  done
done
COMP_INDEX=0
FAILED=()
echo
echo "==== E8 phase 3: comparison at each knee ($COMP_TOTAL cells) ===="
for tier in "${TIERS[@]}"; do
  for val in "${VALS[@]}"; do
    C="${KNEE[${tier}|${val}]:-}"
    [[ -z "$C" ]] && continue
    for mode in vanilla metronome_kf1; do
      for run in $(seq 1 "$RUNS_PER_CELL"); do
        COMP_INDEX=$((COMP_INDEX+1))
        CELL_ID="E8-n${N}-v${val}-${tier}-${mode}-c${C}-run${run}"
        ELAPSED=$(( $(date +%s) - T0 ))
        echo
        echo "###############################################"
        echo "# [comp ${COMP_INDEX}/${COMP_TOTAL}, +${ELAPSED}s] $CELL_ID"
        echo "###############################################"
        VARS=$(mode_to_vars "$mode")
        if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
              "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
              n_servers="$N" disk_tier="$tier" value_size="$val" \
              $VARS n_clients="$C"; then
          echo ">>> cell FAILED ($CELL_ID) — continuing"
          FAILED+=("$CELL_ID")
        fi
        refresh_e8
      done
    done
  done
done

refresh_e8
echo
echo "==== E8 complete in $(( $(date +%s) - T0 ))s ===="
echo "==== knees ===="
cat "$PROJECT_ROOT/results/E8-knees.json" 2>/dev/null
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
