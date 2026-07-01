#!/usr/bin/env bash
# E4-faster — extends the E4 disk-sensitivity sweep with io2 + io2-Block-Express
# tiers (4× IOPS, 4× bandwidth over gp3-provisioned). Tests our model
# prediction that metronome's throughput win disappears when the disk
# is no longer the bottleneck.
#
# Phases (mirror of original E4):
#   1. Per-tier concurrency mini-sweep with metronome_kf1 → find knee
#   2. Comparison cells (vanilla / metronome_kf1) × 3 runs at each knee
#
# Cells:
#   tiers:      io2-fast, io2-extreme                              (2)
#   sweep:      tiers × 5 c-points × metronome_kf1                = 10
#   comparison: tiers × 2 modes × 3 runs                          = 12
#   Total:                                                          22
# Wall: ~1.5h. Cost: ~$15-25 for io2 IOPS.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

N=3
VAL=4096
TIERS=(io2-fast io2-extreme)
RUNS_PER_CELL=3

# Per-tier concurrency sweep band. io2-fast similar to gp3-provisioned;
# io2-extreme should saturate later because of 4x bandwidth.
sweep_for_tier() {
  case "$1" in
    io2-fast)    echo 200 400 600 800 1200 ;;
    io2-extreme) echo 400 800 1200 1600 2000 ;;
    *) echo "bad tier $1" >&2; exit 2 ;;
  esac
}

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

refresh_e4() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E4 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)

# ---------- Phase 1: per-tier knee sweep ----------
SWEEP_TOTAL=0
for tier in "${TIERS[@]}"; do
  for _ in $(sweep_for_tier "$tier"); do SWEEP_TOTAL=$((SWEEP_TOTAL+1)); done
done
SWEEP_INDEX=0
echo "==== E4-faster phase 1: knee sweep ($SWEEP_TOTAL cells) ===="
for tier in "${TIERS[@]}"; do
  for c in $(sweep_for_tier "$tier"); do
    SWEEP_INDEX=$((SWEEP_INDEX+1))
    CELL_ID="E4-n${N}-v${VAL}-${tier}-c${c}-metronome_kf1"
    ELAPSED=$(( $(date +%s) - T0 ))
    echo
    echo "###############################################"
    echo "# [sweep ${SWEEP_INDEX}/${SWEEP_TOTAL}, +${ELAPSED}s] $CELL_ID"
    echo "###############################################"
    VARS=$(mode_to_vars metronome_kf1)
    if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
          "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
          n_servers="$N" disk_tier="$tier" value_size="$VAL" \
          $VARS n_clients="$c"; then
      echo ">>> sweep cell FAILED ($CELL_ID) — continuing"
    fi
    refresh_e4
  done
done

# ---------- Phase 2: knee detection (same slope heuristic as E4) ----------
echo
echo "==== E4-faster phase 2: knee detection ===="
declare -A KNEE
for tier in "${TIERS[@]}"; do
  KNEE_C=$(python3 - "$PROJECT_ROOT/results" "$tier" "$N" "$VAL" <<'PYEOF'
import json, sys, re
from pathlib import Path
root, tier, n, val = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
pat = re.compile(rf"^E4-n{n}-v{val}-{re.escape(tier)}-c(\d+)-metronome_kf1$")
points = []
for d in sorted(Path(root).glob("E4-*")):
    if not d.is_dir(): continue
    m = pat.match(d.name)
    if not m: continue
    rj = d / "results.json"
    if not rj.exists(): continue
    try:
        data = json.loads(rj.read_text())
    except Exception: continue
    h = data.get("headline") or {}
    lat = h.get("latency") or {}
    t = h.get("throughput_ops_per_sec"); p = lat.get("p99_s")
    if t is None or p is None: continue
    points.append({"c": int(m.group(1)), "tput": t, "p99": p})
if not points:
    print(""); sys.exit(0)
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
    echo ">>> no knee detected for $tier — skipping comparison"
    continue
  fi
  KNEE["$tier"]="$KNEE_C"
  echo "  $tier knee: c=$KNEE_C"
done

# Append knees to E4-knees.json (preserving the existing v1/v2 tiers).
python3 - <<PYEOF
import json
p = "$PROJECT_ROOT/results/E4-knees.json"
try:
    d = json.load(open(p))
except Exception:
    d = {}
$(for tier in "${TIERS[@]}"; do
    [[ -n "${KNEE[$tier]:-}" ]] && echo "d['$tier'] = ${KNEE[$tier]}"
done)
open(p, 'w').write(json.dumps(d, indent=2))
print("updated", p, ":", d)
PYEOF

# ---------- Phase 3: vanilla vs metronome_kf1 at each tier knee ----------
COMP_TOTAL=0
for tier in "${TIERS[@]}"; do
  [[ -z "${KNEE[$tier]:-}" ]] && continue
  COMP_TOTAL=$(( COMP_TOTAL + 2 * RUNS_PER_CELL ))
done
COMP_INDEX=0
FAILED=()
echo
echo "==== E4-faster phase 3: comparison at each tier knee ($COMP_TOTAL cells) ===="
for tier in "${TIERS[@]}"; do
  C="${KNEE[$tier]:-}"
  [[ -z "$C" ]] && continue
  for mode in vanilla metronome_kf1; do
    for run in $(seq 1 "$RUNS_PER_CELL"); do
      COMP_INDEX=$((COMP_INDEX+1))
      CELL_ID="E4-n${N}-v${VAL}-${tier}-${mode}-c${C}-run${run}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [comp ${COMP_INDEX}/${COMP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS=$(mode_to_vars "$mode")
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$N" disk_tier="$tier" value_size="$VAL" \
            $VARS n_clients="$C"; then
        echo ">>> cell FAILED ($CELL_ID) — continuing"
        FAILED+=("$CELL_ID")
      fi
      refresh_e4
    done
  done
done
refresh_e4
echo
echo "==== E4-faster complete in $(( $(date +%s) - T0 ))s ===="
echo "==== knees ===="
cat "$PROJECT_ROOT/results/E4-knees.json"
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
