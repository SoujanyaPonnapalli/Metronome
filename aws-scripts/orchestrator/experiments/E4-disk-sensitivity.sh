#!/usr/bin/env bash
# E4 — Disk-sensitivity sweep.
#
# Thesis: metronome's per-op fsync work is K/N of vanilla. When disk is
# the bottleneck, that K/N reduction should translate directly into
# higher throughput. When disk is *not* the bottleneck (cheap fast
# disks, RTT-bound), the relative win shrinks.
#
# This experiment varies disk-tier on a fixed (N=3, val=4096) cluster
# and compares vanilla / metronome_kf1 / inmem at each tier's own knee.
#
# Phases:
#   1. Per-tier concurrency mini-sweep with metronome_kf1 to find the
#      knee for that tier (different tiers will saturate at different
#      client counts).
#   2. Comparison cells (vanilla / metronome_kf1 / inmem) × 3 runs at
#      each tier's knee.
#
# Cells:
#   sweep:      3 tiers × 5 c-points × 1 mode      = 15
#   comparison: 3 tiers × 3 modes  × 3 runs        = 27
#   Total:                                          42
# Wall: ~2.1 h at ~3 min/cell. Cost: ~$5 on Spot.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

N=3
VAL=4096
TIERS=(gp3-provisioned gp3-baseline st1-HDD)
RUNS_PER_CELL=3

# Per-tier concurrency sweep band — bracketing the expected knee.
#   gp3-provisioned (1000 MB/s, 16k IOPS): disk plentiful → higher c
#   gp3-baseline    ( 125 MB/s,  3k IOPS): E0 knee at c=200 for v=4096
#   st1-HDD         (  ~40 MB/s, ~250 IOPS sustained): slowest, low c
sweep_for_tier() {
  case "$1" in
    gp3-provisioned) echo 100 200 400 600 800 ;;
    gp3-baseline)    echo  50 100 200 400 600 ;;
    st1-HDD)         echo  10  25  50 100 200 ;;
    *) echo "bad tier $1" >&2; exit 2 ;;
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

refresh_e4() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E4 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

T0=$(date +%s)

# ---------- Phase 1: per-tier knee sweep (metronome_kf1) ----------
# Skip if results/E4-knees.json already exists with all 3 tiers populated
# (e.g., from a prior v1 run that we want to reuse for v2).
KNEES_JSON="$PROJECT_ROOT/results/E4-knees.json"
SKIP_SWEEP=0
if [[ -f "$KNEES_JSON" ]]; then
  HAVE=$(python3 -c "import json; d=json.load(open('$KNEES_JSON')); print(len([k for k in ['gp3-provisioned','gp3-baseline','st1-HDD'] if k in d]))" 2>/dev/null || echo 0)
  if [[ "$HAVE" == "3" ]]; then
    echo "==== E4 phase 1: SKIPPED ($KNEES_JSON already has all 3 tier knees) ===="
    cat "$KNEES_JSON"
    SKIP_SWEEP=1
  fi
fi

if (( ! SKIP_SWEEP )); then
  SWEEP_TOTAL=0
  for tier in "${TIERS[@]}"; do
    for _ in $(sweep_for_tier "$tier"); do
      SWEEP_TOTAL=$((SWEEP_TOTAL+1))
    done
  done

  SWEEP_INDEX=0
  echo "==== E4 phase 1: per-tier concurrency sweep ($SWEEP_TOTAL cells) ===="
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
            n_servers="$N" \
            disk_tier="$tier" \
            value_size="$VAL" \
            $VARS \
            n_clients="$c"; then
        echo ">>> sweep cell FAILED ($CELL_ID) — continuing"
      fi
      refresh_e4
    done
  done
fi

# ---------- Phase 2: per-tier knee detection ----------
# Use the same slope heuristic as find-knee.py: pick the lower endpoint
# of the last "still gaining ≥30% per 2x concurrency" step. We just
# inline the heuristic here since find-knee.py is hard-coded for the
# (N, val) keying of E0.
#
# When SKIP_SWEEP=1, we've reused the existing knees.json. Don't re-run
# the detection (it would scan an empty result set and overwrite our
# carry-over knees with {}).
declare -A KNEE
if (( SKIP_SWEEP )); then
  echo
  echo "==== E4 phase 2: SKIPPED (reusing carry-over knees from $KNEES_JSON) ===="
  while IFS=':' read -r tier knee; do
    [[ -n "$tier" && -n "$knee" ]] && KNEE["$tier"]="$knee"
  done < <(python3 -c "
import json
d=json.load(open('$KNEES_JSON'))
for k,v in d.items(): print(f'{k}:{v}')
")
  for tier in "${TIERS[@]}"; do
    echo "  $tier knee: c=${KNEE[$tier]:-MISSING}"
  done
else
echo
echo "==== E4 phase 2: per-tier knee detection ===="
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
    except Exception:
        continue
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
    if tg >= TG and pg <= PG:
        idx = i+1
print(points[idx]["c"])
PYEOF
  )
  if [[ -z "$KNEE_C" ]]; then
    echo ">>> no knee detected for $tier — skipping comparison phase for this tier"
    continue
  fi
  KNEE["$tier"]="$KNEE_C"
  echo "  $tier knee: c=$KNEE_C"
done

# Snapshot the knees so the final summary + downstream analysis can read them.
{
  printf '{\n'
  first=1
  for tier in "${TIERS[@]}"; do
    [[ -z "${KNEE[$tier]:-}" ]] && continue
    if (( first )); then first=0; else printf ',\n'; fi
    printf '  "%s": %d' "$tier" "${KNEE[$tier]}"
  done
  printf '\n}\n'
} > "$PROJECT_ROOT/results/E4-knees.json"
echo "wrote $PROJECT_ROOT/results/E4-knees.json"
fi # end SKIP_SWEEP else

# ---------- Phase 3: comparison cells at each tier's knee ----------
# v2: inmem dropped — already characterised in v1 as the disk-free upper
# bound; the disk-sensitivity story is the vanilla vs metronome_kf1 ratio.
echo
echo "==== E4 phase 3: vanilla / metronome_kf1 at each tier's knee ===="
MODES=(vanilla metronome_kf1)
COMP_TOTAL=0
for tier in "${TIERS[@]}"; do
  [[ -z "${KNEE[$tier]:-}" ]] && continue
  COMP_TOTAL=$(( COMP_TOTAL + ${#MODES[@]} * RUNS_PER_CELL ))
done

COMP_INDEX=0
FAILED=()
for tier in "${TIERS[@]}"; do
  C="${KNEE[$tier]:-}"
  [[ -z "$C" ]] && continue
  for mode in "${MODES[@]}"; do
    for run in $(seq 1 "$RUNS_PER_CELL"); do
      COMP_INDEX=$((COMP_INDEX+1))
      CELL_ID="E4-n${N}-v${VAL}-${tier}-${mode}-c${C}-run${run}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [comparison ${COMP_INDEX}/${COMP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      VARS=$(mode_to_vars "$mode")
      if ! "$PROJECT_ROOT/lib/run-cell.sh" "$CELL_ID" \
            "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
            n_servers="$N" \
            disk_tier="$tier" \
            value_size="$VAL" \
            $VARS \
            n_clients="$C"; then
        echo ">>> comparison cell FAILED ($CELL_ID) — continuing"
        FAILED+=("$CELL_ID")
      fi
      refresh_e4
    done
  done
done

refresh_e4

echo
echo "==== E4 sweep complete in $(( $(date +%s) - T0 ))s ===="
echo "==== per-tier knees ===="
cat "$PROJECT_ROOT/results/E4-knees.json"
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
echo
echo "Now: analyze/plot-results.py --experiment E4 --results-dir $PROJECT_ROOT/results"
