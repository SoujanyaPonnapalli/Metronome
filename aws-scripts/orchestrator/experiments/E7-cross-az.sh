#!/usr/bin/env bash
# E7 — Cross-AZ deployment.
#
# Hypothesis: at cross-AZ RTT (1-2 ms in AWS, on the order of the leader's
# fsync time), the metronome win grows in *fractional* terms because the
# leader no longer pays follower-fsync on the critical path.
#
# Why we re-calibrate the knee instead of reusing E0's:
#   E0's knee was found intra-AZ. Cross-AZ adds RTT to per-op latency, so
#   at the SAME concurrency throughput drops (Little's law) and disk no
#   longer saturates. Reusing the intra-AZ knee would measure an
#   under-loaded comparison and understate the real disk-bound win.
#
# Phases:
#   1. Per-N concurrency sweep with metronome_kf1 (single run) to find
#      the cross-AZ knee for each cluster size.
#   2. Multi-run comparison (vanilla vs metronome_kf1, 3 runs each) at
#      each cross-AZ knee.
#
# Cells:
#   N=3 sweep:        6 c-points × metronome_kf1 = 6
#   N=5 sweep:        6 c-points × metronome_kf1 = 6
#   N=3 comparison:   2 modes × 3 runs            = 6
#   N=5 comparison:   2 modes × 3 runs            = 6
#   Total:                                         24 cells (~100 min, ~$2)
#
# Prereqs:
#   1. infra/setup-cross-az-subnet.sh has run.
#   2. cell/terraform supports var.cross_az (variables.tf + main.tf locals
#      + servers.tf round-robin AZ distribution).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

[[ -n "${SUBNET_ID_B:-}" ]] || {
  echo "SUBNET_ID_B not set in infra.env — run infra/setup-cross-az-subnet.sh first" >&2
  exit 2
}

NS=(3 5)
VAL=4096
DISK=gp3-baseline
RUNS_PER_CELL=3

# Cross-AZ knee sweep range. Brackets the intra-AZ knee (c=200 for v=4096):
#   * c=150 is below the intra-AZ knee — acts as a slope-heuristic anchor
#     so we have an audited "healthy growth" baseline at the low end, and
#     catches the unlikely case that cross-AZ pushes the knee DOWN.
#   * c=200..600 walks upward since cross-AZ RTT typically moves the knee
#     to higher concurrency.
# If the sweep tops out at c=600 without saturating, widen the band.
sweep_c() { echo 150 200 300 400 500 600; }

# Terraform list-typed vars. Must be JSON to match list(string) parsing.
AZS_JSON='["us-west-1a","us-west-1b"]'
SUBNETS_JSON="[\"$SUBNET_ID\",\"$SUBNET_ID_B\"]"

mode_to_vars() {
  case "$1" in
    vanilla)        echo "etcd_mode=vanilla" ;;
    metronome_kf1)  echo "etcd_mode=metronome metronome_quorum_offset=0" ;;
    *) echo "bad mode $1" >&2; exit 2 ;;
  esac
}

refresh_e7() {
  "$PROJECT_ROOT/analyze/parse-bench.py" \
    --aggregate --experiment E7 \
    --results-dir "$PROJECT_ROOT/results" 2>/dev/null || true
}

run_cell() {
  local cell_id="$1" mode="$2" n="$3" c="$4"
  local vars
  vars=$(mode_to_vars "$mode")
  "$PROJECT_ROOT/lib/run-cell.sh" "$cell_id" \
    "$PROJECT_ROOT/workloads/etcd-bench-put.sh" \
    n_servers="$n" \
    disk_tier="$DISK" \
    value_size="$VAL" \
    cross_az=true \
    azs="$AZS_JSON" \
    subnet_ids="$SUBNETS_JSON" \
    $vars \
    n_clients="$c"
}

T0=$(date +%s)
FAILED=()

# ---------- Phase 1: per-N knee sweep ----------
SWEEP_TOTAL=0
for n in "${NS[@]}"; do
  for _ in $(sweep_c); do SWEEP_TOTAL=$((SWEEP_TOTAL+1)); done
done
SWEEP_INDEX=0
echo "==== E7 phase 1: cross-AZ concurrency sweep ($SWEEP_TOTAL cells) ===="
for n in "${NS[@]}"; do
  for c in $(sweep_c); do
    SWEEP_INDEX=$((SWEEP_INDEX+1))
    CELL_ID="E7-n${n}-v${VAL}-crossAZ-c${c}-metronome_kf1"
    ELAPSED=$(( $(date +%s) - T0 ))
    echo
    echo "###############################################"
    echo "# [sweep ${SWEEP_INDEX}/${SWEEP_TOTAL}, +${ELAPSED}s] $CELL_ID"
    echo "###############################################"
    if ! run_cell "$CELL_ID" metronome_kf1 "$n" "$c"; then
      echo ">>> sweep cell FAILED ($CELL_ID) — continuing"
      FAILED+=("$CELL_ID")
    fi
    refresh_e7
  done
done

# ---------- Phase 2: cross-AZ knee detection ----------
echo
echo "==== E7 phase 2: cross-AZ knee detection ===="
declare -A CROSS_KNEE
for n in "${NS[@]}"; do
  KNEE_C=$(python3 - "$PROJECT_ROOT/results" "$n" "$VAL" <<'PYEOF'
import json, sys, re
from pathlib import Path
root, n, val = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
pat = re.compile(rf"^E7-n{n}-v{val}-crossAZ-c(\d+)-metronome_kf1$")
points = []
for d in sorted(Path(root).glob("E7-*")):
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
    echo ">>> no cross-AZ knee detected for N=$n (sweep produced no results) — skipping comparison"
    continue
  fi
  CROSS_KNEE["$n"]="$KNEE_C"
  echo "  N=$n cross-AZ knee: c=$KNEE_C"
done

# Snapshot the cross-AZ knees for downstream analysis.
{
  printf '{\n'
  first=1
  for n in "${NS[@]}"; do
    [[ -z "${CROSS_KNEE[$n]:-}" ]] && continue
    if (( first )); then first=0; else printf ',\n'; fi
    printf '  "%s": %d' "$n" "${CROSS_KNEE[$n]}"
  done
  printf '\n}\n'
} > "$PROJECT_ROOT/results/E7-cross-az-knees.json"
echo "wrote $PROJECT_ROOT/results/E7-cross-az-knees.json"

# ---------- Phase 3: comparison at each cross-AZ knee ----------
COMP_TOTAL=0
for n in "${NS[@]}"; do
  [[ -z "${CROSS_KNEE[$n]:-}" ]] && continue
  COMP_TOTAL=$(( COMP_TOTAL + 2 * RUNS_PER_CELL ))
done
COMP_INDEX=0
echo
echo "==== E7 phase 3: vanilla vs metronome_kf1 at each cross-AZ knee ($COMP_TOTAL cells) ===="
for n in "${NS[@]}"; do
  C="${CROSS_KNEE[$n]:-}"
  [[ -z "$C" ]] && continue
  for mode in vanilla metronome_kf1; do
    for run in $(seq 1 "$RUNS_PER_CELL"); do
      COMP_INDEX=$((COMP_INDEX+1))
      CELL_ID="E7-n${n}-v${VAL}-${mode}-crossAZ-c${C}-run${run}"
      ELAPSED=$(( $(date +%s) - T0 ))
      echo
      echo "###############################################"
      echo "# [comparison ${COMP_INDEX}/${COMP_TOTAL}, +${ELAPSED}s] $CELL_ID"
      echo "###############################################"
      if ! run_cell "$CELL_ID" "$mode" "$n" "$C"; then
        echo ">>> comparison cell FAILED ($CELL_ID) — continuing"
        FAILED+=("$CELL_ID")
      fi
      refresh_e7
    done
  done
done

refresh_e7
echo
echo "==== E7 sweep complete in $(( $(date +%s) - T0 ))s ===="
echo "==== cross-AZ knees ===="
cat "$PROJECT_ROOT/results/E7-cross-az-knees.json"
if (( ${#FAILED[@]} > 0 )); then
  echo
  echo ">>> ${#FAILED[@]} cells failed:"
  printf '    %s\n' "${FAILED[@]}"
fi
echo
echo "Compare uplift: E7 (cross-AZ) vs E1 (intra-AZ) at matching (N, val) mode."
