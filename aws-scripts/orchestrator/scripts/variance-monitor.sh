#!/usr/bin/env bash
# Background watcher (nohup'd on orchestrator). Polls v2 results and, for
# each completed (N, val, mode, c) cell-group, computes the σ/μ across
# runs. Cells with σ/μ ≥ THRESH are appended to ~/v2-rerun-candidates.txt
# so we can do additional runs at the end of the pipeline.
#
# Idempotent: tracks which cell-groups have already been flagged so we
# don't append duplicates.
#
# Exits when E3 tmux session is gone (whole v2 pipeline done).
set -uo pipefail
PROJECT_ROOT=/home/ubuntu/metronome-eval
THRESH="${THRESH:-5.0}"   # σ/μ % threshold

OUT=~/v2-rerun-candidates.txt
SEEN=~/v2-variance-seen.txt
touch "$OUT" "$SEEN"

note() { echo "[$(date -u +%FT%TZ)] $*"; }

note "variance-monitor started; threshold=${THRESH}% ; flagging cells in $OUT"

# Sleep loop — poll every 60s.
while :; do
  # Run the analyzer in-process to avoid spawning a process per cell.
  python3 - "$PROJECT_ROOT/results" "$THRESH" "$OUT" "$SEEN" <<'PYEOF'
import csv, math, re, sys, os
from collections import defaultdict
from statistics import mean, stdev
from pathlib import Path

results_dir, thresh, out_path, seen_path = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4]

# Read which groups we've already flagged.
seen = set()
if os.path.exists(seen_path):
    with open(seen_path) as f:
        seen = {line.strip() for line in f if line.strip()}

def mode_of(r):
    em = r.get("etcd_mode", "")
    if em == "metronome":
        off = int(float(r.get("metronome_quorum_offset") or 0))
        return f"metronome_kf{off+1}"
    return em

# Walk each experiment's aggregate.csv (E1, E4, E3).
new_flags = []
for label in ("E1", "E4", "E3"):
    csv_path = Path(results_dir) / f"{label}-aggregate.csv"
    if not csv_path.exists(): continue
    by_group = defaultdict(list)
    try:
        rows = list(csv.DictReader(open(csv_path)))
    except Exception:
        continue
    for r in rows:
        try:
            tput = float(r["throughput_ops_per_sec"])
            N = int(float(r["n_servers"])); val = int(float(r["value_size"])); c = int(float(r["n_clients"]))
        except (ValueError, TypeError, KeyError):
            continue
        m = mode_of(r)
        # Group by (label, N, val, c, mode). Disk tier for E4.
        disk = r.get("disk_tier","")
        key = (label, N, val, c, m, disk)
        by_group[key].append((r.get("cell_id",""), tput))
    for key, lst in by_group.items():
        if len(lst) < 2: continue
        # Only flag once the group has all its expected runs (3 for E1/E4, 2 for E3).
        expected = 2 if key[0] == "E3" else 3
        if len(lst) < expected: continue
        tputs = [t for _, t in lst]
        mu = mean(tputs); sigma = stdev(tputs)
        if mu <= 0: continue
        rel = (sigma/mu) * 100
        group_id = f"{key[0]}-n{key[1]}-v{key[2]}-c{key[3]}-{key[4]}-{key[5] or 'na'}"
        if rel >= thresh and group_id not in seen:
            new_flags.append((group_id, mu, sigma, rel, expected, lst[0][0]))
            seen.add(group_id)

# Append new flags + update seen.
if new_flags:
    with open(out_path, "a") as f:
        for gid, mu, sigma, rel, expected, sample_cell in new_flags:
            f.write(f"{gid}\ttput_mean={mu:.0f}\ttput_std={sigma:.0f}\tsigma_over_mu={rel:.1f}%\truns={expected}\tsample={sample_cell}\n")
    with open(seen_path, "w") as f:
        for g in sorted(seen): f.write(g + "\n")
    print(f"flagged {len(new_flags)} new high-variance cell-group(s):", file=sys.stderr)
    for gid, mu, sigma, rel, _, _ in new_flags:
        print(f"  {gid}  σ/μ={rel:.1f}%", file=sys.stderr)
PYEOF

  # Exit when E3 is done AND auto-trigger has exited.
  if ! tmux has-session -t E3 2>/dev/null && \
     ! tmux has-session -t E4 2>/dev/null && \
     ! tmux has-session -t E1 2>/dev/null && \
     ! pgrep -f auto-trigger-E4-then-E3 >/dev/null 2>&1; then
    note "all v2 sessions and watcher are done; exiting"
    note "final re-run candidates list:"
    cat "$OUT" 2>/dev/null | sed 's/^/  /'
    break
  fi
  sleep 60
done
