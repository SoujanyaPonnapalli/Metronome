#!/usr/bin/env python3
"""Identify the per-(N, value_size) concurrency knee from E0's sweep.

Knee heuristic (slope-based, matches paper convention):
  Walk the sweep ordered by ascending n_clients. For each doubling-step
  (c[i] -> c[i+1]) compute:
    tput_growth = tput[i+1] / tput[i]
    p99_growth  = p99[i+1]  / p99[i]
  The knee is c[i] (the LOWER end of the last 'still-healthy' doubling),
  where healthy means:
    tput_growth >= 1.30   (still gaining at least 30% throughput per 2x clients)
    p99_growth  <= 1.50   (and p99 isn't blowing up by 50%+ per 2x clients)
  If even the first doubling is unhealthy, we pick the lowest-c point.
  If every doubling is healthy, we pick the highest-c point.

Inputs:
  results/E0-*/results.json  (produced by parse-bench.py per cell)

Output:
  results/E0-peak-loads.json
  {
    "3": {"1024": 800, "4096": 400, "8192": 200, "16384": 100},
    "5": {...}, "7": {...}
  }
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

CELL_RE = re.compile(
    r"^E0-n(?P<n>\d+)-v(?P<val>\d+)-c(?P<c>\d+)-metronome_kf1$"
)


def load_e0(results_dir: Path) -> list[dict]:
    cells = []
    for d in sorted(results_dir.glob("E0-*")):
        if not d.is_dir():
            continue
        m = CELL_RE.match(d.name)
        if not m:
            continue
        rj = d / "results.json"
        if not rj.exists():
            print(f"  skipping {d.name}: no results.json", file=sys.stderr)
            continue
        data = json.loads(rj.read_text())
        headline = data.get("headline") or {}
        latency = headline.get("latency") or {}
        cells.append({
            "n": int(m.group("n")),
            "val": int(m.group("val")),
            "c": int(m.group("c")),
            "tput": headline.get("throughput_ops_per_sec"),
            "p99":  latency.get("p99_s"),
        })
    return cells


TPUT_HEALTHY_GROWTH = 1.30   # tput must grow >=30% per doubling of concurrency
P99_TOLERATED_GROWTH = 1.50  # p99 may grow up to 50% per doubling


def find_knees(cells: list[dict]) -> dict[str, dict[str, int]]:
    by_nv: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for c in cells:
        if c["tput"] is None or c["p99"] is None:
            continue
        by_nv[(c["n"], c["val"])].append(c)

    out: dict[str, dict[str, int]] = defaultdict(dict)
    for (n, val), points in sorted(by_nv.items()):
        points.sort(key=lambda x: x["c"])
        if not points:
            continue

        # Walk the curve: for each doubling step, decide if it's "healthy".
        # Knee = the LOWER endpoint of the last healthy step. If even the
        # first step is unhealthy, pick the lowest-c (= most conservative).
        # If every step is healthy, pick the highest-c (sweep didn't reach
        # the saturation knee).
        knee_idx = 0
        last_healthy_lo = None
        for i in range(len(points) - 1):
            lo, hi = points[i], points[i + 1]
            tg = hi["tput"] / lo["tput"] if lo["tput"] else 0
            pg = hi["p99"]  / lo["p99"]  if lo["p99"]  else 0
            healthy = (tg >= TPUT_HEALTHY_GROWTH) and (pg <= P99_TOLERATED_GROWTH)
            if healthy:
                last_healthy_lo = lo  # we'd "absorb" hi too, so update on next iter
                knee_idx = i + 1      # the HIGH side of a healthy step
        # If we ran off the end (every doubling healthy), knee_idx points at
        # the last sample. If we never had a healthy step, knee_idx is 0
        # (the lowest concurrency = safest choice).
        knee = points[knee_idx]
        out[str(n)][str(val)] = knee["c"]

        max_tput = max(p["tput"] for p in points)
        min_p99  = min(p["p99"]  for p in points)
        print(f"\n  N={n}  v={val}  knee=c={knee['c']}  "
              f"tput={knee['tput']:.0f} ops/s of max {max_tput:.0f}; "
              f"p99={knee['p99']*1000:.1f}ms of min {min_p99*1000:.1f}ms",
              file=sys.stderr)
        # Show the full curve with per-step growth so the choice is auditable.
        print(f"   {'c':>5}  {'tput':>7}  {'p99(ms)':>8}  "
              f"{'Δtput':>6}  {'Δp99':>6}  {'health':>7}",
              file=sys.stderr)
        for i, p in enumerate(points):
            mark = "*" if p["c"] == knee["c"] else " "
            if i == 0:
                tg_s, pg_s, h_s = "-", "-", ""
            else:
                lo = points[i - 1]
                tg = p["tput"] / lo["tput"] if lo["tput"] else 0
                pg = p["p99"]  / lo["p99"]  if lo["p99"]  else 0
                h = (tg >= TPUT_HEALTHY_GROWTH) and (pg <= P99_TOLERATED_GROWTH)
                tg_s = f"{tg:.2f}x"
                pg_s = f"{pg:.2f}x"
                h_s = "ok" if h else "sat"
            print(f"  {mark} c={p['c']:>4}  {p['tput']:>7.0f}  {p['p99']*1000:>8.1f}  "
                  f"{tg_s:>6}  {pg_s:>6}  {h_s:>7}", file=sys.stderr)
    return dict(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True, type=Path,
                    help="Root results/ directory (contains E0-* cells).")
    ap.add_argument("--out", required=True, type=Path,
                    help="Output JSON path (e.g. results/E0-peak-loads.json).")
    args = ap.parse_args()

    cells = load_e0(args.results_dir)
    if not cells:
        print("no E0 cells found", file=sys.stderr)
        return 2
    knees = find_knees(cells)
    args.out.write_text(json.dumps(knees, indent=2, sort_keys=True))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
