#!/usr/bin/env python3
"""Generate plots for an experiment from its aggregate CSV.

Runs on the orchestrator. Reads results/<EXP>-aggregate.csv (produced by
parse-bench.py --aggregate) and writes PNGs into results/plots/<EXP>/.

Plots produced (only the ones for which we have data):
  - throughput_vs_N_by_mode_v<val>.png    (E1 headline: bars/lines per mode)
  - p99_vs_N_by_mode_v<val>.png            (E1 headline: latency parity)
  - wal_ratio_vs_mode_n<N>.png             (per-follower WAL byte share vs theory)
  - bookkeeping_cpu_disk.png               (CPU & disk util by cell — sanity)
  - wal_fsync_mean_vs_mode_n<N>.png        (mean fsync latency by mode)

Usage:
  ./analyze/plot-results.py --experiment E1 --results-dir results
  ./analyze/plot-results.py --experiment E1 --results-dir results --out-dir custom-plots/
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

# Headless backend so this works over ssh with no $DISPLAY.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt   # noqa: E402

# Friendly mode display order + colors (matches the E1 mode set).
MODE_ORDER = ["vanilla", "inmem", "metronome_kf1", "metronome_kf2"]
MODE_LABEL = {
    "vanilla":       "vanilla",
    "inmem":         "inmem (no-WAL)",
    "metronome_kf1": "metronome K=f+1",
    "metronome_kf2": "metronome K=f+2",
}
MODE_COLOR = {
    "vanilla":       "#888888",
    "inmem":         "#1f77b4",
    "metronome_kf1": "#d62728",
    "metronome_kf2": "#ff7f0e",
}


def _f(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify_mode(row: dict[str, str]) -> str:
    mode = row.get("etcd_mode", "")
    if mode == "metronome":
        offset = int(_f(row.get("metronome_quorum_offset")) or 0)
        return f"metronome_kf{offset+1}"
    return mode


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            r: dict[str, Any] = dict(row)
            r["_mode"]    = _classify_mode(row)
            r["_n"]       = int(_f(row.get("n_servers")) or 0)
            r["_val"]     = int(_f(row.get("value_size")) or 0)
            r["throughput_ops_per_sec"]    = _f(row.get("throughput_ops_per_sec"))
            r["p50_s"]                     = _f(row.get("p50_s"))
            r["p99_s"]                     = _f(row.get("p99_s"))
            r["p999_s"]                    = _f(row.get("p999_s"))
            r["follower_to_leader_byte_ratio"] = _f(row.get("follower_to_leader_byte_ratio"))
            r["wal_fsync_mean_s"]          = _f(row.get("wal_fsync_mean_s"))
            r["cpu_busy_p99_pct_max"]      = _f(row.get("cpu_busy_p99_pct_max"))
            r["hottest_cpu_busy_p99_pct_max"] = _f(row.get("hottest_cpu_busy_p99_pct_max"))
            r["data_disk_util_pct_p99_max"]= _f(row.get("data_disk_util_pct_p99_max"))
            out.append(r)
    return out


def _mean_or_none(xs: list[float | None]) -> float | None:
    xs = [x for x in xs if x is not None]
    return mean(xs) if xs else None


def plot_throughput_vs_N(rows: list[dict[str, Any]], outdir: Path) -> list[Path]:
    """One chart per value_size: throughput at each N, one line per mode."""
    paths: list[Path] = []
    vals = sorted({r["_val"] for r in rows if r["_val"]})
    for val in vals:
        sub = [r for r in rows if r["_val"] == val]
        if not sub:
            continue
        fig, ax = plt.subplots(figsize=(6.5, 4))
        modes_seen = []
        for mode in MODE_ORDER:
            xs, ys = [], []
            by_n = defaultdict(list)
            for r in sub:
                if r["_mode"] == mode and r["throughput_ops_per_sec"] is not None:
                    by_n[r["_n"]].append(r["throughput_ops_per_sec"])
            if not by_n:
                continue
            for n in sorted(by_n):
                xs.append(n)
                ys.append(mean(by_n[n]))
            ax.plot(xs, ys, marker="o", color=MODE_COLOR[mode], label=MODE_LABEL[mode])
            modes_seen.append(mode)
        if not modes_seen:
            plt.close(fig); continue
        ax.set_xlabel("Cluster size N")
        ax.set_ylabel("Throughput (ops/s)")
        ax.set_title(f"Throughput vs N — value_size={val}B")
        ax.grid(True, linestyle=":", alpha=0.5)
        ax.legend(loc="best", fontsize=9)
        fig.tight_layout()
        p = outdir / f"throughput_vs_N_by_mode_v{val}.png"
        fig.savefig(p, dpi=140); plt.close(fig); paths.append(p)
    return paths


def plot_p99_vs_N(rows: list[dict[str, Any]], outdir: Path) -> list[Path]:
    paths: list[Path] = []
    vals = sorted({r["_val"] for r in rows if r["_val"]})
    for val in vals:
        sub = [r for r in rows if r["_val"] == val]
        if not sub:
            continue
        fig, ax = plt.subplots(figsize=(6.5, 4))
        modes_seen = []
        for mode in MODE_ORDER:
            by_n = defaultdict(list)
            for r in sub:
                if r["_mode"] == mode and r["p99_s"] is not None:
                    by_n[r["_n"]].append(r["p99_s"] * 1000.0)  # ms
            if not by_n:
                continue
            xs = sorted(by_n)
            ys = [mean(by_n[n]) for n in xs]
            ax.plot(xs, ys, marker="o", color=MODE_COLOR[mode], label=MODE_LABEL[mode])
            modes_seen.append(mode)
        if not modes_seen:
            plt.close(fig); continue
        ax.set_xlabel("Cluster size N")
        ax.set_ylabel("p99 PUT latency (ms)")
        ax.set_title(f"p99 latency vs N — value_size={val}B")
        ax.grid(True, linestyle=":", alpha=0.5)
        ax.legend(loc="best", fontsize=9)
        fig.tight_layout()
        p = outdir / f"p99_vs_N_by_mode_v{val}.png"
        fig.savefig(p, dpi=140); plt.close(fig); paths.append(p)
    return paths


def plot_wal_ratio(rows: list[dict[str, Any]], outdir: Path) -> list[Path]:
    """Per-follower WAL byte share vs theory (K/N)."""
    paths: list[Path] = []
    ns = sorted({r["_n"] for r in rows if r["_n"]})
    for n in ns:
        sub = [r for r in rows if r["_n"] == n]
        if not sub:
            continue
        fig, ax = plt.subplots(figsize=(6, 4))
        bars_x: list[str] = []
        bars_y: list[float] = []
        bars_c: list[str] = []
        for mode in MODE_ORDER:
            ys = [r["follower_to_leader_byte_ratio"] for r in sub
                  if r["_mode"] == mode and r["follower_to_leader_byte_ratio"] is not None]
            if not ys:
                continue
            bars_x.append(MODE_LABEL[mode])
            bars_y.append(mean(ys))
            bars_c.append(MODE_COLOR[mode])
        if not bars_x:
            plt.close(fig); continue
        ax.bar(bars_x, bars_y, color=bars_c, edgecolor="black")
        f = n // 2                  # f = floor((n-1)/2) for odd n is same as n//2
        # Reference lines for K/N at K = f+1 and K = f+2.
        ax.axhline((f + 1) / n, linestyle="--", color="#d62728", alpha=0.6,
                   label=f"theory K=f+1 = {(f+1)}/{n} = {(f+1)/n:.3f}")
        if (f + 2) <= n:
            ax.axhline((f + 2) / n, linestyle="--", color="#ff7f0e", alpha=0.6,
                       label=f"theory K=f+2 = {(f+2)}/{n} = {(f+2)/n:.3f}")
        ax.axhline(1.0, linestyle=":", color="gray", alpha=0.6, label="vanilla (1.0)")
        ax.set_ylim(0, 1.1)
        ax.set_ylabel("Follower WAL bytes / leader WAL bytes")
        ax.set_title(f"Per-follower WAL byte share — N={n}")
        ax.legend(loc="lower right", fontsize=8)
        fig.tight_layout()
        p = outdir / f"wal_ratio_vs_mode_n{n}.png"
        fig.savefig(p, dpi=140); plt.close(fig); paths.append(p)
    return paths


def plot_wal_fsync_mean(rows: list[dict[str, Any]], outdir: Path) -> list[Path]:
    paths: list[Path] = []
    ns = sorted({r["_n"] for r in rows if r["_n"]})
    for n in ns:
        sub = [r for r in rows if r["_n"] == n]
        if not sub:
            continue
        fig, ax = plt.subplots(figsize=(6, 4))
        bars_x, bars_y, bars_c = [], [], []
        for mode in MODE_ORDER:
            ys = [r["wal_fsync_mean_s"] * 1000.0 for r in sub
                  if r["_mode"] == mode and r["wal_fsync_mean_s"] is not None]
            if not ys:
                continue
            bars_x.append(MODE_LABEL[mode])
            bars_y.append(mean(ys))
            bars_c.append(MODE_COLOR[mode])
        if not bars_x:
            plt.close(fig); continue
        ax.bar(bars_x, bars_y, color=bars_c, edgecolor="black")
        ax.set_ylabel("Mean WAL fsync duration (ms)")
        ax.set_title(f"Mean WAL fsync latency — N={n}")
        ax.grid(True, axis="y", linestyle=":", alpha=0.5)
        fig.tight_layout()
        p = outdir / f"wal_fsync_mean_vs_mode_n{n}.png"
        fig.savefig(p, dpi=140); plt.close(fig); paths.append(p)
    return paths


def plot_bookkeeping(rows: list[dict[str, Any]], outdir: Path) -> list[Path]:
    """Two-panel: CPU vs disk util across all cells, colored by mode.

    The pitch: if every dot has disk util near saturation while CPU stays well
    below it, the run was disk-bound as designed.
    """
    fig, (axL, axR) = plt.subplots(1, 2, figsize=(11, 4))
    for mode in MODE_ORDER:
        xs, ys_cpu, ys_disk, ys_hot = [], [], [], []
        for r in rows:
            if r["_mode"] != mode:
                continue
            tp = r["throughput_ops_per_sec"]
            if tp is None:
                continue
            xs.append(tp)
            ys_cpu.append(r.get("cpu_busy_p99_pct_max"))
            ys_disk.append(r.get("data_disk_util_pct_p99_max"))
            ys_hot.append(r.get("hottest_cpu_busy_p99_pct_max"))
        if not xs:
            continue
        axL.scatter(xs, ys_cpu, color=MODE_COLOR[mode], label=MODE_LABEL[mode],
                    edgecolor="black", linewidths=0.5)
        axL.scatter(xs, ys_hot, color=MODE_COLOR[mode], marker="x", alpha=0.6)
        axR.scatter(xs, ys_disk, color=MODE_COLOR[mode], label=MODE_LABEL[mode],
                    edgecolor="black", linewidths=0.5)
    for ax, ylab, title in (
        (axL, "CPU busy p99 (%)  •=all-CPU  ×=hottest CPU",
                                  "CPU headroom across cells"),
        (axR, "Data-disk util p99 (%)", "Disk pressure across cells"),
    ):
        ax.set_xlabel("Throughput (ops/s)")
        ax.set_ylabel(ylab)
        ax.set_title(title)
        ax.set_ylim(0, 105)
        ax.grid(True, linestyle=":", alpha=0.5)
        ax.legend(loc="lower right", fontsize=8)
    fig.tight_layout()
    p = outdir / "bookkeeping_cpu_disk.png"
    fig.savefig(p, dpi=140); plt.close(fig)
    return [p]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--experiment", default="E1")
    ap.add_argument("--results-dir", required=True, type=Path)
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="Output dir for PNGs (default: <results-dir>/plots/<experiment>/).")
    args = ap.parse_args()

    csv_path = args.results_dir / f"{args.experiment}-aggregate.csv"
    if not csv_path.exists():
        print(f"missing {csv_path} — run parse-bench.py --aggregate first", file=sys.stderr)
        return 2
    rows = load_rows(csv_path)
    if not rows:
        print(f"no rows in {csv_path}", file=sys.stderr)
        return 2

    outdir = args.out_dir or (args.results_dir / "plots" / args.experiment)
    outdir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    written += plot_throughput_vs_N(rows, outdir)
    written += plot_p99_vs_N(rows, outdir)
    written += plot_wal_ratio(rows, outdir)
    written += plot_wal_fsync_mean(rows, outdir)
    written += plot_bookkeeping(rows, outdir)

    for p in written:
        print(f"wrote {p}")
    if not written:
        print("(no plots produced — empty input?)", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
