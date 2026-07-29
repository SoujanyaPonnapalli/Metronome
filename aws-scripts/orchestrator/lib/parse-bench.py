#!/usr/bin/env python3
"""Parse one cell's raw artifacts into results.json (and optionally aggregate).

Single-cell mode (called by lib/collect-results.sh):
    parse-bench.py --cell-id <id> --results-dir results/<cell_id>
        # writes results/<cell_id>/results.json to stdout
        # (collect-results.sh redirects stdout to results.json)

Aggregate mode (called at end of experiments/E*.sh):
    parse-bench.py --aggregate --experiment E1 --results-dir results
        # writes results/E1-aggregate.csv and results/E1-aggregate.json

Inputs per cell (created by lib/run-workload.sh + lib/collect-results.sh):
    topology.json                  - cell metadata (n_servers, etcd_mode, K, ...)
    driver-bench.log               - stdout from workloads/etcd-bench-put.sh
    server-<i>-etcd.log            - etcd's stderr from each server (errors only)
    server-<i>-iostat.csv          - `iostat -xyt 1`               (per-device r/w/util)
    server-<i>-vmstat.csv          - `vmstat -t -n 1`              (whole-system cpu+mem+io)
    server-<i>-mpstat.csv          - `mpstat -P ALL 1`             (per-cpu breakdown)
    server-<i>-pidstat.csv         - `pidstat -h -u -r -d -p ETCDPID 1`  (etcd cpu/rss/io)
    server-<i>-metrics-pre.prom    - Prometheus scrape BEFORE workload
    server-<i>-metrics-post.prom   - Prometheus scrape AFTER  workload
    server-<i>-metrics.prom        - legacy single-snapshot (== post; back-compat)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any


# --------------------------------------------------------------------------- #
# driver-bench.log: etcd-benchmark stdout                                     #
# --------------------------------------------------------------------------- #

# A new phase starts at:  "## CELL: <id> phase=<warmup|measure|cooldown> ..."
CELL_HEADER_RE = re.compile(
    r"^##\s*CELL:\s*(?P<cell>\S+)\s+phase=(?P<phase>\w+)"
    r"(?:\s+concurrency=(?P<conc>\d+))?"
    r"(?:\s+value_size=(?P<val>\d+))?"
    r"(?:\s+total=(?P<total>\d+))?"
)

# Within a phase, etcd-benchmark emits one "Summary:" block.
# We extract Total, Slowest, Fastest, Average, Stddev, Requests/sec
# from "Summary:" and the percentile lines from "Latency distribution:".
SUMMARY_KV_RE = re.compile(
    r"^\s*(Total|Slowest|Fastest|Average|Stddev|Requests/sec):\s*([0-9.]+)"
)
LATENCY_PCT_RE = re.compile(
    r"^\s*(?P<pct>\d+(?:\.\d+)?)%\s+in\s+(?P<sec>[0-9.]+)\s+secs"
)


def parse_driver_bench(path: Path) -> list[dict[str, Any]]:
    """Return a list of per-phase summaries.

    Each entry has:
        {
          "phase": "warmup" | "measure" | "cooldown",
          "concurrency": int,
          "value_size": int,
          "total": int,
          "total_secs": float,
          "throughput_ops_per_sec": float,
          "latency": {
              "avg_s": ..., "stddev_s": ..., "min_s": ..., "max_s": ...,
              "p50_s": ..., "p90_s": ..., "p95_s": ..., "p99_s": ..., "p999_s": ...,
          },
        }
    """
    if not path.exists():
        return []
    phases: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    for line in path.read_text(errors="replace").splitlines():
        m = CELL_HEADER_RE.match(line)
        if m:
            if cur and "throughput_ops_per_sec" in cur:
                phases.append(cur)
            cur = {
                "phase": m.group("phase"),
                "concurrency": _int(m.group("conc")),
                "value_size": _int(m.group("val")),
                "total": _int(m.group("total")),
                "total_secs": None,
                "throughput_ops_per_sec": None,
                "latency": {},
            }
            continue
        if cur is None:
            continue
        ms = SUMMARY_KV_RE.match(line)
        if ms:
            key, val = ms.group(1), float(ms.group(2))
            if key == "Total":
                cur["total_secs"] = val
            elif key == "Slowest":
                cur["latency"]["max_s"] = val
            elif key == "Fastest":
                cur["latency"]["min_s"] = val
            elif key == "Average":
                cur["latency"]["avg_s"] = val
            elif key == "Stddev":
                cur["latency"]["stddev_s"] = val
            elif key == "Requests/sec":
                cur["throughput_ops_per_sec"] = val
            continue
        mp = LATENCY_PCT_RE.match(line)
        if mp:
            pct = mp.group("pct")
            sec = float(mp.group("sec"))
            key = {
                "50":  "p50_s",
                "90":  "p90_s",
                "95":  "p95_s",
                "99":  "p99_s",
                "99.9": "p999_s",
            }.get(pct)
            if key:
                cur["latency"][key] = sec
    if cur and "throughput_ops_per_sec" in cur and cur["throughput_ops_per_sec"]:
        phases.append(cur)
    return phases


# --------------------------------------------------------------------------- #
# server-<i>-iostat.csv: `iostat -xyt 1`                                      #
# --------------------------------------------------------------------------- #

# iostat -xyt 1 output is repeating blocks of:
#   2024-05-18 14:30:01 PDT
#   avg-cpu:  %user   %nice %system %iowait  %steal   %idle
#              5.00    0.00   10.00    2.00    0.00   83.00
#
#   Device            r/s     w/s     rkB/s     wkB/s ...  %util
#   nvme0n1          0.00   10.00      0.00    400.00 ...   3.00
#   nvme1n1          0.00   50.00      0.00   2000.00 ...  25.00

IOSTAT_DEVICE_HEADER_RE = re.compile(r"^Device\s+r/s\s+w/s")


def parse_iostat(path: Path) -> dict[str, Any]:
    """Aggregate iostat samples across the run.

    Returns a per-device dict of mean/p50/p99 for r_kB_s, w_kB_s, util_pct.
    """
    if not path.exists():
        return {"present": False}

    by_dev: dict[str, dict[str, list[float]]] = {}
    in_device_block = False
    header_cols: list[str] = []

    for line in path.read_text(errors="replace").splitlines():
        s = line.strip()
        if not s:
            in_device_block = False
            continue
        if IOSTAT_DEVICE_HEADER_RE.match(s):
            header_cols = s.split()
            in_device_block = True
            continue
        if not in_device_block:
            continue

        parts = s.split()
        if len(parts) < 4:
            continue
        device = parts[0]
        try:
            rkB = _col_value(header_cols, parts, "rkB/s")
            wkB = _col_value(header_cols, parts, "wkB/s")
            util = _col_value(header_cols, parts, "%util")
        except Exception:
            continue

        rec = by_dev.setdefault(device, {"r_kB_s": [], "w_kB_s": [], "util_pct": []})
        if rkB is not None:
            rec["r_kB_s"].append(rkB)
        if wkB is not None:
            rec["w_kB_s"].append(wkB)
        if util is not None:
            rec["util_pct"].append(util)

    summary: dict[str, Any] = {"present": True, "devices": {}}
    for dev, rec in by_dev.items():
        summary["devices"][dev] = {
            "samples": len(rec["w_kB_s"]),
            "r_kB_s":   _stats(rec["r_kB_s"]),
            "w_kB_s":   _stats(rec["w_kB_s"]),
            "util_pct": _stats(rec["util_pct"]),
        }
    return summary


def _col_value(header: list[str], parts: list[str], col: str) -> float | None:
    try:
        idx = header.index(col)
    except ValueError:
        return None
    if idx >= len(parts):
        return None
    try:
        return float(parts[idx])
    except ValueError:
        return None


def _stats(xs: list[float]) -> dict[str, float]:
    if not xs:
        return {"n": 0}
    xs_sorted = sorted(xs)
    n = len(xs_sorted)
    return {
        "n": n,
        "mean":   mean(xs_sorted),
        "stddev": pstdev(xs_sorted) if n > 1 else 0.0,
        "p50":    median(xs_sorted),
        "p99":    xs_sorted[max(0, int(n * 0.99) - 1)] if n >= 100 else xs_sorted[-1],
        "max":    xs_sorted[-1],
    }


# --------------------------------------------------------------------------- #
# server-<i>-metrics.prom: final Prometheus scrape                            #
# --------------------------------------------------------------------------- #

# Headline counters/histograms we care about for E1's claim:
#   - etcd_disk_wal_write_bytes_total        (THE metronome paper byte count)
#   - etcd_disk_wal_fsync_duration_seconds_* (count + sum -> mean fsync latency)
#   - etcd_disk_backend_commit_duration_seconds_*
#   - etcd_network_peer_sent_bytes_total
#   - etcd_server_leader_changes_seen_total  (sanity: should be ~0)
METRICS_OF_INTEREST = {
    "etcd_disk_wal_write_bytes_total",
    "etcd_disk_wal_fsync_duration_seconds_count",
    "etcd_disk_wal_fsync_duration_seconds_sum",
    "etcd_disk_backend_commit_duration_seconds_count",
    "etcd_disk_backend_commit_duration_seconds_sum",
    "etcd_network_peer_sent_bytes_total",
    "etcd_server_leader_changes_seen_total",
    "etcd_server_proposals_applied_total",
    "etcd_server_proposals_committed_total",
    "etcd_server_proposals_pending",
    "etcd_server_proposals_failed_total",
    # metronome-branch additions, if present in the build:
    "etcd_metronome_entries_skipped_total",
    "etcd_metronome_work_steals_triggered_total",
    "etcd_metronome_commit_clamps_on_load_total",
    "etcd_metronome_incoming_commit_clamps_total",
}

PROM_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?:\{(?P<labels>[^}]*)\})?"
    r"\s+(?P<value>[^\s]+)\s*$"
)


def _scrape_metrics(path: Path) -> dict[str, float]:
    """Return name -> summed value (over all label sets) for the metrics of interest."""
    if not path.exists():
        return {}
    sums: dict[str, float] = {}
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        if name not in METRICS_OF_INTEREST:
            continue
        try:
            val = float(m.group("value"))
        except ValueError:
            continue
        sums[name] = sums.get(name, 0.0) + val
    return sums


def parse_metrics_pre_post(pre_path: Path, post_path: Path) -> dict[str, Any]:
    """Compute deltas (post - pre) when both snapshots are present.

    If only `post` exists (legacy single-snapshot), the deltas degrade to
    absolute values (which is correct for a freshly-started cluster).
    """
    pre = _scrape_metrics(pre_path)
    post = _scrape_metrics(post_path)
    if not pre and not post:
        return {"present": False}
    out: dict[str, Any] = {"present": True, "has_pre": bool(pre)}
    for k in METRICS_OF_INTEREST:
        if k in post:
            out[k] = post[k] - pre.get(k, 0.0) if pre else post[k]
    # Mean fsync latency over the measured interval.
    cnt = out.get("etcd_disk_wal_fsync_duration_seconds_count", 0.0)
    tot = out.get("etcd_disk_wal_fsync_duration_seconds_sum", 0.0)
    if cnt and cnt > 0:
        out["wal_fsync_mean_s"] = tot / cnt
    cnt2 = out.get("etcd_disk_backend_commit_duration_seconds_count", 0.0)
    tot2 = out.get("etcd_disk_backend_commit_duration_seconds_sum", 0.0)
    if cnt2 and cnt2 > 0:
        out["backend_commit_mean_s"] = tot2 / cnt2
    return out


def parse_metrics(path: Path) -> dict[str, Any]:
    """Back-compat wrapper for single-snapshot Prometheus consumption."""
    return parse_metrics_pre_post(Path("/nonexistent"), path)


# --------------------------------------------------------------------------- #
# vmstat / mpstat / pidstat                                                   #
# --------------------------------------------------------------------------- #

# `vmstat -t -n 1` output (no header repeat) looks like:
#   procs -----------memory---------- ---swap-- -----io---- -system-- ------cpu----- -----timestamp-----
#    r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st             UTC
#    1  0      0 1232800  35892 1198368   0    0     0     0  124  246  1  0 99  0  0 2026-05-18 14:30:01
# We want time-series of: us, sy, id, wa (CPU %); free (KB); bo (blocks-out/s); cs (context switches/s).
def parse_vmstat(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"present": False}
    lines = path.read_text(errors="replace").splitlines()
    # Find a header to locate columns; column order is stable.
    cpu_us: list[float] = []; cpu_sy: list[float] = []; cpu_id: list[float] = []; cpu_wa: list[float] = []
    mem_free_kb: list[float] = []; io_bo: list[float] = []; cs: list[float] = []
    for line in lines:
        parts = line.split()
        # Data rows start with two integers (r, b) followed by more integers.
        if len(parts) < 17:
            continue
        try:
            int(parts[0]); int(parts[1])
        except ValueError:
            continue
        try:
            free = float(parts[3])
            bo   = float(parts[9])
            csv  = float(parts[11])
            us   = float(parts[12]); sy = float(parts[13]); idl = float(parts[14]); wa = float(parts[15])
        except (ValueError, IndexError):
            continue
        mem_free_kb.append(free); io_bo.append(bo); cs.append(csv)
        cpu_us.append(us); cpu_sy.append(sy); cpu_id.append(idl); cpu_wa.append(wa)
    cpu_busy = [100.0 - x for x in cpu_id]
    return {
        "present": True,
        "samples": len(cpu_id),
        "cpu_busy_pct":    _stats(cpu_busy),
        "cpu_user_pct":    _stats(cpu_us),
        "cpu_system_pct":  _stats(cpu_sy),
        "cpu_iowait_pct":  _stats(cpu_wa),
        "mem_free_kb":     _stats(mem_free_kb),
        "io_blocks_out":   _stats(io_bo),
        "context_switches": _stats(cs),
    }


# `mpstat -P ALL 1` table rows look like:
#   14:30:02     all    1.50    0.00    0.50    0.00    0.00    0.00    0.00    0.00    0.00   98.00
#   14:30:02       0    2.00    0.00    1.00    ...
# We want, per CPU, mean %busy = 100 - %idle, and the cluster-wide max-CPU busy
# (catches a single-core bottleneck even on 8 vCPUs).
def parse_mpstat(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"present": False}
    per_cpu_busy: dict[str, list[float]] = {}
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) < 11:
            continue
        # mpstat rows: <time> [<AM|PM>] CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle
        # The CPU column is parts[1] (or [2] if locale-AM/PM added a token).
        cpu_idx = 1
        if parts[1] in ("AM", "PM"):
            cpu_idx = 2
        cpu = parts[cpu_idx]
        if cpu in ("CPU",):  # header
            continue
        try:
            idle = float(parts[-1])
        except ValueError:
            continue
        if not (0.0 <= idle <= 100.0):
            continue
        per_cpu_busy.setdefault(cpu, []).append(100.0 - idle)
    if not per_cpu_busy:
        return {"present": False}
    summary: dict[str, Any] = {"present": True, "per_cpu": {}}
    max_busy_p99 = 0.0
    max_busy_max = 0.0
    for cpu, xs in sorted(per_cpu_busy.items(), key=lambda kv: (kv[0] != "all", kv[0])):
        s = _stats(xs)
        summary["per_cpu"][cpu] = s
        if cpu != "all":
            max_busy_p99 = max(max_busy_p99, s.get("p99", 0.0))
            max_busy_max = max(max_busy_max, s.get("max", 0.0))
    summary["hottest_cpu_busy_p99_pct"] = max_busy_p99
    summary["hottest_cpu_busy_max_pct"] = max_busy_max
    return summary


# `pidstat -h -u -r -d -p <PID> 1` is a unified single-table format with -h.
# Columns include: Time UID PID %usr %system %guest %wait %CPU CPU minflt/s majflt/s VSZ RSS %MEM kB_rd/s kB_wr/s kB_ccwr/s iodelay Command
def parse_pidstat(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"present": False}
    cpu_pct: list[float] = []; rss_kb: list[float] = []
    kb_rd: list[float] = []; kb_wr: list[float] = []
    header: list[str] | None = None
    for line in path.read_text(errors="replace").splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            header = s.lstrip("#").split()
            continue
        if header is None:
            continue
        parts = s.split()
        if len(parts) < len(header):
            continue
        try:
            idx_cpu = header.index("%CPU"); cpu_pct.append(float(parts[idx_cpu]))
        except (ValueError, IndexError):
            pass
        try:
            idx_rss = header.index("RSS");  rss_kb.append(float(parts[idx_rss]))
        except (ValueError, IndexError):
            pass
        try:
            idx_rd = header.index("kB_rd/s"); kb_rd.append(float(parts[idx_rd]))
        except (ValueError, IndexError):
            pass
        try:
            idx_wr = header.index("kB_wr/s"); kb_wr.append(float(parts[idx_wr]))
        except (ValueError, IndexError):
            pass
    if not cpu_pct and not rss_kb:
        return {"present": False}
    return {
        "present": True,
        "samples": max(len(cpu_pct), len(rss_kb)),
        "etcd_cpu_pct":  _stats(cpu_pct),
        "etcd_rss_kb":   _stats(rss_kb),
        "etcd_kb_rd_s":  _stats(kb_rd),
        "etcd_kb_wr_s":  _stats(kb_wr),
    }


# --------------------------------------------------------------------------- #
# Per-cell entry point                                                         #
# --------------------------------------------------------------------------- #

def parse_cell(results_dir: Path, cell_id: str) -> dict[str, Any]:
    topo_path = results_dir / "topology.json"
    topo = json.loads(topo_path.read_text()) if topo_path.exists() else {}

    bench = parse_driver_bench(results_dir / "driver-bench.log")
    # The "measure" phase is the one that backs the E1 claim. We also keep
    # the warmup/cooldown for sanity but flag the headline.
    headline = next((p for p in bench if p["phase"] == "measure"), None)

    n_servers = topo.get("n_servers", 0)
    servers: list[dict[str, Any]] = []
    for i in range(1, n_servers + 1):
        pre  = results_dir / f"server-{i}-metrics-pre.prom"
        post = results_dir / f"server-{i}-metrics-post.prom"
        if not post.exists():
            post = results_dir / f"server-{i}-metrics.prom"   # legacy back-compat
        servers.append({
            "index":   i,
            "iostat":  parse_iostat(results_dir / f"server-{i}-iostat.csv"),
            "vmstat":  parse_vmstat(results_dir / f"server-{i}-vmstat.csv"),
            "mpstat":  parse_mpstat(results_dir / f"server-{i}-mpstat.csv"),
            "pidstat": parse_pidstat(results_dir / f"server-{i}-pidstat.csv"),
            "metrics": parse_metrics_pre_post(pre, post),
        })

    driver = {
        "vmstat":  parse_vmstat(results_dir / "driver-vmstat.csv"),
        "mpstat":  parse_mpstat(results_dir / "driver-mpstat.csv"),
        "pidstat": parse_pidstat(results_dir / "driver-pidstat.csv"),
    }

    # Per-cell roll-ups that are interesting on their own.
    wal_bytes_by_node = [
        s["metrics"].get("etcd_disk_wal_write_bytes_total")
        for s in servers if s["metrics"].get("present")
    ]
    wal_bytes_by_node = [b for b in wal_bytes_by_node if b is not None]
    rollup: dict[str, Any] = {}
    if wal_bytes_by_node:
        leader_bytes = max(wal_bytes_by_node)
        follower_bytes = [b for b in wal_bytes_by_node if b < leader_bytes]
        rollup["wal_bytes_leader"] = leader_bytes
        rollup["wal_bytes_followers_mean"] = (
            mean(follower_bytes) if follower_bytes else leader_bytes
        )
        if leader_bytes > 0:
            rollup["follower_to_leader_byte_ratio"] = (
                rollup["wal_bytes_followers_mean"] / leader_bytes
            )

    return {
        "cell_id":   cell_id,
        "topology":  topo,
        "headline":  headline,
        "phases":    bench,
        "servers":   servers,
        "driver":    driver,
        "rollup":    rollup,
    }


# --------------------------------------------------------------------------- #
# Aggregate entry point                                                        #
# --------------------------------------------------------------------------- #

AGG_COLS = [
    "cell_id", "experiment", "n_servers", "etcd_mode",
    "metronome_quorum_offset", "value_size", "disk_tier", "n_clients",
    "throughput_ops_per_sec", "p50_s", "p99_s", "p999_s",
    "wal_bytes_leader", "wal_bytes_followers_mean", "follower_to_leader_byte_ratio",
    "wal_fsync_mean_s", "backend_commit_mean_s",
    # bookkeeping (proves c6in.2xlarge sizing was right)
    "cpu_busy_p99_pct_max",          # max across nodes of vmstat p99 cpu-busy
    "hottest_cpu_busy_p99_pct_max",  # mpstat: most-loaded single-CPU p99 across nodes
    "etcd_rss_kb_max",
    "data_disk_w_kBs_p99_max",       # iostat write throughput on the data device
    "data_disk_util_pct_p99_max",
    # Driver-side bookkeeping (proves the workload isn't client-limited).
    "driver_cpu_busy_p99_pct",
    "driver_hottest_cpu_p99_pct",
    "driver_benchmark_cpu_p99_pct",
    "driver_benchmark_rss_kb_max",
]


def aggregate(experiment: str, results_root: Path) -> tuple[Path, Path]:
    rows: list[dict[str, Any]] = []
    json_blob: list[dict[str, Any]] = []
    for cell_dir in sorted(results_root.glob(f"{experiment}-*")):
        if not cell_dir.is_dir():
            continue
        rj = cell_dir / "results.json"
        if not rj.exists():
            print(f"  (skipping {cell_dir.name}: no results.json)", file=sys.stderr)
            continue
        data = json.loads(rj.read_text())
        json_blob.append(data)

        topo = data.get("topology") or {}
        headline = data.get("headline") or {}
        latency = headline.get("latency") or {}
        rollup = data.get("rollup") or {}

        srvs = data.get("servers", []) or []

        # Mean metrics across nodes for headline single-number summary.
        wal_fsync_means = [
            s["metrics"].get("wal_fsync_mean_s") for s in srvs
            if s.get("metrics", {}).get("present")
        ]
        wal_fsync_means = [x for x in wal_fsync_means if x is not None]
        backend_means = [
            s["metrics"].get("backend_commit_mean_s") for s in srvs
            if s.get("metrics", {}).get("present")
        ]
        backend_means = [x for x in backend_means if x is not None]

        # Bookkeeping: take the MAX across nodes (worst case is the limit).
        def _per_node_p99(field_path: list[str]) -> float | None:
            xs: list[float] = []
            for s in srvs:
                cur: Any = s
                for f in field_path:
                    if not isinstance(cur, dict):
                        cur = None; break
                    cur = cur.get(f)
                if isinstance(cur, dict) and "p99" in cur:
                    xs.append(cur["p99"])
                elif isinstance(cur, (int, float)):
                    xs.append(float(cur))
            return max(xs) if xs else None

        rss_maxes = [
            s.get("pidstat", {}).get("etcd_rss_kb", {}).get("max")
            for s in srvs
        ]
        rss_maxes = [x for x in rss_maxes if x is not None]

        # iostat: pick the highest-write-throughput device per server (data vol).
        disk_w_p99_max: float | None = None
        disk_util_p99_max: float | None = None
        for s in srvs:
            devs = (s.get("iostat") or {}).get("devices") or {}
            if not devs:
                continue
            # pick the device with the largest mean w_kB_s — that's our data vol
            data_dev = max(devs.items(), key=lambda kv: (kv[1].get("w_kB_s") or {}).get("mean", 0.0))
            stats = data_dev[1]
            wkB = (stats.get("w_kB_s") or {}).get("p99")
            util = (stats.get("util_pct") or {}).get("p99")
            if wkB is not None:
                disk_w_p99_max = wkB if disk_w_p99_max is None else max(disk_w_p99_max, wkB)
            if util is not None:
                disk_util_p99_max = util if disk_util_p99_max is None else max(disk_util_p99_max, util)

        rows.append({
            "cell_id":    data.get("cell_id"),
            "experiment": experiment,
            "n_servers":              topo.get("n_servers"),
            "etcd_mode":              topo.get("etcd_mode"),
            "metronome_quorum_offset": topo.get("metronome_quorum_offset", 0),
            "value_size":             topo.get("value_size"),
            "disk_tier":              topo.get("disk_tier"),
            "n_clients":              topo.get("n_clients"),
            "throughput_ops_per_sec": headline.get("throughput_ops_per_sec"),
            "p50_s":  latency.get("p50_s"),
            "p99_s":  latency.get("p99_s"),
            "p999_s": latency.get("p999_s"),
            "wal_bytes_leader":               rollup.get("wal_bytes_leader"),
            "wal_bytes_followers_mean":       rollup.get("wal_bytes_followers_mean"),
            "follower_to_leader_byte_ratio":  rollup.get("follower_to_leader_byte_ratio"),
            "wal_fsync_mean_s":      mean(wal_fsync_means)  if wal_fsync_means else None,
            "backend_commit_mean_s": mean(backend_means)    if backend_means   else None,
            "cpu_busy_p99_pct_max":         _per_node_p99(["vmstat", "cpu_busy_pct"]),
            "hottest_cpu_busy_p99_pct_max": _per_node_p99(["mpstat", "hottest_cpu_busy_p99_pct"]),
            "etcd_rss_kb_max":              max(rss_maxes) if rss_maxes else None,
            "data_disk_w_kBs_p99_max":      disk_w_p99_max,
            "data_disk_util_pct_p99_max":   disk_util_p99_max,
            "driver_cpu_busy_p99_pct":      ((data.get("driver") or {}).get("vmstat")  or {}).get("cpu_busy_pct",  {}).get("p99"),
            "driver_hottest_cpu_p99_pct":   ((data.get("driver") or {}).get("mpstat")  or {}).get("hottest_cpu_busy_p99_pct"),
            "driver_benchmark_cpu_p99_pct": ((data.get("driver") or {}).get("pidstat") or {}).get("etcd_cpu_pct", {}).get("p99"),
            "driver_benchmark_rss_kb_max":  ((data.get("driver") or {}).get("pidstat") or {}).get("etcd_rss_kb",  {}).get("max"),
        })

    csv_path  = results_root / f"{experiment}-aggregate.csv"
    json_path = results_root / f"{experiment}-aggregate.json"

    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=AGG_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in AGG_COLS})

    json_path.write_text(json.dumps(json_blob, indent=2, sort_keys=True, default=_json_default))
    return csv_path, json_path


def _int(x: str | None) -> int | None:
    try:
        return int(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _json_default(o: Any) -> Any:
    # numbers come out fine already; this catches Path & misc.
    if isinstance(o, Path):
        return str(o)
    raise TypeError(f"not json-serializable: {type(o).__name__}")


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cell-id", help="Parse one cell (single-cell mode).")
    ap.add_argument("--results-dir", required=True, type=Path,
                    help="In single-cell mode: results/<cell_id>. "
                         "In aggregate mode: the root `results/` directory.")
    ap.add_argument("--aggregate", action="store_true",
                    help="Aggregate all <experiment>-* cells in --results-dir.")
    ap.add_argument("--experiment", default="E1",
                    help="Experiment prefix to aggregate (default: E1).")
    args = ap.parse_args()

    if args.aggregate:
        csv_path, json_path = aggregate(args.experiment, args.results_dir)
        print(f"wrote {csv_path}", file=sys.stderr)
        print(f"wrote {json_path}", file=sys.stderr)
        return 0

    if not args.cell_id:
        ap.error("--cell-id is required unless --aggregate is set")
    out = parse_cell(args.results_dir, args.cell_id)
    json.dump(out, sys.stdout, indent=2, sort_keys=True, default=_json_default)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
