#!/usr/bin/env python3
"""Post-bench: build the latency-budget CSV from E19 N=5 cells at the knee c.

Run this AFTER E19-full-parallel.py finishes. It walks every E19 cell directory
with N=5 at the knee c (c=400/200/100 for v=4k/8k/16k), parses the leader's
prom histograms + proposals-sampler.csv, and emits data/latency-budget-n5-knee-c.csv
in the schema that scripts/plot_latency_budget_final.py expects.

Schema: value_size,az,mode,c,run,p50,req_per_call,fsync_leader,apply,wfq,aqw,propc,forwarding
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS = PROJECT_ROOT / "results"
DATA_OUT = PROJECT_ROOT / "data" / "latency-budget-n5-knee-c.csv"

KNEE_C = {4096: 400, 8192: 200, 16384: 100}

def _read_text_maybe_gz(path: Path) -> str:
    """Read a file as text; transparently handle .gz alongside the raw path."""
    if path.exists():
        return path.read_text()
    gz = path.with_suffix(path.suffix + ".gz")
    if gz.exists():
        import gzip
        with gzip.open(gz, "rt") as f:
            return f.read()
    raise FileNotFoundError(path)


def _exists_maybe_gz(path: Path) -> bool:
    return path.exists() or path.with_suffix(path.suffix + ".gz").exists()




def diff_sum_count(pre: str, post: str, metric: str, label_filter: str = "") -> tuple[float, float]:
    def grab(text: str, suffix: str) -> float:
        t = 0.0
        for line in text.splitlines():
            if not line.startswith(metric + suffix):
                continue
            if label_filter and label_filter not in line:
                continue
            tail = line[len(metric) + len(suffix):].strip()
            if tail.startswith("{"):
                tail = tail[tail.rfind("}") + 1:].strip()
            t += float(tail.split()[0])
        return t
    return grab(post, "_sum") - grab(pre, "_sum"), grab(post, "_count") - grab(pre, "_count")


def find_leader(cell_dir: Path, n_servers: int) -> Optional[int]:
    for i in range(1, n_servers + 1):
        p = cell_dir / f"server-{i}-metrics-post.prom"
        if not _exists_maybe_gz(p):
            continue
        for line in _read_text_maybe_gz(p).splitlines():
            if line.startswith("etcd_server_is_leader 1"):
                return i
    return None


def analyze_sampler(csvf: Path) -> Optional[dict]:
    rows: list[tuple[float, int, int, int]] = []
    if not _exists_maybe_gz(csvf):
        return None
    for raw in _read_text_maybe_gz(csvf).splitlines():
        if raw.startswith("ts,"):
            continue
        parts = raw.strip().split(",")
        if len(parts) != 4:
            continue
        try:
            rows.append((float(parts[0]),
                         int(parts[1] or 0),
                         int(parts[2] or 0),
                         int(parts[3] or 0)))
        except ValueError:
            continue
    if len(rows) < 20:
        return None
    WIN = 5
    high = []
    for i in range(len(rows) - WIN):
        dt = rows[i + WIN][0] - rows[i][0]
        dc = rows[i + WIN][2] - rows[i][2]
        if dt > 0 and dc / dt > 500:
            high.append(i)
    if not high:
        return None
    sub = rows[high[0]:high[-1] + 1]
    ts_span = sub[-1][0] - sub[0][0]
    if ts_span <= 0:
        return None
    return dict(
        commit_rate=(sub[-1][2] - sub[0][2]) / ts_span,
        mean_pending=sum(r[1] for r in sub) / len(sub),
        mean_backlog=sum(r[2] - r[3] for r in sub) / len(sub),
    )


def analyze_run(cell_dir: Path, n_servers: int) -> Optional[dict]:
    leader = find_leader(cell_dir, n_servers)
    if leader is None:
        return None
    pre  = _read_text_maybe_gz(cell_dir / f"server-{leader}-metrics-pre.prom")
    post = _read_text_maybe_gz(cell_dir / f"server-{leader}-metrics-post.prom")
    rs, rc = diff_sum_count(pre, post, "etcd_server_request_duration_seconds", 'type="Put"')
    if rc <= 0:
        return None
    req_per_call = rs / rc * 1000
    fs, fc = diff_sum_count(pre, post, "etcd_disk_wal_fsync_duration_seconds")
    fsync_leader = fs / fc * 1000 if fc else 0
    aps, apc = diff_sum_count(pre, post, "etcd_server_apply_duration_seconds", 'op="Put",success="true"')
    apply_leader = aps / apc * 1000 if apc else 0
    s = analyze_sampler(cell_dir / f"server-{leader}-proposals-sampler.csv")
    if s is None:
        return None
    wfq = (s["mean_pending"] / s["commit_rate"]) * 1000 if s["commit_rate"] else 0
    aqw = max((s["mean_backlog"] / s["commit_rate"]) * 1000, 0)
    propc = max(req_per_call - wfq - apply_leader - aqw, 0)
    bench = _read_text_maybe_gz(cell_dir / "driver-bench.log")
    m = re.search(r"phase=measure.*?(?=phase=cooldown|\Z)", bench, re.DOTALL)
    if not m:
        return None
    body = m.group(0)
    avg = float(re.search(r"Average:\s+([\d.]+)\s+secs", body).group(1)) * 1000
    forwarding = max(avg - req_per_call, 0)
    return dict(p50=avg, req_per_call=req_per_call, fsync_leader=fsync_leader,
                apply=apply_leader, wfq=wfq, aqw=aqw, propc=propc, forwarding=forwarding)


CELL_RE = re.compile(
    r"^E19-n(?P<n>\d+)-v(?P<val>\d+)-(?P<az>intra|cross)AZ-(?P<mode>vanilla|metronome_kf1)-c(?P<c>\d+)-run(?P<run>\d+)$")


def main() -> int:
    rows: list[dict] = []
    n_total = n_match = 0
    for d in sorted(RESULTS.glob("E19-*")):
        if not d.is_dir():
            continue
        n_total += 1
        m = CELL_RE.match(d.name)
        if not m:
            continue
        n = int(m.group("n"))
        if n != 5:
            continue
        val = int(m.group("val"))
        c = int(m.group("c"))
        if c != KNEE_C.get(val):
            continue
        n_match += 1
        az = m.group("az")
        mode = "etcd" if m.group("mode") == "vanilla" else "metronome"
        run = int(m.group("run"))
        res = analyze_run(d, n_servers=5)
        if not res:
            print(f"  skip {d.name} (analyze_run failed)", file=sys.stderr)
            continue
        rows.append(dict(value_size=val, az=az, mode=mode, c=c, run=run, **res))

    DATA_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "value_size", "az", "mode", "c", "run",
            "p50", "req_per_call", "fsync_leader", "apply", "wfq", "aqw", "propc", "forwarding"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"scanned {n_total} E19 cell dirs, parsed {n_match} N=5 knee-c cells, wrote {len(rows)} rows to {DATA_OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
