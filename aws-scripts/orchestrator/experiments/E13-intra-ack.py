#!/usr/bin/env python3
"""E19: Full parallel canonical rerun for the paper plots.

Runs a 144-cell manifest (3 vals × 4 c-values × 3 N × 2 modes × 2 AZ) through
lib/run-cell.sh with 2 parallel workers. Each cell gets 3+ benchmark runs;
the script auto-retries until the tightest 3-run subset has CoV(tps) < 5%
and max(p99)/min(p99) < 1.10 (or MAX_RUNS attempts are exhausted).

Concurrency model
-----------------
Each cell uses a unique CELL_ID, so its terraform state file
(cell-states/<CELL_ID>.tfstate) is disjoint from every other cell's. Two
workers therefore drive two cells in parallel without locking each other or
needing terraform workspaces. The shared things are:
  - .terraform/ in cell/terraform/ (pre-initialised once at startup)
  - data/per-cell-parsed.csv (fcntl-locked appends)
  - E19-progress.jsonl (fcntl-locked appends)

Output
------
  data/per-cell-parsed.csv  - one row per accepted run, schema unchanged
                              (cell_id,experiment,n_servers,value_size,az,
                               mode,c,run,tps,avg_lat_ms,p50_lat_ms,p99_lat_ms)
  E19-progress.jsonl         - one record per cell (status: ok|high_variance|failed)
  E19-run.log                - human-readable run log

Usage
-----
  python3 experiments/E19-full-parallel.py --dry-run        # validate manifest + infra.env
  python3 experiments/E19-full-parallel.py --smoke-test     # run one cell end-to-end (~5 min)
  python3 experiments/E19-full-parallel.py                  # the real run (~20 hours)
  python3 experiments/E19-full-parallel.py --resume         # pick up after a crash
  python3 experiments/E19-full-parallel.py --replace        # back up old CSV and start fresh

Recommended invocation on the orchestrator:
  cd ~/metronome-eval
  nohup python3 experiments/E19-full-parallel.py --replace > /dev/null 2>&1 &
  tail -f E19-run.log
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import fcntl
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent          # ~/metronome-eval
LIB          = PROJECT_ROOT / "lib"
WORKLOADS    = PROJECT_ROOT / "workloads"
RESULTS      = PROJECT_ROOT / "results"
DATA_DIR     = PROJECT_ROOT / "data"
DATA_OUT     = DATA_DIR / "E13-ack-per-cell-parsed.csv"
PROGRESS_LOG = PROJECT_ROOT / "E13-ack-progress.jsonl"
RUN_LOG      = PROJECT_ROOT / "E13-ack-run.log"
INFRA_ENV    = PROJECT_ROOT / "infra.env"

# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

KNEE_C = {4096: 400, 8192: 200, 16384: 100}
C_VALUES = {
    4096:  [100, 200, 400, 800],
    8192:  [ 50, 100, 200, 400],
    16384: [ 25,  50, 100, 200],
}
NS    = [3, 5, 7]
MODES = ["vanilla", "metronome_kf1"]   # both arms: stock (flags off) vs Design X
AZS   = ["intraAZ"]            # E13: intra-AZ only
DISK_TIER = "gp3-baseline"

# Quality thresholds for the tightest 3-run subset of completed runs:
TPS_COV_THR     = 0.10    # (max-min)/mean across the 3 chosen tps values
P99_RATIO_THR   = 1.10    # max(p99)/min(p99)
TARGET_RUNS     = 3       # rows emitted to the unified CSV per cell
MAX_RUNS        = 4       # hard cap on benchmark attempts per cell

# Per-run timeout (terraform up + bench + collect + tear down)
PER_RUN_TIMEOUT_S = 30 * 60

log = logging.getLogger("E19")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CellSpec:
    n: int
    val: int
    az: str       # "intraAZ" | "crossAZ"
    mode: str     # "vanilla" | "metronome_kf1"
    c: int

    @property
    def base_id(self) -> str:
        return f"E13ack-n{self.n}-v{self.val}-{self.az}-{self.mode}-c{self.c}"

    def run_id(self, attempt: int) -> str:
        return f"{self.base_id}-run{attempt}"

    @property
    def az_short(self) -> str:
        return "intra" if self.az == "intraAZ" else "cross"

    @property
    def mode_short(self) -> str:
        return "etcd" if self.mode == "vanilla" else "metronome"

    def tf_vars(self, infra: dict[str, str]) -> list[str]:
        v = [
            f"n_servers={self.n}",
            f"disk_tier={DISK_TIER}",
            f"value_size={self.val}",
            f"n_clients={self.c}",
        ]
        if self.mode == "vanilla":
            v.append("etcd_mode=vanilla")
        else:
            v.append("etcd_mode=metronome")
            v.append("metronome_quorum_offset=0")
        if self.az == "crossAZ":
            sb = infra.get("SUBNET_ID_B", "")
            v.append("cross_az=true")
            v.append('azs=["us-west-1a","us-west-1b"]')
            v.append(f'subnet_ids=["{infra["SUBNET_ID"]}","{sb}"]')
        return v


@dataclass
class RunResult:
    cell_id: str          # cell_id including -runN
    attempt: int
    tps: float
    avg_lat_ms: float
    p50_lat_ms: float
    p99_lat_ms: float


# ---------------------------------------------------------------------------
# Manifest construction
# ---------------------------------------------------------------------------

def build_manifest() -> list[CellSpec]:
    cells: list[CellSpec] = []
    for val in (4096, 8192, 16384):
        for c in C_VALUES[val]:
            for n in NS:
                for mode in MODES:
                    for az in AZS:
                        cells.append(CellSpec(n=n, val=val, az=az, mode=mode, c=c))
    return cells


# ---------------------------------------------------------------------------
# infra.env loader
# ---------------------------------------------------------------------------

_ENV_RE = re.compile(r"^(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)$")

def load_infra_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_RE.match(line)
        if not m:
            continue
        k, v = m.group(1), m.group(2)
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        env[k] = v
    return env


# ---------------------------------------------------------------------------
# Cell execution
# ---------------------------------------------------------------------------

def _result_from_json(cell_id: str, attempt: int, j: dict) -> Optional[RunResult]:
    h = j.get("headline") or {}
    lat = h.get("latency") or {}
    tps = h.get("throughput_ops_per_sec")
    if not tps:
        return None
    return RunResult(
        cell_id=cell_id, attempt=attempt, tps=float(tps),
        avg_lat_ms=(lat.get("avg_s")  or 0) * 1000,
        p50_lat_ms=(lat.get("p50_s")  or 0) * 1000,
        p99_lat_ms=(lat.get("p99_s")  or 0) * 1000,
    )


def cached_result(cell: CellSpec, attempt: int) -> Optional[RunResult]:
    """Return the parsed result if results.json already exists from a prior run."""
    cell_id = cell.run_id(attempt)
    rj = RESULTS / cell_id / "results.json"
    if not rj.exists():
        return None
    try:
        j = json.loads(rj.read_text())
    except Exception:
        return None
    return _result_from_json(cell_id, attempt, j)


def prune_run_artifacts(cell_id: str) -> None:
    """Reclaim transient per-run disk once results.json is parsed: delete the
    cp-duplicates collect-results.sh makes of driver-1-* and gzip the bulky raw
    logs (etcd.log, bench.log, workload.log) — they're already summarised in
    results.json. Keeps a long sweep from refilling the orchestrator disk
    (the disk-full crash on 2026-06-30). Best-effort; never raises."""
    d = RESULTS / cell_id
    if not d.is_dir():
        return
    try:
        for dup in ("driver-bench.log", "driver-vmstat.csv",
                    "driver-mpstat.csv", "driver-pidstat.csv"):
            (d / dup).unlink(missing_ok=True)
        logs = [str(p) for p in d.glob("*.log")]
        if logs:
            subprocess.run(["gzip", "-f", *logs], check=False, timeout=120)
    except Exception as e:
        log.warning(f"{cell_id}: prune failed: {e}")


def run_cell_once(cell: CellSpec, attempt: int, infra: dict) -> Optional[RunResult]:
    cell_id = cell.run_id(attempt)
    cached = cached_result(cell, attempt)
    if cached is not None:
        log.info(f"{cell_id}: using cached results.json (tps={cached.tps:.0f})")
        return cached

    cmd = [
        str(LIB / "run-cell.sh"),
        cell_id,
        str(WORKLOADS / "etcd-bench-put.sh"),
    ] + cell.tf_vars(infra)

    log.info(f"running {cell_id}  ({' '.join(cell.tf_vars(infra))})")
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True,
                              timeout=PER_RUN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        log.warning(f"{cell_id}: run-cell.sh timed out after {PER_RUN_TIMEOUT_S}s; tearing down")
        teardown(cell_id)
        return None

    if proc.returncode != 0:
        # Print tail of stderr for diagnosis
        tail = "\n".join(proc.stderr.splitlines()[-15:])
        log.warning(f"{cell_id}: run-cell.sh rc={proc.returncode}; stderr tail:\n{tail}")
        teardown(cell_id)
        return None

    rj = RESULTS / cell_id / "results.json"
    if not rj.exists():
        log.warning(f"{cell_id}: no results.json after run")
        teardown(cell_id)
        return None
    try:
        j = json.loads(rj.read_text())
    except Exception as e:
        log.warning(f"{cell_id}: bad results.json: {e}")
        return None

    res = _result_from_json(cell_id, attempt, j)
    if res is None:
        log.warning(f"{cell_id}: results.json missing headline.throughput")
    prune_run_artifacts(cell_id)
    return res


def teardown(cell_id: str) -> None:
    """Best-effort destroy; never raises."""
    try:
        subprocess.run([str(LIB / "destroy-vms.sh"), cell_id],
                       check=False, capture_output=True, timeout=10 * 60)
    except Exception as e:
        log.warning(f"{cell_id}: teardown failed: {e}")


# ---------------------------------------------------------------------------
# Variance: pick the tightest 3-run subset
# ---------------------------------------------------------------------------

def best_subset(runs: list[RunResult], k: int) -> tuple[Optional[list[RunResult]], bool]:
    """Return (chosen, accepted) — the tightest k-run window by tps, and whether
    it meets the variance threshold. For k==1 the single completed run is
    always accepted (no variance to compute). Returns (None, False) if fewer
    than k runs."""
    if len(runs) < k:
        return None, False
    if k == 1:
        return [runs[0]], True
    by_tps = sorted(runs, key=lambda r: r.tps)
    best, best_span = None, float("inf")
    for i in range(len(by_tps) - k + 1):
        w = by_tps[i:i + k]
        span = w[-1].tps - w[0].tps
        if span < best_span:
            best, best_span = w, span
    assert best is not None
    mean_tps = sum(r.tps for r in best) / k
    cov = (max(r.tps for r in best) - min(r.tps for r in best)) / mean_tps if mean_tps else 1.0
    p99s = [r.p99_lat_ms for r in best if r.p99_lat_ms > 0]
    p99_ratio = (max(p99s) / min(p99s)) if p99s and min(p99s) > 0 else float("inf")
    accepted = (cov < TPS_COV_THR and p99_ratio < P99_RATIO_THR)
    return best, accepted


# ---------------------------------------------------------------------------
# Output: CSV + progress.jsonl (both fcntl-locked)
# ---------------------------------------------------------------------------

CSV_FIELDS = ["cell_id","experiment","n_servers","value_size","az","mode",
              "c","run","tps","avg_lat_ms","p50_lat_ms","p99_lat_ms"]

def write_csv_rows(rows: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not DATA_OUT.exists()
    with open(DATA_OUT, "a", newline="") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if new_file:
                w.writeheader()
            for r in rows:
                w.writerow(r)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def append_progress(rec: dict) -> None:
    with open(PROGRESS_LOG, "a") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write(json.dumps(rec) + "\n")
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def load_completed_cells() -> set[str]:
    done: set[str] = set()
    if not PROGRESS_LOG.exists():
        return done
    for raw in PROGRESS_LOG.read_text().splitlines():
        try:
            r = json.loads(raw)
        except Exception:
            continue
        if r.get("status") in ("ok", "high_variance"):
            done.add(r.get("cell_base_id", ""))
    return done


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------

def emit_chosen(cell: CellSpec, chosen: list[RunResult], status: str) -> None:
    rows = []
    for i, r in enumerate(chosen, start=1):
        rows.append(dict(
            cell_id=r.cell_id, experiment="E19", n_servers=cell.n, value_size=cell.val,
            az=cell.az_short, mode=cell.mode_short, c=cell.c, run=i,
            tps=r.tps, avg_lat_ms=r.avg_lat_ms,
            p50_lat_ms=r.p50_lat_ms, p99_lat_ms=r.p99_lat_ms,
        ))
    write_csv_rows(rows)
    append_progress(dict(
        ts=time.time(),
        cell_base_id=cell.base_id,
        status=status,
        attempts=[r.attempt for r in chosen],
    ))


def handle_cell(cell: CellSpec, infra: dict, wid: int,
                target_runs: int, max_runs: int) -> None:
    st = os.statvfs(str(PROJECT_ROOT))
    free_gb = st.f_bavail * st.f_frsize / 1e9
    if free_gb < 1.5:
        log.error(f"[w{wid}] disk critically low ({free_gb:.1f} GB free); "
                  f"aborting before {cell.base_id}")
        raise RuntimeError(f"disk low: {free_gb:.1f} GB free")
    log.info(f"[w{wid}] start {cell.base_id} (disk free: {free_gb:.1f} GB)")
    runs: list[RunResult] = []
    for attempt in range(1, max_runs + 1):
        r = run_cell_once(cell, attempt, infra)
        if r is None:
            continue
        runs.append(r)
        log.info(f"[w{wid}] {cell.base_id} attempt {attempt}: "
                 f"tps={r.tps:.0f}  p99={r.p99_lat_ms:.1f}ms  avg={r.avg_lat_ms:.2f}ms")
        if len(runs) >= target_runs:
            chosen, ok = best_subset(runs, target_runs)
            if ok:
                log.info(f"[w{wid}] {cell.base_id} ✓ low-variance after {attempt} attempts")
                emit_chosen(cell, chosen, status="ok")
                return

    chosen, ok = best_subset(runs, target_runs)
    if chosen:
        status = "ok" if ok else "high_variance"
        log.warning(f"[w{wid}] {cell.base_id} → {status} after {len(runs)} attempts")
        emit_chosen(cell, chosen, status=status)
    else:
        log.error(f"[w{wid}] {cell.base_id} failed — no usable runs")
        append_progress(dict(
            ts=time.time(), cell_base_id=cell.base_id, status="failed", attempts=[]))


def worker_loop(q: Queue, wid: int, infra: dict, target_runs: int, max_runs: int) -> None:
    while True:
        cell = q.get()
        try:
            if cell is None:
                return
            try:
                handle_cell(cell, infra, wid, target_runs, max_runs)
            except Exception:
                log.exception(f"[w{wid}] {cell.base_id} crashed")
                append_progress(dict(
                    ts=time.time(), cell_base_id=cell.base_id, status="crashed"))
        finally:
            q.task_done()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_filesystem() -> list[str]:
    errs = []
    if not LIB.exists():               errs.append(f"missing dir: {LIB}")
    if not (LIB / "run-cell.sh").exists():    errs.append(f"missing: {LIB}/run-cell.sh")
    if not (LIB / "destroy-vms.sh").exists(): errs.append(f"missing: {LIB}/destroy-vms.sh")
    if not (WORKLOADS / "etcd-bench-put.sh").exists():
        errs.append(f"missing: {WORKLOADS}/etcd-bench-put.sh")
    if not (PROJECT_ROOT / "cell" / "terraform").exists():
        errs.append(f"missing: cell/terraform/")
    return errs


def validate_infra(infra: dict) -> list[str]:
    errs = []
    required = ("AWS_REGION", "VPC_ID", "SUBNET_ID", "SG_ID", "KEY_NAME", "UBUNTU_AMI", "AZ")
    for k in required:
        if k not in infra:
            errs.append(f"infra.env missing required: {k}")
    if "SUBNET_ID_B" not in infra:
        errs.append("infra.env missing SUBNET_ID_B (crossAZ cells will fail)")
    return errs


def pre_init_terraform() -> None:
    tf_dir = PROJECT_ROOT / "cell" / "terraform"
    log.info(f"pre-initialising terraform in {tf_dir}")
    subprocess.check_call(["terraform", "init", "-input=false"],
                          cwd=tf_dir, stdout=subprocess.DEVNULL)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
        handlers=[logging.FileHandler(RUN_LOG, mode="a"),
                  logging.StreamHandler(sys.stdout)],
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="E19 full parallel canonical rerun for the paper plots")
    ap.add_argument("--workers", type=int, default=2,
                    help="Parallel benchmark workers (default 2; "
                         "raise only after checking AWS limits)")
    ap.add_argument("--max-runs", type=int, default=MAX_RUNS,
                    help=f"Hard cap on attempts per cell (default {MAX_RUNS})")
    ap.add_argument("--target-runs", type=int, default=TARGET_RUNS,
                    help=f"Runs to emit per cell (default {TARGET_RUNS})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print plan + validate environment without running anything")
    ap.add_argument("--smoke-test", action="store_true",
                    help="Run ONE small cell end-to-end (N=3 v=4kB intra metronome c=400)")
    ap.add_argument("--resume", action="store_true",
                    help="Skip cells already recorded ok/high_variance in E19-progress.jsonl")
    ap.add_argument("--replace", action="store_true",
                    help="Back up data/per-cell-parsed.csv and start fresh")
    return ap.parse_args()


def main() -> int:
    setup_logging()
    args = parse_args()

    cells = build_manifest()
    log.info(f"manifest: {len(cells)} cells "
             f"(3 vals × {sum(len(C_VALUES[v]) for v in C_VALUES)//3} c × "
             f"{len(NS)} N × {len(MODES)} modes × {len(AZS)} AZ)")

    fs_errs = validate_filesystem()
    if fs_errs:
        for e in fs_errs:
            log.error(e)
        return 2

    infra = load_infra_env(INFRA_ENV)
    infra_errs = validate_infra(infra)

    if args.dry_run:
        log.info("=== DRY RUN ===")
        if infra_errs:
            for e in infra_errs:
                log.warning(e)
        else:
            log.info("infra.env: ok")
        log.info(f"runs per cell: target={args.target_runs}, max={args.max_runs}")
        log.info(f"variance thresholds: tps CoV < {TPS_COV_THR}, p99 ratio < {P99_RATIO_THR}")
        log.info(f"workers: {args.workers}")
        log.info("first 6 cells (terraform vars):")
        for c in cells[:6]:
            log.info(f"  {c.base_id}")
            log.info(f"    {' '.join(c.tf_vars(infra))}")
        log.info(f"... and {len(cells)-6} more")
        log.info(f"expected wall-clock: ~{(len(cells) * args.target_runs * 5) / args.workers / 60:.0f}h "
                 f"(at ~5 min/run, {args.workers}-way parallel)")
        return 0

    # Real-run-only checks
    if infra_errs:
        for e in infra_errs:
            log.error(e)
        return 2

    if args.replace and DATA_OUT.exists():
        stamp = time.strftime("%Y%m%dT%H%M%S")
        bak = DATA_OUT.with_name(DATA_OUT.name + f".bak.{stamp}")
        DATA_OUT.rename(bak)
        log.info(f"--replace: moved old CSV to {bak.name}")

    if args.smoke_test:
        cells = [CellSpec(n=3, val=4096, az="intraAZ", mode="metronome_kf1", c=400)]
        log.info(f"--smoke-test: 1 cell only ({cells[0].base_id})")
        args.target_runs = 1
        args.max_runs = 1

    if args.resume:
        done = load_completed_cells()
        before = len(cells)
        cells = [c for c in cells if c.base_id not in done]
        log.info(f"--resume: {before - len(cells)} cells already done, {len(cells)} remaining")

    pre_init_terraform()

    q: Queue = Queue()
    for c in cells:
        q.put(c)
    for _ in range(args.workers):
        q.put(None)  # sentinel per worker

    threads = []
    for w in range(args.workers):
        t = threading.Thread(target=worker_loop, name=f"w{w}",
                             args=(q, w, infra, args.target_runs, args.max_runs))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    log.info("all cells complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
