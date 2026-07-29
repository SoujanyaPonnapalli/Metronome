# aws-scripts — Metronome EC2 benchmark harness + session drivers

Scripts used to launch and orchestrate the Metronome (Design X) performance
experiments on AWS. Two parts:

- **`orchestrator/`** — the harness that lives on the eval orchestrator box
  (`ubuntu@18.144.2.232:~/metronome-eval`, us-west-1). Copied verbatim (minus
  `*.bak`, `__pycache__`, terraform state, and `.terraform/` provider blobs).
- **`session-helpers/`** — the local driver/monitor scripts used from the dev
  machine to stop / clean / swap binaries / relaunch / monitor the sweeps.

The SSH key (`~/.ssh/metronome-aws.pem`) and the built etcd binaries are **not**
included here.

## How a run works (orchestrator/)

1. **`scripts/build-binaries.sh`** — builds the etcd binaries and stages them in
   `/opt/metronome-eval/bin/` on the orchestrator: `etcd-vanilla` (stock) and
   `etcd-metronome` (the metronome/Design X build). (In practice we cross-built
   linux/amd64 locally and scp'd them; see session-helpers.)
2. **`experiments/<E>.py|sh`** — the experiment driver. It defines the cell
   manifest (N × value_size × concurrency × mode × AZ), runs a worker pool, and
   writes `data/<...>-per-cell-parsed.csv` + a `*-progress.jsonl`.
   - `E13-intra-ack.py` — the intra-AZ both-arms sweep run this session
     (vanilla vs metronome, 3 vals × 4 c × 3 N × 2 modes). Flags: `--replace`,
     `--resume`, `--workers N`, `--smoke-test`, `--target-runs`, `--max-runs`.
     Includes the per-run **prune** (`prune_run_artifacts`) + disk guard.
   - `E19-full-parallel.py` — the 144-cell canonical paper sweep;
     `E19-build-latency-budget.py` builds the latency-budget CSV.
3. Per cell, the driver calls **`lib/run-cell.sh`**, which composes:
   - **`lib/launch-vms.sh`** — `terraform apply` of `cell/terraform/` (N servers
     + 1 driver, on-demand, no placement group; each server gets a 200 GB gp3
     data volume). `run-cell.sh` has an EXIT trap → `destroy-vms.sh`.
   - **`lib/run-workload.sh`** — scp's the chosen binary to the servers
     (`etcd_mode` → `etcd-vanilla` / `etcd-metronome`), starts the cluster with
     the right flags, and runs the benchmark (`workloads/etcd-bench-put.sh`).
   - **`lib/collect-results.sh`** — pulls per-server metrics (`*.prom`, iostat,
     proposals-sampler) + driver bench log, then `lib/parse-bench.py` writes
     `results/<cell_id>/results.json`.
   - **`lib/destroy-vms.sh`** — `terraform destroy` (removes instances + volumes).
4. `lib/{find-knee.py, plot-results.py}` + the paper repo's plot scripts turn the
   parsed CSV into the figures.

`infra.env` holds the AWS config (region, VPC/subnet/SG ids, key name, AMI) —
IDs only, no secrets.

## Key operational lessons (learned the hard way this session)

- **`--metronome-work-steal-timeout`**: the default (1 s) **misfires** under heavy
  benchmark load — a transient commit stall trips work-steal, which flips the
  node into "log-everything" for the 60 s window and **wipes out metronome's byte
  savings for the whole cell** (metronome then writes the same WAL bytes as
  vanilla → looks like no benefit). For a NO-FAILURE benchmark, pass a high value
  (`--metronome-work-steal-timeout=300s`) so it never fires. `run-workload.sh`'s
  metronome `MODE_FLAG` was patched to include it.
- **Validity check before trusting any metronome number**: confirm
  `etcd_metronome_work_steals_triggered_total == 0` AND metronome
  `etcd_disk_wal_write_bytes_total ≈ (K/N) × vanilla` (≈2/3 for N=3,K=2). If not,
  work-steal disabled metronome and the result is vanilla + overhead.
- **Orchestrator disk (49 G) fills up** → terraform can't persist state → sweep
  crashes → **orphaned instances + volumes + stale tfstate locks**. Keep results
  pruned (the E13 script does this); clear `results.bak*`/`results-v1*` if low.
- **EBS leak**: cell data volumes are NOT delete-on-termination-only in the crash
  path; if a run is killed mid-apply, terminate leftover instances **by tag**
  (`Name=E13ack-*`) and delete the `available` volumes.
- **`pgrep -f` self-match footgun**: use the bracket trick (`pgrep -f "[E]13-…"`).

## session-helpers/ (run from the dev machine; hardcode ORCH=18.144.2.232, KEY=~/.ssh/metronome-aws.pem)

- `stop-clean-only.sh` — stop the sweep + force-clean all E13ack infra (no relaunch).
- `stop-clean-swap.sh` — stop/clean, swap in a staged binary, relaunch the sweep.
- `relaunch-both.sh`, `relaunch-both-v2.sh` — wait for teardown, clean, relaunch the
  both-arms sweep (v2 fixes the `pgrep -c` exit-code bug in v1).
- `sweep-complete-monitor.sh`, `sweep-opt-monitor.sh`, `sweep-final-monitor.sh` —
  poll until a sweep finishes, then dump a summary (log tail, cells-by-mode,
  residual instances).
- `plot_e13_summary.py` — throughput/latency summary plot from the parsed CSV.

_Not tracked in git by default. Copied 2026-06-30._
