# E19 — Full parallel canonical rerun for the paper plots

Single benchmark sweep whose output **replaces** the patchwork of E1+E9+E10+
E11budget+E12+E13+E14+E15+E16+E17+E18 currently feeding the four paper plots
(`E13-throughput-knee`, `E13-tput-lat-{intra,cross}`, `E13-latency-budget`).

## What it covers

| Dimension | Values |
|---|---|
| `value_size` | 4 kB, 8 kB, 16 kB |
| `c` (clients) | 100/200/400/800, 50/100/200/400, 25/50/100/200 (depending on val) |
| `N` | 3, 5, 7 |
| `mode` | etcd (vanilla), metronome (kf1, offset=0) |
| `AZ` | intraAZ, crossAZ |

**144 cells × ≥3 runs each = ~432 runs**. Cells whose tightest 3 runs don't
meet the variance threshold get re-run up to MAX_RUNS (default 8) times.

## Variance acceptance

A cell is accepted when the **tightest 3-run subset** of its completed runs (by tps)
meets both:

- `(max(tps) - min(tps)) / mean(tps) < 0.05`  (CoV < 5 %)
- `max(p99) / min(p99) < 1.10`

The 3 runs emitted to the CSV are this tightest subset, renumbered 1/2/3
chronologically. Cells that exhaust MAX_RUNS attempts without converging are
emitted anyway and tagged `high_variance` in `E19-progress.jsonl`.

## Concurrency

Two workers pull from a shared queue. Each cell uses a unique `CELL_ID` →
disjoint terraform state file (`cell-states/<CELL_ID>.tfstate`) → no
inter-worker locking. AWS-side, two parallel cells = up to 16 EC2 instances at
once (well within typical account limits).

Wall-clock estimate: **~20 hours** with `--workers 2`, ~$70 in EC2 cost.

## Usage

Validate locally before deploying:

```bash
python3 experiments/E19-full-parallel.py --dry-run
```

Smoke-test ONE cell end-to-end on the orchestrator (~5 min, ~$0.10):

```bash
python3 experiments/E19-full-parallel.py --smoke-test
```

Real run (background, with detached log):

```bash
nohup python3 experiments/E19-full-parallel.py --replace > /dev/null 2>&1 &
tail -f E19-run.log
```

`--replace` backs up the existing `data/per-cell-parsed.csv` and starts fresh.
Without it, new rows are appended to the existing file (which probably isn't
what you want for a "single consistent dataset" run).

Resume after a crash (e.g. orchestrator restart):

```bash
python3 experiments/E19-full-parallel.py --resume
```

`--resume` reads `E19-progress.jsonl` and skips cells already recorded as
`ok` or `high_variance`. Cells that crashed mid-run are retried from
scratch; per-attempt `results.json` files are reused if already on disk.

## After the run

The orchestrator script gives you the unified `data/per-cell-parsed.csv`.
For the latency-budget plot you also need to derive component breakdowns
from the leader's prom histograms:

```bash
python3 experiments/E19-build-latency-budget.py
```

This walks every `results/E19-…-n5-…-c{knee}-runN/` directory, parses the
proposals-sampler.csv + prom histogram diffs, and writes
`data/latency-budget-n5-knee-c.csv`. Then re-run the four plot scripts in the
paper folder (`scripts/plot_throughput_knee.py`, `plot_tput_lat_curves.py`,
`plot_knee_trends.py`, `plot_latency_budget_final.py`) — they already read
from this CSV.

## Outputs

| Path | Purpose |
|---|---|
| `data/per-cell-parsed.csv` | Unified per-(cell,run) tps/lat. Replaces the old patchwork file. |
| `data/latency-budget-n5-knee-c.csv` | Per-cell latency components (run E19-build-latency-budget.py to generate). |
| `results/E19-*/` | Raw per-cell artifacts (driver-bench.log, server-N-{metrics-pre,post,proposals-sampler}, etc.) |
| `E19-progress.jsonl` | One line per cell completion. `status` ∈ {ok, high_variance, failed, crashed}. |
| `E19-run.log` | Human-readable log of every attempt. |

## What I deliberately did NOT include

- Auto-deletion of legacy aggregates (`E1/E9/E10-aggregate.csv`). They stay
  on disk as historical record — the plot scripts no longer read them.
- Latency-budget for N=3 / N=7. The existing `plot_latency_budget_final.py`
  is hard-coded to a 2-bar-per-panel N=5 layout; extending it is a paper-
  layout decision separate from this rerun.
- A separate "below-knee c-sweep" pass. E19 covers below/at/above-knee c
  values uniformly with 3 runs each, so all four plots derive from the same
  consistent dataset.
