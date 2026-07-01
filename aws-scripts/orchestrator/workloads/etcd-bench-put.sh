#!/usr/bin/env bash
# Workload: write-only PUT sweep with etcd's official `benchmark` tool.
# Invoked on the driver VM by lib/run-workload.sh.
#
# Args:
#   $1 = comma-separated endpoints (http://prv1:2379,...)
#   $2 = cell_id
#   $3+ = k=v overrides, e.g. value_size=4096, n_clients=500, total=200000
#
# Output (stdout, captured into driver-bench.log):
#   one or more "Summary:" blocks from the etcd benchmark, prefixed with
#   "## CELL: <cell_id> phase=<warmup|measure|cooldown> concurrency=<c>"
set -euo pipefail

ENDPOINTS="${1:?endpoints required}"; shift
CELL_ID="${1:?cell_id required}"; shift

# Defaults — overridable by k=v from the experiment script.
VALUE_SIZE=4096
KEY_SIZE=64
N_CLIENTS=200          # number of concurrent clients (== concurrency)
N_CONNS=200            # number of connections (paper convention: == clients)
TOTAL_WARMUP=2000      # ops for warmup
TOTAL_MEASURE=200000   # ops for measurement window
TOTAL_COOLDOWN=2000

for kv in "$@"; do
  case "$kv" in
    value_size=*)     VALUE_SIZE="${kv#*=}" ;;
    key_size=*)       KEY_SIZE="${kv#*=}" ;;
    n_clients=*)      N_CLIENTS="${kv#*=}"; N_CONNS="${kv#*=}" ;;
    n_conns=*)        N_CONNS="${kv#*=}" ;;
    total_measure=*)  TOTAL_MEASURE="${kv#*=}" ;;
    total_warmup=*)   TOTAL_WARMUP="${kv#*=}" ;;
    total_cooldown=*) TOTAL_COOLDOWN="${kv#*=}" ;;
    *) echo "## ignored kv: $kv" ;;
  esac
done

run_phase() {
  local phase="$1" total="$2"
  echo "## CELL: $CELL_ID phase=$phase concurrency=$N_CLIENTS value_size=$VALUE_SIZE total=$total"
  /usr/local/bin/etcd-benchmark \
    --endpoints="$ENDPOINTS" \
    --clients="$N_CLIENTS" --conns="$N_CONNS" \
    put --key-size="$KEY_SIZE" --val-size="$VALUE_SIZE" --total="$total" \
    || true
  echo
}

run_phase warmup    "$TOTAL_WARMUP"
run_phase measure   "$TOTAL_MEASURE"
run_phase cooldown  "$TOTAL_COOLDOWN"
