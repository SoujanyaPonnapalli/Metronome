#!/usr/bin/env bash
# Background watcher (intended to run via nohup on the orchestrator).
# Waits for the E0 tmux session to exit, sanity-checks E0 output,
# launches E1 in a new tmux session.
#
# Exits:
#   0 - E1 launched successfully
#   2 - E0 did not produce a usable E0-peak-loads.json
#   3 - 12h ceiling exceeded
#
# Reads progress from this script's own ~/auto-trigger-E1.log.
set -uo pipefail
PROJECT_ROOT=/home/ubuntu/metronome-eval

note() { echo "[$(date -u +%FT%TZ)] $*"; }

start=$(date +%s)
note "watcher started; waiting for tmux session E0 to end"
while tmux has-session -t E0 2>/dev/null; do
  if (( $(date +%s) - start > 12 * 3600 )); then
    note "ERROR: 12h ceiling exceeded; giving up"
    exit 3
  fi
  sleep 60
done
note "tmux session E0 ended after $(( $(date +%s) - start ))s"

# Verify E0 actually finished and wrote the peak-loads file.
if ! grep -q "E0 sweep complete" "$HOME/E0-sweep.log" 2>/dev/null; then
  note "WARN: 'E0 sweep complete' marker missing in ~/E0-sweep.log (E0 may have crashed)"
fi
PEAK="$PROJECT_ROOT/results/E0-peak-loads.json"
if [[ ! -s "$PEAK" ]]; then
  note "ERROR: $PEAK missing or empty; not launching E1"
  exit 2
fi
note "E0 peak-loads:"
cat "$PEAK" | sed 's/^/    /'

# Hand off to E1.
note "launching E1 in tmux session E1"
mv "$HOME/E1-sweep.log" "$HOME/E1-sweep.log.$(date +%Y%m%dT%H%M%S)" 2>/dev/null || true
tmux new-session -d -s E1 -c "$PROJECT_ROOT" \
  "bash experiments/E1-failure-free-perf.sh 2>&1 | tee ~/E1-sweep.log"
note "E1 launched."
