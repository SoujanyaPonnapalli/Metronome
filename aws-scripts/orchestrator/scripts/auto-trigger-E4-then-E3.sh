#!/usr/bin/env bash
# Background watcher (intended to run via nohup on the orchestrator).
# Waits for E1 to finish, then launches E4 (disk sensitivity) and
# afterwards E3 (stragglers), each in its own tmux session.
#
# Exits:
#   0  - both E4 and E3 launched successfully
#   2  - some prerequisite did not produce its sweep-complete marker
#   3  - per-stage wait ceiling exceeded
#
# Reads progress from this script's own ~/auto-trigger-E4-then-E3.log.
set -uo pipefail
PROJECT_ROOT=/home/ubuntu/metronome-eval

note() { echo "[$(date -u +%FT%TZ)] $*"; }

wait_session() {
  local name="$1" max_wait="$2"
  local start=$(date +%s)
  note "waiting for tmux session '$name' to end (ceiling ${max_wait}s)"
  while tmux has-session -t "$name" 2>/dev/null; do
    if (( $(date +%s) - start > max_wait )); then
      note "ERROR: ceiling exceeded waiting for $name"
      return 1
    fi
    sleep 60
  done
  note "tmux session '$name' ended after $(( $(date +%s) - start ))s"
  return 0
}

launch_in_tmux() {
  local sess="$1" script="$2"
  mv "$HOME/${sess}-sweep.log" "$HOME/${sess}-sweep.log.$(date +%Y%m%dT%H%M%S)" 2>/dev/null || true
  tmux new-session -d -s "$sess" -c "$PROJECT_ROOT" \
    "bash ${script} 2>&1 | tee ~/${sess}-sweep.log"
  note "${sess} launched."
}

# ---------- Wait for E1 (16h ceiling) ----------
if ! wait_session E1 $((16 * 3600)); then
  exit 3
fi
if ! grep -q "E1 sweep complete" "$HOME/E1-sweep.log" 2>/dev/null; then
  note "WARN: 'E1 sweep complete' marker missing in ~/E1-sweep.log (may have crashed); proceeding"
fi

# ---------- Launch E4 (disk sensitivity) ----------
note "launching E4 (disk sensitivity, ~2-3h expected)"
launch_in_tmux E4 experiments/E4-disk-sensitivity.sh

# Wait for E4 (4h ceiling for safety)
if ! wait_session E4 $((4 * 3600)); then
  note "WARN: E4 wait ceiling exceeded — launching E3 anyway"
fi
if ! grep -q "E4 sweep complete" "$HOME/E4-sweep.log" 2>/dev/null; then
  note "WARN: 'E4 sweep complete' marker missing in ~/E4-sweep.log; proceeding to E3"
fi

# ---------- Launch E3 (stragglers) ----------
note "launching E3 (stragglers, ~1-1.5h expected)"
launch_in_tmux E3 experiments/E3-stragglers.sh

note "auto-trigger exiting; E3 still running"
