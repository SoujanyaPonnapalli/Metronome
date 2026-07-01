#!/usr/bin/env bash
# Background watcher (nohup'd on orchestrator). Waits for E8 tmux to
# end, then provisions the cross-AZ subnet and launches E7 in tmux.
#
# Exits:
#   0 - both E8 and E7 launched (E7 still running on exit)
#   2 - missing prereq
#   3 - 8h ceiling exceeded
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

# ---------- Wait for E8 (8h ceiling) ----------
if ! wait_session e8 $((8 * 3600)); then
  exit 3
fi
if ! grep -q "E8 complete" "$HOME/E8-sweep.log" 2>/dev/null; then
  note "WARN: 'E8 complete' marker missing in ~/E8-sweep.log; proceeding"
fi

# ---------- Provision cross-AZ subnet ----------
note "running setup-cross-az-subnet.sh"
bash "$PROJECT_ROOT/infra/setup-cross-az-subnet.sh"

# Re-source infra.env so we pick up the new SUBNET_ID_B.
source "$PROJECT_ROOT/infra.env"
if [[ -z "${SUBNET_ID_B:-}" ]]; then
  note "ERROR: SUBNET_ID_B not set after subnet provisioning"
  exit 2
fi
note "SUBNET_ID_B=$SUBNET_ID_B"

# ---------- Launch E7 in tmux ----------
note "launching E7 in tmux"
mv "$HOME/E7-sweep.log" "$HOME/E7-sweep.log.$(date +%Y%m%dT%H%M%S)" 2>/dev/null || true
tmux new-session -d -s e7 -c "$PROJECT_ROOT" \
  "bash experiments/E7-cross-az.sh 2>&1 | tee ~/E7-sweep.log"
note "E7 launched. auto-trigger exiting; E7 still running."
