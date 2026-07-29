#!/usr/bin/env bash
# Block until the E1 sweep finishes on the orchestrator, then auto-fetch the
# final plots. Designed to be invoked via Bash run_in_background so the agent
# is notified the moment it exits.
#
# Exits:
#   0  - sweep completed; plots fetched.
#   2  - sweep log shows an error pattern.
#   3  - no progress for >1h (likely stalled/dead).
#   4  - 14h max wall exceeded (defensive ceiling).
#
# Args: experiment label (default E1).
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"
EXPERIMENT="${1:-E1}"
LOCAL_LOG="$PROJECT_ROOT/poll-${EXPERIMENT}.log"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=30 -o ServerAliveInterval=30 -o LogLevel=ERROR
          -i ~/.ssh/${KEY_NAME}.pem)

start_ts=$(date +%s)
last_progress_ts=$start_ts
last_completed=-1
poll_interval=300        # 5 min
max_wall=$((16*3600))    # 16h ceiling
stale_window=$((6*3600)) # 6h with no new cells -> stalled (covers E0 + E1 transitions)

note() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOCAL_LOG"; }

while :; do
  now=$(date +%s)
  elapsed=$((now - start_ts))
  if (( elapsed > max_wall )); then
    note "ERROR: 14h ceiling exceeded; giving up"
    exit 4
  fi

  # Single ssh that returns: "<completed-cell-count>|<has-complete-marker>|<tail>"
  status=$(ssh "${SSH_OPTS[@]}" "ubuntu@$ORCHESTRATOR_IP" bash <<EOF 2>/dev/null || echo "0|0|ssh-failed"
cd ~/metronome-eval
n_done=\$(ls results/${EXPERIMENT}-*/results.json 2>/dev/null | wc -l)
done_marker=\$(grep -c "sweep complete" ~/${EXPERIMENT}-sweep.log 2>/dev/null || echo 0)
tail_line=\$(tail -n 1 ~/${EXPERIMENT}-sweep.log 2>/dev/null | tr '|' ',' | head -c 200)
echo "\${n_done}|\${done_marker}|\${tail_line}"
EOF
  )
  IFS='|' read -r n_done done_marker tail_line <<< "$status"
  n_done=${n_done:-0}
  done_marker=${done_marker:-0}

  note "elapsed=${elapsed}s  cells_done=${n_done}/108  done_marker=${done_marker}  tail: ${tail_line}"

  if [[ "$done_marker" =~ ^[1-9] ]]; then
    note "SWEEP COMPLETE — fetching final plots"
    bash "$HERE/fetch-plots.sh" "$EXPERIMENT" "$PROJECT_ROOT/plots-local/$EXPERIMENT" \
      2>&1 | tee -a "$LOCAL_LOG"
    note "DONE"
    exit 0
  fi

  if (( n_done != last_completed )); then
    last_completed=$n_done
    last_progress_ts=$now
  elif (( n_done > 0 && now - last_progress_ts > stale_window )); then
    # Only count staleness once we've seen at least one E1 cell complete;
    # during the E0 phase, n_done can legitimately stay at 0 for hours.
    note "ERROR: no new completed cells in $((stale_window/60))min — sweep may be stuck"
    exit 3
  fi

  sleep "$poll_interval"
done
