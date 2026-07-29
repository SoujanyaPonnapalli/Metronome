#!/usr/bin/env bash
# Live progress view for an in-flight experiment sweep.
# Run from the laptop; ssh-tails the sweep log on the orchestrator and
# periodically prints a one-line digest of cells completed so far.
#
# Usage:  ./scripts/watch-sweep.sh [E1]   # defaults to E1
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"
EXPERIMENT="${1:-E1}"
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o LogLevel=ERROR
          -i ~/.ssh/${KEY_NAME}.pem)

ssh "${SSH_OPTS[@]}" "ubuntu@$ORCHESTRATOR_IP" bash -s <<EOF
set -e
cd ~/metronome-eval
LOG=\$(ls -t ~/${EXPERIMENT}-sweep.log 2>/dev/null | head -1)
[[ -z "\$LOG" ]] && { echo "no sweep log yet"; exit 0; }
echo "tailing \$LOG (Ctrl-C to detach)"
echo "----"
ls results/${EXPERIMENT}-*/results.json 2>/dev/null | wc -l | xargs -I{} echo "completed cells: {}"
echo "current matplotlib plots:"
ls results/plots/${EXPERIMENT}/ 2>/dev/null | sed 's|^|  |'
echo "----"
tail -F "\$LOG"
EOF
