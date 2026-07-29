#!/usr/bin/env bash
# Fetch ONLY the plot PNGs (not raw artifacts) from the orchestrator's
# results/plots/ tree to a local dir. Use this when you want to look at
# results on your laptop without pulling tens of MB of raw logs.
#
# Defaults to fetching results/plots/E1/* into ./plots-local/E1/.
#
# Usage:
#   ./scripts/fetch-plots.sh                 # E1 plots → ./plots-local/E1/
#   ./scripts/fetch-plots.sh E2 ./somewhere  # E2 plots → ./somewhere/
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)
source "$PROJECT_ROOT/infra.env"

EXPERIMENT="${1:-E1}"
LOCAL_DIR="${2:-$PROJECT_ROOT/plots-local/$EXPERIMENT}"
mkdir -p "$LOCAL_DIR"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o LogLevel=ERROR
          -i ~/.ssh/${KEY_NAME}.pem)

# Use rsync if present (much faster + skips unchanged); fall back to scp.
if command -v rsync >/dev/null 2>&1; then
  rsync -az -e "ssh ${SSH_OPTS[*]}" \
        "ubuntu@$ORCHESTRATOR_IP:/home/ubuntu/metronome-eval/results/plots/${EXPERIMENT}/" \
        "$LOCAL_DIR/"
else
  scp ${SSH_OPTS[*]} -r \
      "ubuntu@$ORCHESTRATOR_IP:/home/ubuntu/metronome-eval/results/plots/${EXPERIMENT}/*" \
      "$LOCAL_DIR/"
fi

echo "fetched into $LOCAL_DIR/"
ls -1 "$LOCAL_DIR" | head -20
