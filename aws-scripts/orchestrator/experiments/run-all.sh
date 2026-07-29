#!/usr/bin/env bash
# Drives the full pipeline: E0 (concurrency calibration) → E1 (full K sweep).
# Each phase aborts the script on failure so we can intervene.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$HERE/.." && pwd)

echo "==== running E0 (concurrency calibration) ===="
"$HERE/E0-concurrency-calibration.sh"

echo
echo "==== running E1 (full K sweep, using E0's per-(N,val) peaks) ===="
"$HERE/E1-failure-free-perf.sh"

echo
echo "==== run-all complete ===="
