#!/usr/bin/env bash
# Poll until the both-arms sweep finishes, then dump a final summary.
set -uo pipefail
ORCH=18.144.2.232
KEY=~/.ssh/metronome-aws.pem
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=12 ubuntu@$ORCH"

for i in $(seq 1 84); do
  alive=$($SSH 'if pgrep -f "[E]13-intra-ack.py" >/dev/null 2>&1; then echo Y; else echo N; fi' 2>/dev/null)
  alive=${alive:-?}
  stats=$($SSH '
    ok=$(grep -cE "\"status\": ?\"(ok|high_variance)\"" ~/metronome-eval/E13-ack-progress.jsonl 2>/dev/null || true)
    bad=$(grep -cE "\"status\": ?\"(failed|crashed)\"" ~/metronome-eval/E13-ack-progress.jsonl 2>/dev/null || true)
    rows=$(tail -n +2 ~/metronome-eval/data/E13-ack-per-cell-parsed.csv 2>/dev/null | wc -l)
    free=$(df -h / | tail -1 | awk "{print \$4}")
    echo "ok=$ok bad=$bad csvrows=$rows disk=$free"' 2>/dev/null)
  echo "[$(date -u +%H:%M:%SZ)] alive=$alive $stats"
  if [ "$alive" = "N" ]; then
    echo "===== SWEEP FINISHED ====="
    $SSH '
      echo "--- run-log tail ---"; tail -n 6 ~/metronome-eval/E13-designx-opt-both.log
      echo "--- cells by mode (parsed CSV) ---"
      tail -n +2 ~/metronome-eval/data/E13-ack-per-cell-parsed.csv | awk -F, "{print \$6}" | sort | uniq -c
      echo "--- residual instances (should be ~0) ---"' 2>&1
    aws ec2 describe-instances --region us-west-1 --filters Name=instance-state-name,Values=running,pending "Name=tag:Name,Values=E13ack-*" --query 'length(Reservations[].Instances[])' --output text 2>/dev/null
    break
  fi
  sleep 300
done
