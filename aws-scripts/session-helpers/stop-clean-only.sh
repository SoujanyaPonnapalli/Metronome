#!/usr/bin/env bash
# Stop the running sweep + force-clean all E13ack infra. NO relaunch.
set -uo pipefail
ORCH=18.144.2.232; KEY=~/.ssh/metronome-aws.pem
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=12 ubuntu@$ORCH"
R="aws ec2 --region us-west-1"

echo "===== stop python orchestrator ====="
$SSH 'pkill -TERM -f "[E]13-intra-ack.py" && echo "SIGTERM sent" || echo "(none running)"; sleep 2; echo "python alive: $(pgrep -fc "[E]13-intra-ack.py" || true)"'

echo "===== grace for self-teardown, then force-kill ====="
for i in $(seq 1 4); do
  rc=$($SSH 'pgrep -fc "[r]un-cell.sh" || true' 2>/dev/null)
  echo "[grace $((i*30))s] run-cell.sh: ${rc:-?}"; [ "${rc:-1}" = "0" ] && break; sleep 30
done
$SSH 'for p in $(pgrep -f "[r]un-cell.sh"); do kill -9 "$p" 2>/dev/null; done
for p in $(pgrep -f "[t]erraform"); do kill -9 "$p" 2>/dev/null; done
rm -f ~/metronome-eval/cell-states/.*.lock.info 2>/dev/null
echo "after kill: run-cell=$(pgrep -fc "[r]un-cell.sh" || true) terraform=$(pgrep -fc "[t]erraform" || true)"'

echo "===== terminate E13ack instances + delete orphan volumes ====="
IDS=$($R describe-instances --filters Name=instance-state-name,Values=running,pending "Name=tag:Name,Values=E13ack-*" --query 'Reservations[].Instances[].InstanceId' --output text)
[ -n "$IDS" ] && { echo "terminating $(echo $IDS|wc -w)"; $R terminate-instances --instance-ids $IDS >/dev/null 2>&1; }
for i in $(seq 1 24); do
  n=$($R describe-instances --filters Name=instance-state-name,Values=running,pending,shutting-down "Name=tag:Name,Values=E13ack-*" --query 'length(Reservations[].Instances[])' --output text 2>/dev/null)
  echo "[term $((i*15))s] non-terminated: ${n:-?}"; [ "${n:-1}" = "0" ] && break; sleep 15
done
VOLS=$($R describe-volumes --filters Name=status,Values=available "Name=tag:Name,Values=E13ack-*" --query 'Volumes[].VolumeId' --output text)
nv=0; for v in $VOLS; do $R delete-volume --volume-id "$v" >/dev/null 2>&1 && nv=$((nv+1)); done
echo "deleted $nv orphan volumes"

echo "===== clear state/cache/progress ====="
$SSH 'cd ~/metronome-eval
rm -f cell-states/E13ack-*.tfstate* cell-states/.E13ack-*
rm -rf results/E13ack-*
mv E13-ack-progress.jsonl E13-ack-progress.jsonl.optrun1-$(date -u +%H%M%SZ) 2>/dev/null || true
echo "E13ack cell-states=$(ls cell-states/|grep -c E13ack) results=$(ls results/|grep -c E13ack) disk=$(df -h /|tail -1|awk "{print \$4}")"'
echo "===== STOPPED + CLEANED (no relaunch) ====="
