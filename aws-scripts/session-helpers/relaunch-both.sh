#!/usr/bin/env bash
# Wait for the killed sweep's cells to finish self-teardown, clean any
# stragglers, clear cache/state, then relaunch the BOTH-ARMS Design X sweep.
set -uo pipefail
ORCH=18.144.2.232
KEY=~/.ssh/metronome-aws.pem
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=12 ubuntu@$ORCH"

echo "===== phase 1: wait for in-flight teardown to drain ====="
for i in $(seq 1 36); do
  rc=$($SSH 'pgrep -fc "[r]un-cell.sh"' 2>/dev/null || echo "?")
  inst=$(aws ec2 describe-instances --region us-west-1 \
    --filters Name=instance-state-name,Values=running,pending "Name=tag:Name,Values=E13ack-*" \
    --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo "?")
  echo "[$(date -u +%H:%M:%SZ)] run-cell=$rc running_instances=$inst"
  if [ "$rc" = "0" ] && [ "$inst" = "0" ]; then echo "teardown drained"; break; fi
  sleep 30
done

echo "===== phase 2: defensive cleanup of any E13ack stragglers ====="
STRAG=$(aws ec2 describe-instances --region us-west-1 \
  --filters Name=instance-state-name,Values=running,pending "Name=tag:Name,Values=E13ack-*" \
  --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
if [ -n "$STRAG" ]; then echo "terminating stragglers: $STRAG"; aws ec2 terminate-instances --region us-west-1 --instance-ids $STRAG >/dev/null 2>&1; sleep 20; fi
VOLS=$(aws ec2 describe-volumes --region us-west-1 --filters Name=status,Values=available "Name=tag:Name,Values=E13ack-*" --query 'Volumes[].VolumeId' --output text 2>/dev/null)
if [ -n "$VOLS" ]; then echo "deleting orphan vols: $(echo $VOLS|wc -w)"; for v in $VOLS; do aws ec2 delete-volume --region us-west-1 --volume-id "$v" >/dev/null 2>&1; done; fi

echo "===== phase 3: clear cache/state/progress for a fresh both-arms run ====="
$SSH '
cd ~/metronome-eval
rm -rf results/E13ack-*
rm -f cell-states/E13ack-*.tfstate* cell-states/.E13ack-*
mv E13-ack-progress.jsonl E13-ack-progress.jsonl.killedrun-$(date -u +%Y%m%dT%H%M%SZ) 2>/dev/null || true
echo "E13ack results: $(ls results/ 2>/dev/null | grep -c E13ack)  cell-states: $(ls cell-states/ 2>/dev/null | grep -c E13ack)  disk: $(df -h / | tail -1 | awk "{print \$4}") free"
echo "binaries: vanilla=$(md5sum /opt/metronome-eval/bin/etcd-vanilla | cut -c1-12) metronome=$(md5sum /opt/metronome-eval/bin/etcd-metronome | cut -c1-12)"
'

echo "===== phase 4: relaunch BOTH-ARMS sweep (vanilla + Design X metronome) ====="
$SSH '
cd ~/metronome-eval
nohup python3 experiments/E13-intra-ack.py --replace --workers 4 --target-runs 3 --max-runs 4 > E13-designx-both.log 2>&1 &
echo "launch PID: $!"
sleep 10
echo "--- first log lines ---"
head -16 E13-designx-both.log
'
echo "===== relaunch sequence complete ====="
