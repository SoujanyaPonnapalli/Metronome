#!/usr/bin/env bash
# Wait for the orphaned instances to terminate, delete the volumes they orphan,
# then relaunch the BOTH-ARMS Design X sweep. Clean exit-code handling.
set -uo pipefail
ORCH=18.144.2.232
KEY=~/.ssh/metronome-aws.pem
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=12 ubuntu@$ORCH"
R="aws ec2 --region us-west-1"

echo "===== phase 1: wait for orphaned E13ack instances to fully terminate ====="
for i in $(seq 1 24); do
  inst=$($R describe-instances --filters Name=instance-state-name,Values=running,pending,shutting-down,stopping "Name=tag:Name,Values=E13ack-*" --query 'length(Reservations[].Instances[])' --output text 2>/dev/null)
  inst=${inst:-?}
  echo "[$(date -u +%H:%M:%SZ)] non-terminated E13ack instances: $inst"
  [ "$inst" = "0" ] && { echo "all terminated"; break; }
  sleep 20
done

echo "===== phase 2: delete orphaned (available) E13ack data volumes ====="
VOLS=$($R describe-volumes --filters Name=status,Values=available "Name=tag:Name,Values=E13ack-*" --query 'Volumes[].VolumeId' --output text 2>/dev/null)
n=0; for v in $VOLS; do $R delete-volume --volume-id "$v" >/dev/null 2>&1 && n=$((n+1)); done
echo "deleted $n orphan volumes"

echo "===== phase 3: sanity-check binaries differ (vanilla stock vs metronome DesignX) ====="
$SSH 'echo "vanilla  : $(md5sum /opt/metronome-eval/bin/etcd-vanilla | cut -c1-12)"; echo "metronome: $(md5sum /opt/metronome-eval/bin/etcd-metronome | cut -c1-12)"'

echo "===== phase 4: relaunch BOTH-ARMS sweep (stock vanilla + Design X metronome) ====="
$SSH '
cd ~/metronome-eval
nohup python3 experiments/E13-intra-ack.py --replace --workers 4 --target-runs 3 --max-runs 4 > E13-designx-both.log 2>&1 &
echo "launch PID: $!"
sleep 10
echo "--- manifest + first cells ---"
head -16 E13-designx-both.log
'
echo "===== relaunch complete ====="
