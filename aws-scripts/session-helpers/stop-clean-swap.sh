#!/usr/bin/env bash
# Stop the un-optimized sweep, force-clean all E13ack infra, swap in the
# OPTIMIZED Design X binary, and relaunch the both-arms sweep. Robust
# exit-code handling (pgrep -c returns 1 at zero -> guard with || true).
set -uo pipefail
ORCH=18.144.2.232
KEY=~/.ssh/metronome-aws.pem
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=12 ubuntu@$ORCH"
R="aws ec2 --region us-west-1"

echo "===== phase 1: stop the python orchestrator (no new cells) ====="
$SSH 'pkill -TERM -f "[E]13-intra-ack.py" && echo "SIGTERM sent" || echo "(no python sweep running)"; sleep 2; echo "python alive: $(pgrep -fc "[E]13-intra-ack.py" || true)"'

echo "===== phase 2: brief grace for in-flight run-cell.sh self-teardown, then force-kill ====="
for i in $(seq 1 4); do
  rc=$($SSH 'pgrep -fc "[r]un-cell.sh" || true' 2>/dev/null)
  echo "[grace $((i*30))s] run-cell.sh running: ${rc:-?}"
  [ "${rc:-1}" = "0" ] && break
  sleep 30
done
$SSH '
for p in $(pgrep -f "[r]un-cell.sh"); do kill -9 "$p" 2>/dev/null; done
for p in $(pgrep -f "[t]erraform"); do kill -9 "$p" 2>/dev/null; done
rm -f ~/metronome-eval/cell-states/.*.lock.info 2>/dev/null
echo "after force-kill: run-cell=$(pgrep -fc "[r]un-cell.sh" || true) terraform=$(pgrep -fc "[t]erraform" || true)"'

echo "===== phase 3: terminate all E13ack instances + delete orphan volumes ====="
IDS=$($R describe-instances --filters Name=instance-state-name,Values=running,pending "Name=tag:Name,Values=E13ack-*" --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$IDS" ]; then echo "terminating $(echo $IDS | wc -w) instances"; $R terminate-instances --instance-ids $IDS >/dev/null 2>&1; fi
for i in $(seq 1 24); do
  n=$($R describe-instances --filters Name=instance-state-name,Values=running,pending,shutting-down "Name=tag:Name,Values=E13ack-*" --query 'length(Reservations[].Instances[])' --output text 2>/dev/null)
  echo "[term $((i*15))s] non-terminated E13ack: ${n:-?}"
  [ "${n:-1}" = "0" ] && break
  sleep 15
done
VOLS=$($R describe-volumes --filters Name=status,Values=available "Name=tag:Name,Values=E13ack-*" --query 'Volumes[].VolumeId' --output text)
nv=0; for v in $VOLS; do $R delete-volume --volume-id "$v" >/dev/null 2>&1 && nv=$((nv+1)); done
echo "deleted $nv orphan volumes"

echo "===== phase 4: clear state / cache / progress ====="
$SSH '
cd ~/metronome-eval
rm -f cell-states/E13ack-*.tfstate* cell-states/.E13ack-*
rm -rf results/E13ack-*
mv E13-ack-progress.jsonl E13-ack-progress.jsonl.unopt-$(date -u +%H%M%SZ) 2>/dev/null || true
echo "E13ack cell-states: $(ls cell-states/ 2>/dev/null | grep -c E13ack)  results: $(ls results/ 2>/dev/null | grep -c E13ack)  disk free: $(df -h / | tail -1 | awk "{print \$4}")"'

echo "===== phase 5: swap in the OPTIMIZED metronome binary ====="
$SSH '
sudo cp /opt/metronome-eval/bin/etcd-metronome.designx-opt-489c5d901 /opt/metronome-eval/bin/etcd-metronome
sudo chmod +x /opt/metronome-eval/bin/etcd-metronome
echo "active etcd-metronome:  $(md5sum /opt/metronome-eval/bin/etcd-metronome | cut -c1-12)  (optimized = ef7dcfbad841)"
echo "vanilla (unchanged):    $(md5sum /opt/metronome-eval/bin/etcd-vanilla | cut -c1-12)  (stock = bb5289e9d5bc)"'

echo "===== phase 6: relaunch BOTH-ARMS sweep (stock vanilla + OPTIMIZED Design X) ====="
$SSH '
cd ~/metronome-eval
nohup python3 experiments/E13-intra-ack.py --replace --workers 4 --target-runs 3 --max-runs 4 > E13-designx-opt-both.log 2>&1 &
echo "launch PID: $!"
sleep 10
echo "--- manifest + first cells ---"
head -16 E13-designx-opt-both.log'
echo "===== DONE ====="
