#!/bin/bash
set -uo pipefail

# Polls CPU on all StreamShard nodes + gateways every INTERVAL seconds and appends
# to OUT. Run it in the background while a sweep runs:
#   ./bench/poll_cpu.sh > /dev/null 2>&1 &
# then `tail -f bench/results/cpu_poll.csv`. Ctrl-C / kill to stop.

ZONE="${ZONE:-europe-west3-a}"
PROJECT="${PROJECT:-se-streamshard}"
NODES="${NODES:-5}"
GATEWAYS="${GATEWAYS:-5}"
INTERVAL="${INTERVAL:-5}"
OUT="${OUT:-bench/results/cpu_poll.csv}"

mkdir -p "$(dirname "$OUT")"
echo "ts,role,idx,cpu_pct,cpu_us,cpu_sy,cpu_id" > "$OUT"

sample() {
  local role="$1" name="$2" idx="$3"
  # %CPU of the streamshard process, plus the %Cpu(s) line (us/sy/id), one ssh round-trip.
  # The process %CPU comes from `ps`.
  local raw
  raw="$(gcloud compute ssh "$name" --zone "$ZONE" --project "$PROJECT" \
    --command "top -bn1 | grep '%Cpu'; ps -C $role -o %cpu= | head -1" 2>/dev/null)"
  local proc cpuline
  cpuline="$(echo "$raw" | grep '%Cpu')"
  proc="$(echo "$raw" | grep -v '%Cpu' | tr -d ' ' | head -1)"
  local us sy id
  us="$(echo "$cpuline" | sed -n 's/.*: *\([0-9.]*\) us.*/\1/p')"
  sy="$(echo "$cpuline" | sed -n 's/.* \([0-9.]*\) sy.*/\1/p')"
  id="$(echo "$cpuline" | sed -n 's/.* \([0-9.]*\) id.*/\1/p')"
  echo "$(date +%s),$role,$idx,${proc:-NA},${us:-NA},${sy:-NA},${id:-NA}" >> "$OUT"
}

echo "polling every ${INTERVAL}s -> $OUT (Ctrl-C to stop)"
while true; do
  for i in $(seq 0 $((NODES-1)));    do sample node    "streamshard-node-$i"    "$i" & done
  for i in $(seq 0 $((GATEWAYS-1)));  do sample gateway "streamshard-gateway-$i" "$i" & done
  wait
  sleep "$INTERVAL"
done
