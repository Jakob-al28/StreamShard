#!/usr/bin/env bash
# Resilience demo: runs the three overload/failure mechanisms against a local cluster.
#   1. token bucket    (gateway): hammer one key -> 429 rate limited
#   2. load shedding   (node):    flood the apply queue -> 429 overloaded
#   3. circuit breaker (gateway): kill a node -> breaker opens
#
#   ./demo/resilience.sh        run all three phases and tear down
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/demo/.run-resilience"
BIN="$RUN/bin"

GW_ADDR="127.0.0.1:7071"
NODE_ADDRS=("127.0.0.1:8091" "127.0.0.1:8092" "127.0.0.1:8093")
PEERS="$(IFS=,; echo "${NODE_ADDRS[*]}")"

cleanup() {
  for f in "$RUN"/gw.pid "$RUN"/node*/pid; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null || true
  done
  rm -rf "$RUN"
}
trap cleanup EXIT

# A leftover process from a previous unclean exit can still hold these ports,
# which makes the fresh node below fail to bind while an orphan keeps serving
# in the background - kill node2 then looks like a no-op. Fail loudly instead.
for addr in "${NODE_ADDRS[@]}" "$GW_ADDR"; do
  port="${addr##*:}"
  if lsof -ti tcp:"$port" >/dev/null 2>&1; then
    echo "port $port is already in use (stale run?). Kill it and retry:" >&2
    echo "  lsof -ti tcp:$port | xargs kill -9" >&2
    exit 1
  fi
done

echo "building..."
mkdir -p "$BIN"
go build -o "$BIN/gateway" "$ROOT/cmd/gateway"
go build -o "$BIN/node"    "$ROOT/cmd/node"

# Tiny apply queue so shedding is visible locally; long breaker cooldown so the
# open state is still visible when we check it.
for i in 0 1 2; do
  dir="$RUN/node$i"
  mkdir -p "$dir"
  "$BIN/node" --addr "${NODE_ADDRS[$i]}" --data-dir "$dir" \
    --peers "$PEERS" --rf 3 --w 2 --queue-cap 1 \
    >"$dir/log" 2>&1 &
  echo $! >"$dir/pid"
  disown
done
sleep 0.5

for i in 0 1 2; do
  pid="$(cat "$RUN/node$i/pid")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "node$i exited immediately, see $RUN/node$i/log:" >&2
    cat "$RUN/node$i/log" >&2
    exit 1
  fi
done

"$BIN/gateway" --addr "$GW_ADDR" --peers "$PEERS" --rf 3 --w 2 \
  --rate 5 --burst 3 --breaker-threshold 5 --breaker-cooldown 60s \
  >"$RUN/gw.log" 2>&1 &
echo $! >"$RUN/gw.pid"
disown
sleep 0.7

if ! kill -0 "$(cat "$RUN/gw.pid")" 2>/dev/null; then
  echo "gateway exited immediately, see $RUN/gw.log:" >&2
  cat "$RUN/gw.log" >&2
  exit 1
fi

post() {  # url id key -> body appended with |code
  curl -s -w '|%{http_code}' --max-time 4 -X POST "$1" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$2\",\"key\":\"$3\",\"value\":{}}"
}

echo
echo "=== 1. token bucket (gateway): 30 fast writes to one key, bucket rate=5/s burst=3 ==="
limited=0
for n in $(seq 1 30); do
  out="$(post "http://$GW_ADDR/events" "b$n" "hot-key")"
  case "$out" in *"rate limited"*) limited=$((limited+1)) ;; esac
done
echo "rate limited: $limited/30 rejected with 429"

echo
echo "=== 2. load shedding (node): 200 parallel writes at node0, queue-cap=1 ==="
shed="$(seq 1 200 | xargs -P 200 -I{} curl -s -o /dev/null -w '%{http_code}\n' --max-time 4 \
  -X POST "http://${NODE_ADDRS[0]}/events" \
  -H 'Content-Type: application/json' \
  -d '{"id":"s{}","key":"k{}","value":{}}' | grep -c '^429$' || true)"
echo "$shed/200 writes rejected with 429 because the queue was full."

echo
echo "=== 3. circuit breaker (gateway): kill node2, write until the breaker opens ==="
pid="$(cat "$RUN/node2/pid")"
kill -9 "$pid"
for t in $(seq 1 20); do
  kill -0 "$pid" 2>/dev/null || break
  sleep 0.1
done
if kill -0 "$pid" 2>/dev/null; then
  echo "node2 (pid $pid) did not die" >&2
  exit 1
fi

for n in $(seq 1 20); do post "http://$GW_ADDR/events" "c$n" "key-$n" >/dev/null || true; done
open=""
for t in $(seq 1 20); do
  if curl -s "http://$GW_ADDR/health" | grep -q '"breaker":"open"'; then open=yes; break; fi
  sleep 0.5
done
if [ "$open" = "yes" ]; then
  echo "Circuit Breaker detected the dead node and tripped to OPEN."
  echo "Gateway /health endpoint reports:"
  curl -s "http://$GW_ADDR/health" | jq -c '.[] | select(.breaker == "open")'
else
  echo "breaker did not open within 10s (check $RUN/gw.log)"
  exit 1
fi

echo
echo "tearing down."
