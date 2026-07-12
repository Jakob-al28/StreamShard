#!/usr/bin/env bash
# StreamShard live demo: 3-node RF=3 cluster, kill the primary, watch writes survive.
# Optional bonus: add a 4th node and trigger a reshard.
#
#   ./demo.sh up        build + launch CP, gateway, 3 nodes, then print the primary PID
#   ./demo.sh load      drive steady moderate load on the demo key (Ctrl-C to stop)
#   ./demo.sh watch     live table of each node: alive? epoch, queue depth, WAL entries
#   ./demo.sh primary   print which node is primary for the demo key, and its PID
#   ./demo.sh kill       kill the current primary for the demo key (the money shot)
#   ./demo.sh reshard    BONUS: add node-3 and trigger a reshard via the control plane
#   ./demo.sh down       stop everything, wipe demo state
#
# How to run it:
#   terminal 1:  ./demo/demo.sh up      -> starts the cluster, prints who's primary
#   terminal 1:  ./demo/demo.sh load    -> starts sending writes
#   terminal 2:  ./demo/demo.sh watch   -> live table of all nodes
#   in terminal 3:  ./demo/demo.sh kill
#     -> kills whichever node is currently primary
#     -> watch terminal 2: writes start failing (503)
#     -> watch terminal 2: breaker column flips closed -> open (gateway stops
#        hammering the dead node) before the node's row flips to DEAD - SWIM is
#        still converging in the background
#     -> once SWIM converges: node flips to DEAD, ring re-routes, terminal 2
#        recovers (a couple seconds - tuned to be visible, not sub-second)
#   same terminal as kill:  ./demo/demo.sh reshard
#     -> spins up a 4th node and migrates a partition onto it live
#     -> watch terminal 2: node3 shows up and its epoch/wal_entries climb
#   when done, stop load (Ctrl-C in terminal 2) and watch (Ctrl-C in terminal 3),
#   then:  ./demo/demo.sh down   -> kills everything, wipes state
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/demo/.run"
BIN="$RUN/bin"
DEMO_KEY="demo-key"

CP_ADDR="127.0.0.1:6060"
GW_ADDR="127.0.0.1:7070"
NODE_ADDRS=("127.0.0.1:8081" "127.0.0.1:8082" "127.0.0.1:8083")
NODE3_ADDR="127.0.0.1:8084"   # bonus reshard node
PEERS="$(IFS=,; echo "${NODE_ADDRS[*]}")"

GW_SWIM="127.0.0.1:9070"
NODE_SWIM=("127.0.0.1:9081" "127.0.0.1:9082" "127.0.0.1:9083")
NODE3_SWIM="127.0.0.1:9084"
SWIM_SEEDS="$(IFS=,; echo "${NODE_SWIM[*]}")"


# Slowed enough (vs. the earlier 250ms/50ms/400ms) that the circuit breaker visibly
# trips OPEN before SWIM converges and the ring drops the dead node - the demo shows
# the layering (breaker stops the gateway hammering a dead node; SWIM is what
# actually restores availability) instead of a single instant jump. Still much
# faster than the 2s/1s/10s production defaults, so it stays legible in a live demo.
export SWIM_PING_INTERVAL="500ms"
export SWIM_PING_TIMEOUT="200ms"
export SWIM_SUSPECT_TIMEOUT="1500ms"
BREAKER_THRESHOLD=3
BREAKER_COOLDOWN="30s"

build() {
  mkdir -p "$BIN"
  echo "building..."
  go build -o "$BIN/controlplane" "$ROOT/cmd/controlplane"
  go build -o "$BIN/gateway"      "$ROOT/cmd/gateway"
  go build -o "$BIN/node"         "$ROOT/cmd/node"
}

start_node() {  
  local idx="$1" addr="$2" swim="$3"
  local dir="$RUN/node$idx"
  mkdir -p "$dir"
  # Seed off every original node PLUS the gateway itself. Node3 (added later, via
  # reshard) isn't in NODE_SWIM, so without the gateway as a direct seed it only
  # reaches the gateway's membership table transitively, several gossip rounds
  # later - until then the gateway has never heard of it, and the watch table's
  # "unknown -> dead" fallback shows it as dead even though it's actually alive.
  local seeds="$GW_SWIM"
  for s in "${NODE_SWIM[@]}"; do [ "$s" != "$swim" ] && seeds="${seeds:+$seeds,}$s"; done
  "$BIN/node" \
    --addr "$addr" --data-dir "$dir" \
    --peers "$PEERS" --rf 3 --w 2 \
    --swim-addr "$swim" --swim-seeds "$seeds" --swim-http-addr "$addr" \
    --no-idempotent --primary-replication \
    >"$dir/log" 2>&1 &
  echo $! >"$dir/pid"
}

up() {
  down >/dev/null 2>&1 || true
  build
  mkdir -p "$RUN"

  "$BIN/controlplane" --addr "$CP_ADDR" --data-dir "$RUN/cp" >"$RUN/cp.log" 2>&1 &
  echo $! >"$RUN/cp.pid"
  sleep 0.3

  for i in 0 1 2; do start_node "$i" "${NODE_ADDRS[$i]}" "${NODE_SWIM[$i]}"; done
  sleep 0.5

  "$BIN/gateway" \
    --addr "$GW_ADDR" --peers "$PEERS" --rf 3 --w 2 \
    --controlplane "$CP_ADDR" --disable-ratelimit \
    --swim-addr "$GW_SWIM" --swim-seeds "$SWIM_SEEDS" \
    --primary-replication \
    --breaker-threshold "$BREAKER_THRESHOLD" --breaker-cooldown "$BREAKER_COOLDOWN" \
    >"$RUN/gw.log" 2>&1 &
  echo $! >"$RUN/gw.pid"
  sleep 0.7

  echo
  echo "cluster up: CP=$CP_ADDR  GW=$GW_ADDR  nodes=${NODE_ADDRS[*]}  RF=3 W=2"
  primary
  echo
}

primary() {
  local owner
  owner="$(curl -s "http://$GW_ADDR/ring?key=$DEMO_KEY" | sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')"
  if [ -z "$owner" ]; then echo "could not read ring (is the gateway up?)"; return 1; fi
  local idx=-1
  for i in 0 1 2; do [ "${NODE_ADDRS[$i]}" = "$owner" ] && idx=$i; done
  local pidfile="$RUN/node$idx/pid"
  local pid="?"; [ -f "$pidfile" ] && pid="$(cat "$pidfile")"
  echo "primary for '$DEMO_KEY' = $owner  (node$idx, pid $pid)"
}

load() {
  echo "driving load on '$DEMO_KEY' (Ctrl-C to stop)..."
  local n=0 ok=0
  trap 'echo; pct=$(awk "BEGIN { if ($n > 0) printf \"%.1f\", ($ok/$n)*100; else print 0 }"); echo "stopped: $ok/$n committed ($pct%)"; exit 0' INT
  while true; do
    n=$((n+1))
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' \
      -X POST "http://$GW_ADDR/events" \
      -H 'Content-Type: application/json' \
      --max-time 4 \
      -d "{\"id\":\"$n-$RANDOM\",\"key\":\"$DEMO_KEY\",\"value\":{\"ts\":0}}" || echo 000)"
    [ "$code" = "200" ] || [ "$code" = "201" ] && ok=$((ok+1))
    printf '\rsent %5d  committed %5d  last=%s   ' "$n" "$ok" "$code"
    sleep 0.05
  done
}

wal_count() {  # addr -> number of committed WAL entries
  curl -s --max-time 1 "http://$1/log?from=0" 2>/dev/null \
    | grep -o '"Offset"' | wc -l | tr -d ' '
}

breaker_state_for() {  # addr -> breaker state as seen by the gateway, or empty.
  # Once SWIM removes a dead node from the ring it also drops out of the gateway's
  # /health array entirely - that's a normal, expected outcome here, so this must
  # never propagate a non-zero exit under `set -e`, or it silently kills watch_loop.
  local addr="$1"
  local line
  line="$(echo "$GW_HEALTH_CACHE" | tr '{' '\n' | grep "\"addr\":\"$addr\"" || true)"
  [ -z "$line" ] && return 0
  echo "$line" | sed -n 's/.*"breaker":"\([^"]*\)".*/\1/p' | head -1
  return 0
}

swim_state_for() {  # addr -> SWIM's own alive/suspect view (short label), or empty
  # Must split on every key boundary (comma), not just '{' - the whole "states"
  # object is one line otherwise, so every address's grep matches the same line
  # and picks up whichever node's state happens to contain "suspect" first.
  local addr="$1"
  local line
  line="$(echo "$GW_SWIM_CACHE" | tr ',' '\n' | grep "\"$addr\":" || true)"
  [ -z "$line" ] && return 0
  case "$line" in
    *suspect*) echo "susp" ;;
    *alive*)   echo "alive" ;;
  esac
  return 0
}

watch_once() {
  # One gateway /health poll (breaker state) and one /swim poll (SWIM's own
  # alive/suspect view) per refresh, reused below instead of re-fetched per node.
  # `|| true` throughout so a curl hiccup or grep no-match never kills watch_loop
  # under `set -e`.
  GW_HEALTH_CACHE="$(curl -s --max-time 1 "http://$GW_ADDR/health" 2>/dev/null || true)"
  GW_SWIM_CACHE="$(curl -s --max-time 1 "http://$GW_ADDR/swim" 2>/dev/null || true)"

  printf '%-22s %-6s %-6s %-8s %s\n' "node" "swim" "epoch" "breaker" "wal_entries"
  local addrs=("${NODE_ADDRS[@]}")
  # node3 only exists after ./demo.sh reshard; show it once it's reachable.
  if [ -f "$RUN/node3/pid" ]; then addrs+=("$NODE3_ADDR"); fi
  for idx in "${!addrs[@]}"; do
    local addr="${addrs[$idx]}"
    local h brk swim
    h="$(curl -s --max-time 1 "http://$addr/health" 2>/dev/null || true)"
    brk="$(breaker_state_for "$addr" || true)"
    swim="$(swim_state_for "$addr" || true)"
    # Blank swim state is ambiguous: either SWIM fenced it out (dead) or the
    # gateway just hasn't gossiped-discovered it yet (e.g. node3, seconds after
    # ./demo.sh reshard). Disambiguate using the node's own /health instead of
    # defaulting to "dead", which would mislabel a node that just joined.
    if [ -z "$swim" ]; then
      if [ -n "$h" ]; then swim="new"; else swim="dead"; fi
    fi
    [ -z "$brk" ] && brk="-"
    if [ -z "$h" ]; then
      printf '%-22s %-6s %-6s %-8s %s\n' "node$idx $addr" "$swim" "-" "$brk" "-"
    else
      local ep wal
      ep="$(echo "$h"  | sed -n 's/.*"epoch":\([0-9]*\).*/\1/p')"
      wal="$(wal_count "$addr")"
      printf '%-22s %-6s %-6s %-8s %s\n' "node$idx $addr" "$swim" "$ep" "$brk" "$wal"
    fi
  done
}

watch_loop() {
  while true; do
    clear
    echo "StreamShard nodes   $(date +%T)"
    echo "------------------------------------------------------------"
    watch_once
    sleep 1
  done
}

kill_primary() {
  local owner idx
  owner="$(curl -s "http://$GW_ADDR/ring?key=$DEMO_KEY" | sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')"
  idx=-1
  for i in 0 1 2; do [ "${NODE_ADDRS[$i]}" = "$owner" ] && idx=$i; done
  if [ "$idx" -lt 0 ]; then echo "no live primary found for '$DEMO_KEY'"; return 1; fi
  local pid; pid="$(cat "$RUN/node$idx/pid")"
  echo ">>> killing primary node$idx ($owner) pid $pid"
  kill -9 "$pid" 2>/dev/null || true
}

reshard() {
  local source
  source="$(curl -s "http://$GW_ADDR/ring?key=$DEMO_KEY" | sed -n 's/.*"owner":"\([^"]*\)".*/\1/p')"
  if [ -z "$source" ]; then echo "no live primary to migrate from"; return 1; fi
  echo ">>> starting node-3 ($NODE3_ADDR) as migration target"
  start_node 3 "$NODE3_ADDR" "$NODE3_SWIM"
  sleep 0.7
  echo ">>> live-migrating partition  $source  ->  $NODE3_ADDR "
  curl -s -X POST "http://$CP_ADDR/reshard" \
    -H 'Content-Type: application/json' \
    -d "{\"source\":\"$source\",\"target\":\"$NODE3_ADDR\",\"live\":true}" \
    && echo
}

down() {
  for f in "$RUN"/gw.pid "$RUN"/cp.pid "$RUN"/node*/pid; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null || true
  done
  rm -rf "$RUN"
  echo "cluster down, state wiped."
}

cmd="${1:-}"
case "$cmd" in
  up)       up ;;
  load)     load ;;
  watch)    watch_loop ;;
  primary)  primary ;;
  kill)     kill_primary ;;
  reshard)  reshard ;;
  down)     down ;;
  *) echo "usage: $0 {up|load|watch|primary|kill|reshard|down}"; exit 1 ;;
esac
