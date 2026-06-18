#!/bin/bash
set -euo pipefail

# Full scalability matrix: for each (rf, replication-mode, machine) cell, deploy 1/3/5
# node clusters, run the load sweep against each, and collect results. Gateways scale
# 1:1 with nodes (1 gw + 1 node, 3 gw + 3 node, 5 gw + 5 node).
#
# Methodology:
#   * --no-idempotent on every node (set in deploy/terraform/startup/node.sh)
#     Every committed write is a real durable append. Gateway fan-out runs have no
#     suffix; primary-replication runs are _pr.
#   * enable_swim=false: SWIM gossip is off; replication uses the static --peers ring.
#     SWIM was not converging on the VMs (false-dead -> quorum could
#     never be met -> 100% 503s). It is not a perf one (SWIM is
#     ~5 UDP pkt/s, <0.001% of load), so disabling it does not change throughput.
#   * 12 k6 workers. 4 workers under-drove the high-ceiling configs (RF=1, gateway
#     fan-out) and reported peaks below saturation.
#   * Per-config offered-load cap (MAX_RPS): each config saturates at a different load,
#     so a single 100k cap wastes most of the sweep. RF=1 has
#     the highest ceiling (5-node ~44k), the replicated RF=3 paths saturate much lower.
#   * disable_ratelimit=true for clean throughput measurement.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/../deploy/terraform"

PROJECT="se-streamshard"
ZONE="europe-west3-a"
REGION="europe-west3"
WORKERS=12

# Each matrix cell: "rf_label machine_type instance_label primary_replication label_suffix max_rps"
#   instance_label: e2 = e2-standard-2, e2big = e2-standard-4 (vertical-scaling comparison)
#   primary_replication: false = gateway fans out replicas, true = primary node fans out
#   label_suffix: - = none (gateway fan-out, the default path), _pr = primary-replication
#   max_rps: per-config offered-load ceiling
MATRIX=(
  "rf1 e2-standard-2 e2     false -    80000"
  "rf3 e2-standard-2 e2     false -    40000"
  "rf3 e2-standard-2 e2     true  _pr  40000"
  "rf3 e2-standard-4 e2big  false -    60000"
  "rf3 e2-standard-4 e2big  true  _pr  60000"
)

NODE_COUNTS=(1 3 5)

release_stale_addresses() {
  # terraform destroy occasionally leaves reserved internal addresses behind,
  # which then collide on the next apply. Release any that are no longer attached.
  local stale
  stale="$(gcloud compute addresses list --project "$PROJECT" --regions "$REGION" \
    --filter="status=RESERVED AND name~streamshard-node" \
    --format="value(name)" 2>/dev/null || true)"
  for a in $stale; do
    echo "==> releasing stale address $a"
    gcloud compute addresses delete "$a" --region "$REGION" --project "$PROJECT" --quiet || true
  done
}

deploy() {
  local nodes="$1" rf_num="$2" machine="$3" primary_repl="$4"
  release_stale_addresses
  echo "==> terraform: nodes=$nodes gateways=$nodes rf=$rf_num machine=$machine primary_replication=$primary_repl swim=off no-idempotent=on"
  terraform -chdir="$TF_DIR" apply -auto-approve \
    -var="node_count=$nodes" \
    -var="gateway_count=$nodes" \
    -var="machine_type=$machine" \
    -var="gateway_machine_type=$machine" \
    -var="rf=$rf_num" \
    -var="primary_replication=$primary_repl" \
    -var="enable_swim=false" \
    -var="disable_ratelimit=true"
}

wait_healthy() {
  local lb="$1"
  echo "==> waiting for gateway+nodes to come up at $lb ..."
  for _ in $(seq 1 60); do
    if curl -sf --max-time 5 "http://$lb:7070/health" | grep -q '"breaker"'; then
      echo "==> healthy"
      return 0
    fi
    sleep 10
  done
  echo "ERROR: cluster did not become healthy in time" >&2
  return 1
}

verify_write() {
  echo "==> verifying a single durable write commits ..."
  local code
  code="$(gcloud compute ssh streamshard-node-0 --zone "$ZONE" --project "$PROJECT" \
    --command "curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/events \
      -H 'Content-Type: application/json' -H 'X-Epoch: 0' \
      -d '{\"id\":\"matrix-probe\",\"key\":\"matrix-probe\",\"value\":{}}'" 2>/dev/null || true)"
  if [[ "$code" == "201" || "$code" == "200" ]]; then
    echo "==> write OK ($code)"
    return 0
  fi
  echo "ERROR: probe write returned '$code' (want 201/200) — cluster not committing, skipping cell" >&2
  return 1
}

for cell in "${MATRIX[@]}"; do
  read -r rf_label machine instance primary_repl label_suffix max_rps <<<"$cell"
  rf_num=$([[ "$rf_label" == "rf3" ]] && echo 3 || echo 1)
  [[ "$label_suffix" == "-" ]] && label_suffix=""   # "-" sentinel means no suffix

  for nodes in "${NODE_COUNTS[@]}"; do
    echo
    echo "########################################################"
    echo "# CELL: nodes=$nodes  rf=$rf_label  machine=$machine  mode=$([[ "$primary_repl" == true ]] && echo primary-repl || echo gateway-fanout)  rps=$max_rps"
    echo "########################################################"

    result_file="$SCRIPT_DIR/results/${nodes}node_${instance}_${rf_label}${label_suffix}.json"
    if [[ -f "$result_file" ]]; then
      echo "==> SKIP: $result_file already exists"
      continue
    fi

    # A single cell failing (deploy quota, slow health, no quorum, etc.) must not abort the matrix
    if ! (
      terraform -chdir="$TF_DIR" destroy -auto-approve || true
      deploy "$nodes" "$rf_num" "$machine" "$primary_repl"
      lb="$(terraform -chdir="$TF_DIR" output -raw lb_ip)"
      wait_healthy "$lb"
      verify_write
      "$SCRIPT_DIR/run_benchmark.sh" "http://$lb:7070" "$nodes" \
        --k8s --workers "$WORKERS" --rps "$max_rps" \
        --rf "$rf_label" --instance "$instance" --label-suffix "$label_suffix" \
        --cluster streamshard-bench --region "$ZONE" --project "$PROJECT"
    ); then
      echo "!! CELL FAILED: nodes=$nodes rf=$rf_label machine=$machine — continuing" >&2
    fi
  done
done

echo
echo "==> matrix complete. results in $SCRIPT_DIR/results/"
ls "$SCRIPT_DIR/results/"
