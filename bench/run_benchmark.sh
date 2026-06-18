#!/bin/bash
set -euo pipefail

usage() {
  cat <<EOF
usage: $0 <base_url> <node_count> [options]

  base_url    gateway URL, e.g. http://34.1.2.3:7070
  node_count  1, 3, or 5

options:
  --rps N           TOTAL offered load at the top of the ramp (default: 100000).
                    The sweep staircases 0 -> N and records achieved throughput per
                    step, so the peak (rise-then-collapse) is captured automatically.
                    Set N comfortably above expected capacity so the collapse is visible.
                    The operator splits N across --workers pods via execution segments.
  --instance TYPE   e2 or c2  (default: e2)
  --rf LABEL        result file label: rf1 or rf3 (default: rf3)
  --label-suffix S  suffix appended to result file name (default: none = gateway fan-out;
                    use _pr for the primary-replication path). All runs are real durable
                    writes (the node always runs with --no-idempotent).

  # Kubernetes mode (recommended)
  --k8s             run via GKE k6 operator
  --cluster NAME    GKE cluster name (default: streamshard-bench)
  --region ZONE     GKE cluster zone/region (default: europe-west3-a)
  --workers N       number of parallel k6 pods (default: 9)
  --project ID      GCP project ID (default: se-streamshard)

  Peak committed_rps is the USL data point. If the peak sits at the very top of
  the ramp, raise --rps. If dropped_iterations is high before the peak, raise --workers.

examples:
  $0 http://IP:7070 1 --k8s --workers 8 --rps 100000 --rf rf1
  $0 http://IP:7070 3 --k8s --workers 8 --rps 100000 --rf rf1
  $0 http://IP:7070 5 --k8s --workers 8 --rps 100000 --rf rf1
EOF
  exit 1
}

[[ $# -lt 2 ]] && usage

BASE_URL="$1"; shift
NODE_COUNT="$1"; shift

MAX_RPS=100000
STEPS=40
BASE_URL2=""
INSTANCE="e2"
RF="rf3"
LABEL_SUFFIX=""
USE_K8S=false
GKE_CLUSTER="streamshard-bench"
GKE_REGION="europe-west3-a"
WORKERS=9
GCP_PROJECT="se-streamshard"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rps)           MAX_RPS="$2"; shift 2 ;;
    --steps)         STEPS="$2";   shift 2 ;;
    --base-url2)     BASE_URL2="$2"; shift 2 ;;
    --instance)      INSTANCE="$2";   shift 2 ;;
    --rf)            RF="$2";         shift 2 ;;
    --label-suffix)  LABEL_SUFFIX="$2"; shift 2 ;;
    --k8s)           USE_K8S=true;    shift   ;;
    --cluster)       GKE_CLUSTER="$2"; shift 2 ;;
    --region)        GKE_REGION="$2"; shift 2 ;;
    --workers)       WORKERS="$2";    shift 2 ;;
    --project)       GCP_PROJECT="$2"; shift 2 ;;
    -h|--help)       usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

LABEL="${INSTANCE}_${RF}${LABEL_SUFFIX}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

RESULT_FILE="$RESULTS_DIR/${NODE_COUNT}node_${LABEL}.json"
K8S_DIR="$SCRIPT_DIR/k8s"

# MAX_RPS is the total system offered load at the top. The k6 operator
# splits this across worker pods via execution segments, so each pod is handed the
# full MAX_RPS and k6 divides it internally
echo "==> sweep: nodes=$NODE_COUNT  instance=$INSTANCE  rf=$RF  max_rps=$MAX_RPS (0 -> MAX ramp)  url=$BASE_URL"


run_k8s() {
  echo "==> k8s mode: cluster=$GKE_CLUSTER  workers=$WORKERS  max_rps=$MAX_RPS (split across pods)"

  gcloud container clusters get-credentials "$GKE_CLUSTER" \
    --zone "$GKE_REGION" --project "$GCP_PROJECT" --quiet

  if ! kubectl get crd testruns.k6.io &>/dev/null; then
    echo "==> installing k6 operator ..."
    curl -sL https://raw.githubusercontent.com/grafana/k6-operator/main/bundle.yaml \
      | kubectl apply -f -
    echo "==> waiting for operator to be ready ..."
    kubectl wait deployment/k6-operator-controller-manager \
      -n k6-operator-system --for=condition=available --timeout=120s
  fi

  kubectl create namespace bench --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f "$K8S_DIR/configmap.yaml"

  kubectl delete testrun k6-sweep -n bench --ignore-not-found

  # Sample per-process CPU on every node and gateway for the duration of the sweep,
  # so each result has a matching cpu_poll alongside it
  CPU_POLL_OUT="${RESULT_FILE%.json}_cpu.csv"
  NODES="$NODE_COUNT" GATEWAYS="$NODE_COUNT" ZONE="$GKE_REGION" PROJECT="$GCP_PROJECT" \
    OUT="$CPU_POLL_OUT" "$SCRIPT_DIR/poll_cpu.sh" >/dev/null 2>&1 &
  CPU_POLL_PID=$!
  trap '[[ -n "${CPU_POLL_PID:-}" ]] && kill "$CPU_POLL_PID" 2>/dev/null' EXIT
  echo "==> cpu polling started (pid $CPU_POLL_PID) -> $CPU_POLL_OUT"

  # The operator splits load across parallelism pods using execution segments,
  # so all pods start simultaneously and each covers a distinct slice of VUs.
  sed \
    -e "s|WORKERS_PLACEHOLDER|${WORKERS}|g" \
    -e "s|BASE_URL_PLACEHOLDER|${BASE_URL}|g" \
    -e "s|BASE_URL2_PLACEHOLDER|${BASE_URL2}|g" \
    -e "s|MAX_RPS_PLACEHOLDER|${MAX_RPS}|g" \
    -e "s|STEPS_PLACEHOLDER|${STEPS}|g" \
    "$K8S_DIR/testrun.yaml" | kubectl apply -f -

  echo "==> waiting for TestRun k6-sweep to finish ..."
  # The operator sometimes leaves status.stage stuck at "created" even after every
  # runner pod has Completed, so also break once all runner pods have finished.
  until kubectl get testrun k6-sweep -n bench \
        -o jsonpath='{.status.stage}' 2>/dev/null | grep -qE "finished|error"; do
    running="$(kubectl get pods -n bench -l "k6_cr=k6-sweep,runner=true" \
      --field-selector=status.phase!=Succeeded -o name 2>/dev/null | wc -l)"
    total="$(kubectl get pods -n bench -l "k6_cr=k6-sweep,runner=true" -o name 2>/dev/null | wc -l)"
    if [[ "$total" -gt 0 && "$running" -eq 0 ]]; then
      echo "==> all $total runner pods Completed (status.stage stuck); proceeding"
      break
    fi
    sleep 5
  done

  kill "$CPU_POLL_PID" 2>/dev/null
  trap - EXIT
  CPU_POLL_PID=""
  echo "==> cpu poll saved -> $CPU_POLL_OUT"

  echo "==> collecting results ..."
  python3 "$K8S_DIR/aggregate.py" \
    --namespace bench \
    --testrun k6-sweep \
    --out "$RESULT_FILE"

  kubectl delete testrun k6-sweep -n bench --ignore-not-found
}

if $USE_K8S; then
  run_k8s
else
  echo "ERROR: --k8s required" >&2; exit 1
fi

echo "==> saved $RESULT_FILE"

# USL fitting

VENV="$SCRIPT_DIR/analysis/venv"
ensure_venv() {
  if [[ ! -f "$VENV/bin/python" ]]; then
    echo "==> creating python venv"
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q scipy matplotlib numpy
  fi
}

# Auto-fit a scaling curve once all three node counts for a label exist. lbl is the
# full file label including the replication suffix (gateway fan-out has none, primary
# replication is _pr).
try_usl() {
  local lbl="$1"
  local title="$2"
  local xmax="$3"
  local f1="$RESULTS_DIR/1node_${lbl}.json"
  local f3="$RESULTS_DIR/3node_${lbl}.json"
  local f5="$RESULTS_DIR/5node_${lbl}.json"
  [[ -f "$f1" && -f "$f3" && -f "$f5" ]] || return 0
  ensure_venv
  echo "==> USL fit: $lbl"
  "$VENV/bin/python" "$SCRIPT_DIR/analysis/usl.py" \
    "$f1" "$f3" "$f5" \
    --nodes 1 3 5 \
    --title "$title" \
    --xmax "$xmax" \
    --out "$RESULTS_DIR/scaling_curve_${lbl}.png"
}

try_usl "e2_rf1"        "RF=1, e2-standard-2"                     60000
try_usl "e2_rf3"        "RF=3 gateway-fanout, e2-standard-2"      30000
try_usl "e2_rf3_pr"     "RF=3 primary-replication, e2-standard-2" 24000
try_usl "e2big_rf3_pr"  "RF=3 primary-replication, e2-standard-4" 24000

echo "==> have: $(ls "$RESULTS_DIR"/*.json 2>/dev/null | xargs -n1 basename | tr '\n' ' ')"
