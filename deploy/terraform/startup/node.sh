#!/bin/bash
set -euo pipefail

apt-get update -qq && apt-get install -y -qq git curl

export HOME="/root"
export GOROOT="/usr/local/go"
export GOPATH="/root/go"
export GOCACHE="/root/.cache/go-build"
export GOTOOLCHAIN=local
export PATH="$GOROOT/bin:$GOPATH/bin:$PATH"

mkdir -p "$GOPATH" "$GOCACHE"

if ! command -v go &>/dev/null; then
  curl -sSL https://go.dev/dl/go1.24.4.linux-amd64.tar.gz | tar -C /usr/local -xz
fi

INTERNAL_IP=$(curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" -H "Metadata-Flavor: Google")

mkdir -p /opt/streamshard/bin ${data_dir}
git clone ${repo_url} /opt/streamshard/src 2>/dev/null || git -C /opt/streamshard/src pull
cd /opt/streamshard/src
go build -o /opt/streamshard/bin/node ./cmd/node

SWIM_FLAGS=""
if [ "${enable_swim}" = "true" ]; then
  SWIM_FLAGS="--swim-addr 0.0.0.0:${swim_port} --swim-http-addr $${INTERNAL_IP}:${node_port}"
  if [ -n "${swim_seeds}" ]; then
    SWIM_FLAGS="$${SWIM_FLAGS} --swim-seeds ${swim_seeds}"
  fi
fi

PRIMARY_REPL_FLAG=""
if [ "${primary_replication}" = "true" ]; then
  PRIMARY_REPL_FLAG="--primary-replication"
fi

cat > /etc/systemd/system/streamshard-node.service <<EOF
[Unit]
Description=StreamShard partition node
After=network.target

[Service]
ExecStart=/opt/streamshard/bin/node \
  --addr :${node_port} \
  --data-dir ${data_dir} \
  --window 1m \
  --topk 10 \
  --queue-cap ${queue_cap} \
  --peers ${node_addrs} \
  --rf ${rf} \
  --w ${w} \
  --no-idempotent \
  --wal-batch ${wal_batch} \
  $${SWIM_FLAGS} $${PRIMARY_REPL_FLAG}
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now streamshard-node
