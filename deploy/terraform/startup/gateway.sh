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

mkdir -p /opt/streamshard/bin
git clone ${repo_url} /opt/streamshard/src 2>/dev/null || git -C /opt/streamshard/src pull
cd /opt/streamshard/src
go build -o /opt/streamshard/bin/gateway ./cmd/gateway

cat > /etc/systemd/system/streamshard-gateway.service <<EOF
[Unit]
Description=StreamShard gateway
After=network.target

[Service]
ExecStart=/opt/streamshard/bin/gateway \
  --addr :${gw_port} \
  --peers ${node_addrs} \
  --controlplane ${cp_addr} \
  --rf ${rf} \
  --w ${w} \
  --rate ${gw_rate} \
  --burst ${gw_burst} \
  --breaker-threshold ${breaker_threshold} \
  --breaker-cooldown 10s \
  --max-inflight 100000 \
  ${disable_ratelimit_flag} ${primary_replication_flag}
Restart=on-failure
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now streamshard-gateway
