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

mkdir -p /opt/streamshard/bin /var/lib/streamshard-cp
git clone ${repo_url} /opt/streamshard/src 2>/dev/null || git -C /opt/streamshard/src pull
cd /opt/streamshard/src
go build -o /opt/streamshard/bin/controlplane ./cmd/controlplane

cat > /etc/systemd/system/streamshard-controlplane.service <<EOF
[Unit]
Description=StreamShard control plane
After=network.target

[Service]
ExecStart=/opt/streamshard/bin/controlplane --addr :${cp_port} --data-dir /var/lib/streamshard-cp
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now streamshard-controlplane
