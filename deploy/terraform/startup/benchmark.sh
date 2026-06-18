#!/bin/bash
set -euo pipefail

apt-get update -qq && apt-get install -y -qq curl

K6_VERSION="v0.54.0"
curl -sSL "https://github.com/grafana/k6/releases/download/${K6_VERSION}/k6-${K6_VERSION}-linux-amd64.tar.gz" \
  | tar -C /usr/local/bin -xz --strip-components=1 "k6-${K6_VERSION}-linux-amd64/k6"

touch /tmp/k6-ready
