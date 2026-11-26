#!/usr/bin/env bash
# Stop the local LavinMQ cluster. Broker data under lavin-data/ is preserved;
# etcd membership data is discarded whenever cluster_up.sh next runs.

set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
  echo "error: podman is required to manage the LavinMQ cluster" >&2
  exit 1
fi

for container in grabbit-lavinmq-haproxy grabbit-lavinmq-etcd grabbit-lavinmq1 grabbit-lavinmq2 grabbit-lavinmq3; do
  podman rm -f "$container" >/dev/null 2>&1 || true
done

# The network is recreated by cluster_up.sh when it is missing.
podman network rm -f grabbit-lavinmq-network >/dev/null 2>&1 || true

echo "LavinMQ HA cluster stopped; data directories were preserved."
