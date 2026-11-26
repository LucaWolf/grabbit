#!/usr/bin/env bash
# Start the local high-availability LavinMQ cluster used by the examples.
#
# Topology:
#   - grabbit-lavinmq-haproxy  192.168.44.5   :5672 (AMQP), :9000 (stats)
#   - grabbit-lavinmq-etcd     192.168.44.10  :2379 / :2380
#   - grabbit-lavinmq1         192.168.44.101 :15672
#   - grabbit-lavinmq2         192.168.44.102 :15672 (published as :25672)
#   - grabbit-lavinmq3         192.168.44.103 :15672 (published as :35672)
#
# Requires Podman. Re-running the script replaces the containers; persisted
# LavinMQ data under lavin-data/ is kept. etcd coordination state is reset on
# every start because registrations from a previous container generation make
# LavinMQ reject duplicate node URIs.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v podman >/dev/null 2>&1; then
  echo "error: podman is required to start the LavinMQ cluster" >&2
  exit 1
fi

# Remove the current generation before clearing etcd's membership records.
for container in grabbit-lavinmq-haproxy grabbit-lavinmq-etcd grabbit-lavinmq1 grabbit-lavinmq2 grabbit-lavinmq3; do
  podman rm -f "$container" >/dev/null 2>&1 || true
done

rm -rf "$SCRIPT_DIR/etcd-data"
mkdir -p lavin-data/lavinmq1 lavin-data/lavinmq2 lavin-data/lavinmq3 etcd-data

if ! podman network exists grabbit-lavinmq-network; then
  podman network create --subnet 192.168.44.0/24 grabbit-lavinmq-network
fi

podman run -d --name grabbit-lavinmq-haproxy --replace \
  -p 5672:5672 \
  -p 9000:9000 \
  -v "$SCRIPT_DIR/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" \
  --network grabbit-lavinmq-network \
  --ip 192.168.44.5 \
  haproxy

podman run -d --replace \
  --name grabbit-lavinmq-etcd \
  --hostname etcd \
  --network grabbit-lavinmq-network \
  --network-alias etcd \
  --ip 192.168.44.10 \
  -p 2379:2379 -p 2380:2380 \
  -v "$SCRIPT_DIR/etcd-data:/var/lib/etcd/data" \
  gcr.io/etcd-development/etcd:v3.6.5 \
  /usr/local/bin/etcd \
  --name=etcd \
  --data-dir=/var/lib/etcd/data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --advertise-client-urls=http://etcd:2379 \
  --initial-advertise-peer-urls=http://etcd:2380 \
  --initial-cluster=etcd=http://etcd:2380 \
  --initial-cluster-state=new \
  --initial-cluster-token=etcd-cluster-0

# LavinMQ does not need to export 5672 (data) or 5679 (replication); HAProxy
# owns the public AMQP port.
podman run -d --replace \
  --name grabbit-lavinmq1 \
  --hostname lavinmq1 \
  --network grabbit-lavinmq-network \
  --network-alias lavinmq1 \
  --ip 192.168.44.101 \
  -p 15672:15672 \
  -v "$SCRIPT_DIR/lavinmq.ini:/etc/lavinmq/lavinmq.ini:ro" \
  -v "$SCRIPT_DIR/lavin-data/lavinmq1:/var/lib/lavinmq/data" \
  cloudamqp/lavinmq:latest

podman run -d --replace \
  --name grabbit-lavinmq2 \
  --hostname lavinmq2 \
  --network grabbit-lavinmq-network \
  --network-alias lavinmq2 \
  --ip 192.168.44.102 \
  -p 25672:15672 \
  -v "$SCRIPT_DIR/lavinmq.ini:/etc/lavinmq/lavinmq.ini:ro" \
  -v "$SCRIPT_DIR/lavin-data/lavinmq2:/var/lib/lavinmq/data" \
  cloudamqp/lavinmq:latest

podman run -d --replace \
  --name grabbit-lavinmq3 \
  --hostname lavinmq3 \
  --network grabbit-lavinmq-network \
  --network-alias lavinmq3 \
  --ip 192.168.44.103 \
  -p 35672:15672 \
  -v "$SCRIPT_DIR/lavinmq.ini:/etc/lavinmq/lavinmq.ini:ro" \
  -v "$SCRIPT_DIR/lavin-data/lavinmq3:/var/lib/lavinmq/data" \
  cloudamqp/lavinmq:latest

echo
echo "LavinMQ HA cluster started."
echo "  AMQP (HAProxy):     amqp://guest:guest@localhost:5672/"
echo "  HAProxy stats:      http://localhost:9000/"
echo "  LavinMQ management: http://localhost:15672/ (also :25672, :35672)"
echo
echo "Give the nodes a few seconds to elect a leader before running an example."
