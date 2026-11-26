# Local LavinMQ HA cluster

Podman-managed broker infrastructure for running the demos in `examples/`.
It is a three-node LavinMQ cluster with etcd-based service discovery and an
HAProxy in front of the AMQP port.

| Component | Container | Host access |
| --- | --- | --- |
| AMQP through HAProxy | `grabbit-lavinmq-haproxy` | `localhost:5672` |
| HAProxy stats | `grabbit-lavinmq-haproxy` | http://localhost:9000/ |
| LavinMQ management | `grabbit-lavinmq1` | http://localhost:15672/ |
| LavinMQ management | `grabbit-lavinmq2` | http://localhost:25672/ |
| LavinMQ management | `grabbit-lavinmq3` | http://localhost:35672/ |
| etcd | `grabbit-lavinmq-etcd` | `localhost:2379` |

All examples connect to `amqp://guest:guest@localhost` (`per_record` uses
`127.0.0.1`), so the HAProxy endpoint is a drop-in replacement for a local
RabbitMQ or LavinMQ instance.

## Usage

```sh
make lavinmq-up     # start and/or replace the containers
make lavinmq-down   # stop the containers, keep data
make lavinmq-reset  # stop the containers and delete data
```

The first `lavinmq-up` pulls the `haproxy`, etcd, and `cloudamqp/lavinmq`
images. After the script returns, allow a few seconds for leader election
before running an example:

```sh
make lavinmq-up
go run ./examples/consumer
```

## Files

- `cluster_up.sh` — creates the Podman network, data directories, and five
  containers (HAProxy, etcd, three LavinMQ nodes).
- `cluster_down.sh` — removes the containers and network.
- `lavinmq.ini` — LavinMQ clustering, management, and AMQP configuration.
- `haproxy.cfg` — AMQP round-robin backend and the stats UI.

Persisted broker data lives in `lavin-data/` and is ignored by Git. etcd's
`etcd-data/` directory is also ignored but is reset on every `lavinmq-up`: it only
holds cluster-membership records for the current container generation, and stale
records make LavinMQ reject a recreated node as a duplicate URI. The upstream
project also carried an obsolete RabbitMQ configuration (`rmq-obsolete.conf`); it
is not used by LavinMQ and was intentionally not copied.

## Troubleshooting

If nodes fail to form a cluster, run `make lavinmq-reset` and then
`make lavinmq-up`; this clears both the broker data and the transient etcd
state.

Port `5672` must be free. If a local RabbitMQ is already running, stop it first
or change the HAProxy publish port in `cluster_up.sh` and the example broker
URLs.
