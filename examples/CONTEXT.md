# `examples/` — Intermediate Blueprint

**Module graph note:** every directory below belongs to the root module
`github.com/LucaWolf/grabbit`; there are no nested `go.mod` / `go.sum` files or `replace`
directives. Each child is an independent `package main` at import path
`github.com/LucaWolf/grabbit/examples/<name>`, compiled by the root module's
`go build ./...` and `go test ./...`.

Human-oriented catalog and run instructions: [README.md](./README.md). This file is
the agent-facing contract.

## Overview

Ten single-purpose demo programs, each a self-contained `package main` with one `main.go`,
no CLI flags, no tests, and one demo concern. All connect to a hardcoded broker
`amqp://guest:guest@localhost` (guest/guest), except `per_record`, which pins
`127.0.0.1` to avoid an IPv6 loopback mismatch; **running** any of them requires a live
RabbitMQ. Together they form a tutorial progression: lifecycle observation
(`connection`, `channel`, `contexts`), publishing (baseline → routing → confirmations),
and consuming (competing consumers → batch processing → per-record concurrency).

All examples share the root module's dependency versions (`amqp091-go` v1.10.0,
`go-notifying-latch` v1.1.0, Go 1.25.7), because there is only one `go.mod`. Add example-only
dependencies with care: they become dependencies of the library module.

[lavinmq-ha](./lavinmq-ha/README.md) is local broker infrastructure, not a Go package: a
Podman-managed three-node LavinMQ cluster with etcd coordination and an HAProxy AMQP front
end. `make lavinmq-up` exposes it at `amqp://guest:guest@localhost:5672/`, matching the
hardcoded broker address used by every demo.

## Subdirectories & Boundaries

- [channel](./channel/CONTEXT.md) — lower-level managed-`Channel` lifecycle: managed connection + data channel, lifecycle callbacks, and a queue manually re-declared from the `EventUp` handler on every channel-up.
- [connection](./connection/CONTEXT.md) — minimal managed-connection demo: lifecycle events and Down/Up/Recovering callbacks, `Close()` on SIGINT/SIGTERM; the baseline the other examples build on.
- [consumer](./consumer/CONTEXT.md) — two competing consumers on the durable queue `workload` randomly `Ack` vs `Nack(requeue=true)` the same 35 seeded messages, exercising recovery callbacks and consumer options.
- [consumer_batch](./consumer_batch/CONTEXT.md) — batch consumption at scale: 500 published messages, four consumers with distinct prefetch counts (8/3/4/7), a processor that bulk-ACKs or splits/NAcks batches, and an exactly-once ACK registry check at shutdown.
- [contexts](./contexts/CONTEXT.md) — context-tree control of channel lifecycles: asserts that closing a channel does **not** close sibling-context channels and that cancelling a parent context closes descendants; self-terminating, waits with `AwaitStatus(false, ...)` (context death emits no `EventClosed`).
- [per_record](./per_record/CONTEXT.md) — per-record concurrent consumption: 24 records, 8 workers, a 200 ms handler, explicit ack / requeue-then-drop / poison-drop outcomes, a terminal-outcome + peak-overlap summary, and the repository's JSON `log/slog` integration example for grabbit events and record logs. Run it with `go run ./examples/per_record` from the repository root.
- [pub_deferred](./pub_deferred/CONTEXT.md) — deferred publish confirmations via `Publisher.PublishDeferredConfirm` + `AwaitDeferredConfirmation`, one-by-one and in-order bulk awaits.
- [publisher](./publisher/CONTEXT.md) — baseline fire-and-forget publisher: five paced publishes to the durable queue `workload`, confirm callbacks for logging only, then five deliberate post-close failures.
- [publisher_await_delivery](./publisher_await_delivery/CONTEXT.md) — synchronous publisher confirmations: captures `Channel.GetNextPublishSeqNo()` before each publish and blocks (≤ 7 s) for the broker's confirmation, matching request vs response sequence numbers; uses the library's `ConfirmationOutcome` enum and stringer.
- [publisher_routing](./publisher_routing/CONTEXT.md) — topology and routing dispatch: two ephemeral exchanges (direct + topic), two queues, four bindings, 30 messages through three routing paths (direct exact-key, topic wildcard, default exchange), then two consumers drain both queues.
- [lavinmq-ha](./lavinmq-ha/README.md) — Podman infrastructure only (no Go package): a three-node LavinMQ cluster, etcd coordination, and HAProxy on `localhost:5672`. Not built by Go tooling.

## Verification

There are no test files under `examples/` — verification is compile-only, from the
repository root:

```sh
go build ./examples/...
go test ./examples/...
make examples          # builds every demo into bin/
```

Running any demo requires a live RabbitMQ-compatible broker. The demos use
`amqp://guest:guest@localhost` except `per_record`, which uses
`amqp://guest:guest@127.0.0.1:5672/`:

```sh
make lavinmq-up        # start the bundled LavinMQ HA cluster on :5672
go run ./examples/<name>
# or
make run EXAMPLE=<name>
```

`make lavinmq-up` requires Podman and publishes the cluster through HAProxy on
`localhost:5672`; `make lavinmq-down` stops it and `make lavinmq-reset` clears its data.
See [lavinmq-ha/README.md](./lavinmq-ha/README.md) for topology and ports.

See each child contract for its exact expected runtime behavior.
