# `examples/` — Intermediate Blueprint

**Module graph note:** none of the directories below belong to the root module
`github.com/LucaWolf/grabbit`. Each is a **standalone Go module** (own `go.mod` / `go.sum`)
whose `replace github.com/LucaWolf/grabbit => ../../../grabbit` directive pins it to the
local working tree. The root module's `go test ./...` does **not** compile them.

## Overview

Nine single-purpose demo programs, each a self-contained `package main` with one `main.go`,
no CLI flags, no tests, and one demo concern. All connect to the same hardcoded broker
`amqp://guest:guest@localhost` (guest/guest), so actually **running** any of them
(`go run .`) requires a live RabbitMQ. Together they form a tutorial progression:
lifecycle observation (`connection`, `channel`, `contexts`), publishing (baseline →
routing → confirmations), and consuming (competing consumers → batch processing).

Shared build caveat (applies to every child): the demo `go.mod`/`go.sum` files predate
the current library tree — they pin `amqp091-go v1.9.0` while the root module requires
`v1.10.0`, and they lack `go-notifying-latch v1.1.0` (imported by grabbit's non-test
code). `go test ./...` / `go build ./...` may fail with missing `go.sum` entries until
`go mod tidy` is run in the demo directory; the replaced module's `go 1.25.7` directive
also means a Go toolchain ≥ 1.25.7 is required even though each demo declares `go 1.21`.

## Subdirectories & Boundaries

- [channel](./channel/CONTEXT.md) — lower-level managed-`Channel` lifecycle: managed connection + data channel, lifecycle callbacks, and a queue manually re-declared from the `EventUp` handler on every channel-up.
- [connection](./connection/CONTEXT.md) — minimal managed-connection demo: lifecycle events and Down/Up/Recovering callbacks, `Close()` on SIGINT/SIGTERM; the baseline the other examples build on.
- [consumer](./consumer/CONTEXT.md) — two competing consumers on the durable queue `workload` randomly `Ack` vs `Nack(requeue=true)` the same 35 seeded messages, exercising recovery callbacks and consumer options.
- [consumer_batch](./consumer_batch/CONTEXT.md) — batch consumption at scale: 500 published messages, four consumers with distinct prefetch counts (8/3/4/7), a processor that bulk-ACKs or splits/NAcks batches, and an exactly-once ACK registry check at shutdown.
- [contexts](./contexts/CONTEXT.md) — context-tree control of channel lifecycles: asserts that closing a channel does **not** close sibling-context channels and that cancelling a parent context closes descendants; self-terminating, polls `IsClosed()` (context death emits no `EventClosed`).
- [pub_deferred](./pub_deferred/CONTEXT.md) — deferred publish confirmations via `Publisher.PublishDeferredConfirm` + `AwaitDeferredConfirmation`, one-by-one and in-order bulk awaits; module path is `pub_deferred_confirm` (≠ directory name).
- [publisher](./publisher/CONTEXT.md) — baseline fire-and-forget publisher: five paced publishes to the durable queue `workload`, confirm callbacks for logging only, then five deliberate post-close failures.
- [publisher_await_delivery](./publisher_await_delivery/CONTEXT.md) — synchronous publisher confirmations: captures `Channel.GetNextPublishSeqNo()` before each publish and blocks (≤ 7 s) for the broker's confirmation, matching request vs response sequence numbers; contains its own `ConfirmationOutcome` enum + stringer.
- [publisher_routing](./publisher_routing/CONTEXT.md) — topology and routing dispatch: two ephemeral exchanges (direct + topic), two queues, four bindings, 30 messages through three routing paths (direct exact-key, topic wildcard, default exchange), then two consumers drain both queues.

## Verification

There are no test files anywhere under `examples/` — verification is compile-only, per
module (run inside the target directory):

```sh
go test ./...
```

Running any demo requires a live RabbitMQ broker at `amqp://guest:guest@localhost`:

```sh
go run .
```

See each child contract for its exact expected runtime behavior and the shared
`go mod tidy` build prerequisite described above.
