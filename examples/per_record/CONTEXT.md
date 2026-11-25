# `examples/per_record` — Component Package Contract

> **Classification:** Component Package (Peanut) — leaf, standalone executable demo module.
> No subdirectories, no test files, no exported API: this module is reference material for
> humans showing per-record concurrent consumption with grabbit's record processor; it is
> not imported by any other package.

## Overview

A single-file demo (`main.go`, `package main`) of **per-record concurrent consumption**:
publish 24 messages (`record-000` … `record-023`) to the durable queue
`workload.per_record`, then consume them with one consumer running `ProcessorConcurrency(8)`
over `PrefetchCount(8)`. Every record callback sleeps 200 ms, so the demo's in-flight
counter shows the bounded worker overlap. The callback branches on the message index to
exercise all three `RecordOutcome` values: ack, nack-requeue, and nack-drop. A requeued
record is dropped on its redelivery, and a poison record is dropped immediately; no record
loops. The program self-terminates once all 24 messages reach a terminal outcome and prints
a summary with attempt/outcome counts and peak overlap.

Module facts (from `go.mod` / `go.sum` / `.gitignore`):

- Own Go module: `module per_record`, `go 1.25.7` — this directory is **not** part of the
  root module's package graph; the root module's `go test ./...` does not compile it.
- Requires `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` with
  `replace github.com/LucaWolf/grabbit => ../../../grabbit` — the demo compiles against the
  **local working-tree copy** of the library, not the published version.
- Requires `github.com/rabbitmq/amqp091-go v1.10.0`; `go.sum` also carries
  `go-notifying-latch v1.1.0`, `rabbit-hole/v3 v3.2.0`, and `go.uber.org/goleak v1.3.0`
  from the replaced module graph. The `go 1.25.7` directive comes from the local grabbit
  module and is why this demo needs Go ≥ 1.25.7.
- `.gitignore` contains `per_record` (the compiled binary produced by `go build` in this
  directory).

## Interface

**Entrypoint:** `main()` in `main.go`. No CLI flags and no environment variables — all
configuration is hardcoded:

| Item | Value |
| --- | --- |
| Broker address | `amqp://guest:guest@127.0.0.1:5672/` (guest/guest, default vhost `/`), `amqp.Config{}` |
| Connection name | `conn.per_record`, bound to cancelable `ctx` (`context.WithCancel`) |
| Event channel | buffered `chan grabbit.Event`, capacity 32; a goroutine logs until `ctx` is cancelled |
| Publisher channel name | `chan.per_record.publisher` |
| Queue | `workload.per_record` — declared via `TopologyOptions{Name: ..., IsDestination: true, Durable: true, Declare: true}` |
| Publish routing | default exchange (`""`), routing key `workload.per_record` (via `WithKey`), confirmation count 24 |
| Message count | `messageCount = 24`, bodies `record-000` … `record-023`; each publishing carries `MessageId` = index |
| Consumer | name `consumer.per_record`, channel `chan.per_record`, queue `workload.per_record`, `PrefetchCount(8)`, `ProcessorConcurrency(8)` |
| Record delay | `recordDelay = 200ms` inside every callback invocation |
| Run timeout | `runTimeout = 60s`; the demo also exits on `SIGINT`/`SIGTERM` |
| Queue hygiene | `QueuePurge` before publishing so a rerun starts from zero |

**grabbit API surface called by this demo:**

- `grabbit.NewConnection`, `grabbit.NewPublisher`, `grabbit.NewConsumer`
- Connection options: `WithConnectionOptionContext`, `WithConnectionOptionName`,
  `WithConnectionOptionNotification`
- Publisher options: `DefaultPublisherOptions()`, then `.WithKey(...)`, `.WithContext(...)`,
  `.WithConfirmationsCount(...)`
- Consumer options: `DefaultConsumerOptions()`, then `.WithName(...)`, `.WithQueue(...)`,
  `.WithPrefetchCount(...)`, `.WithProcessorConcurrency(...)`
- Channel options: `WithChannelOptionName`, `WithChannelOptionTopology`,
  `WithChannelOptionProcessRecord`
- Publisher methods: `(*Publisher).AwaitStatus`, `PublishDeferredConfirm`,
  `AwaitDeferredConfirmation`, `Close`, `AwaitManager`; channel `QueuePurge`
- Consumer methods: `(*Consumer).AwaitStatus`, `Close`, `AwaitManager`
- Types & callbacks: `grabbit.TopologyOptions`, `grabbit.Event`, `grabbit.DeliveryData`
  (`.MessageId`, `.Redelivered`), `grabbit.DeliveriesProperties` (`.ConsumerTag`),
  `grabbit.CallbackProcessRecord`, `grabbit.RecordOutcome` (`RecordAck`,
  `RecordNackRequeue`, `RecordNackDrop`), `grabbit.ConfirmationACK`

**Program flow:**

1. Builds the connection with `ctx` and the shared event channel; a goroutine logs every
   event until cancellation.
2. Configures the publisher with the queue topology and a confirmation count of 24; gates on
   `AwaitStatus(true, 30s)`.
3. Purges the queue, then publishes all 24 records with `PublishDeferredConfirm` and waits
   for each `ConfirmationACK`. Closes the publisher and waits for its manager.
4. Creates the concurrent consumer with `PrefetchCount(8)` and
   `ProcessorConcurrency(8)`, and registers `recordProcessor` via
   `WithChannelOptionProcessRecord`; gates on `AwaitStatus(true, 30s)`.
5. Each callback sleeps 200 ms, then branches on `MessageId % 3`:
   - `0` → `RecordAck` (normal completion);
   - `1` → `RecordNackRequeue` on first delivery, `RecordNackDrop` when
     `Redelivered == true`;
   - `2` → `RecordNackDrop` (poison record).
6. The callback logs the branch, redelivery flag, current in-flight count, and
   terminal-outcome progress. On the 24th terminal outcome it signals `done`
   (non-blocking, buffered).
7. `main` waits for `done`, a signal, or the 60 s timeout; after `done` it waits 500 ms for
   the last outcomes to reach the wire, prints the summary, and exits. Deferred cancels and
   `Close`/`AwaitManager` calls let the connection and consumer loops terminate.

## Invariants

- **One dispatch model, fixed at construction:** the demo always uses the concurrent path
  because `ProcessorConcurrency(8)` is positive; no batch processor is registered and no
  runtime model switching occurs.
- **All three outcomes are exercised deterministically:** ack (`idx%3==0`), requeue then
  drop (`idx%3==1`), and immediate poison drop (`idx%3==2`).
- **No infinite requeue:** a requeued record is dropped on its first redelivery, so each
  record reaches exactly one terminal outcome. The terminal-by-index map prevents duplicate
  terminal accounting.
- **Every record is manually acknowledged:** `DefaultConsumerOptions()` leaves AutoAck
  false; the library settles the returned outcome with `multiple=false`. The callback never
  touches a channel or issues an ack itself.
- **Bounded overlap:** within one stable generation at most 8 callbacks run at once
  (`ProcessorConcurrency`), further capped by `PrefetchCount(8)` under manual ack; the
  expected peak is 8. A recovery transition can transiently overlap the old and new
  generations for up to `2N = 16` callbacks, per ADR-0008; this demo does not force one.
- **No batch machinery:** `PrefetchTimeout` and
  `EventDataPartial`/`EventDataExhausted` never fire on this path; completion is tracked by
  the callback's terminal-outcome counter, not by a drain event.
- **Publisher confirms before consumption:** each publish is confirmed before the consumer
  starts, so the workload is complete and stable for the demonstration.
- **No new event kinds:** the demo only logs the existing `grabbit.Event` values and its own
  record/outcome lines.

## Side Effects

- **Goroutines:** the connection manager, publisher channel manager, consumer channel
  manager, the per-generation record worker pool (8 workers), and the event-logging
  goroutine. The managed objects are closed before `main` returns; the event logger stops
  when the final deferred `cancel()` runs, so teardown events can still appear after
  `EXIT`.
- **Broker state:** the durable queue `workload.per_record` is declared and purged at the
  start. The demo does not delete the queue on exit, so a rerun reuses it (after purging).
- **Logs:** one line per callback invocation with the chosen outcome, redelivery flag,
  in-flight count, and terminal-outcome progress, plus infrastructure events and a final
  `SUMMARY` line.
- **Shutdown:** normal completion after all records reach a terminal outcome;
  `SIGINT`/`SIGTERM` exits early
  with a partial summary; the 60 s timeout guards a stalled broker. The process does not
  hang on missing infrastructure because publisher/consumer readiness gates return after
  30 s.

## Verification

Compile-only, from this directory:

```sh
go test ./...
```

Run against a live broker at the hardcoded address:

```sh
go run .
```

Against the LavinMQ container used by the root test suite, the expected terminal summary is:

```text
SUMMARY exit="all records terminal" attempts=32 acks=8 requeues=8 drops=16 terminal=24/24 peak-overlap=8
```

`attempts` is 24 first deliveries plus 8 requeues. The peak is up to 8 in one generation;
the observed value under LavinMQ is 8. A failed or interrupted run prints `INCOMPLETE` with
the terminal-outcome count.
