# `examples/consumer` — Component Package (Peanut)

> EGM contract for `/home/luci/projects/go/grabbit/examples/consumer`.
> **Classification:** Component Package (Peanut) — standalone leaf demo module; nothing imports it.
> **Files:** `.gitignore`, `go.mod`, `go.sum`, `main.go` (the only source file). No `*_test.go` files exist.
> **Generated:** 2026-11-22 by the EGM bootstrap worker. Parent blueprint: [`../CONTEXT.md`](../CONTEXT.md).

## Overview

A self-contained demo module (module path `consumer`, `go 1.21`) — **not** part of the main
`github.com/LucaWolf/grabbit` module. Its single `package main` program demonstrates the consumer
side of grabbit, a managed, auto-reconnecting RabbitMQ client (AMQP is the wire protocol RabbitMQ
speaks). Step by step, `main.go`:

1. Creates a buffered notification channel `events` (`make(chan grabbit.Event, 10)`) and a drain
   goroutine that logs every event; the channel is closed by `defer close(events)` when `main`
   returns.
2. Creates `ctxMaster` via `context.WithCancel(context.TODO())`.
3. Opens a managed connection `conn.main` to **`amqp://guest:guest@localhost`** with
   `WithConnectionOptionContext(ctxMaster)` and `WithConnectionOptionNotification(events)`.
4. Builds publisher options `DefaultPublisherOptions().WithKey("workload").WithContext(ctxMaster).WithConfirmationsCount(20)`
   and a topology declaring the durable queue `workload`
   (`TopologyOptions{Name: "workload", IsDestination: true, Durable: true, Declare: true}`).
5. Creates publisher `chan.publisher.example` with `NotifyPublish`/`NotifyReturn` callbacks, then
   gates on `publisher.AwaitStatus(true, 30*time.Second)`. On timeout it cancels the context, waits
   7 s, logs `EXIT`, and exits early — no consumers are started.
6. Publishes 35 messages with bodies `test number 0000` … `test number 0034`
   (`PublishMsg(publisher, 0, 35)`).
7. Starts two competing consumers, `consumer.one` and `consumer.two` (channels
   `chan.consumer.one` / `chan.consumer.two`), each configured via
   `DefaultConsumerOptions().WithName(...).WithPrefetchCount(1).WithQueue("auto_generated_will_take_over").WithPrefetchTimeout(7 * time.Second)`
   with `Down`/`Up`/`Recovering` lifecycle callbacks and a shared message processor. Both
   `*Consumer` results are discarded (`_ =`).
8. The processor (`MsgHandler`, seeded `rand.NewSource(773274)`) randomly `Ack`s or
   `Nack(requeue=true)`s roughly half of each delivery batch, so the two consumers keep
   re-fighting over the same messages.
9. Blocks on `<-done` until `SIGINT`/`SIGTERM` arrives; the deferred cleanup cancels `ctxMaster`,
   waits 3 s, and logs `EXIT`. `Close()` is never called explicitly — shutdown relies on context
   cancellation.

Intended live drill: restart RabbitMQ while the demo runs — the durable queue `workload`
survives the restart server-side, so consuming resumes after recovery. The closing comment in
`main.go` describes the topology as "recreated" — which matches the *intended* contract
([ADR-0003: recovery re-declares all declared topology](../../docs/adr/0003-recovery-redeclares-all-declared-topology.md)) —
but the current code **skips durable elements** during recovery (`channel.go:336`), a known
deviation pending fix.

## Interface

Nothing here is importable — the module's only entrypoint is `main`:

| Symbol | Signature | Role |
| --- | --- | --- |
| `main` | `func main()` | Demo entrypoint. No CLI flags, no arguments, no environment variables are read. |
| `OnConsumerDown` | `func(name string, err grabbit.OptionalError) bool` | Satisfies `grabbit.CallbackWhenDown`; returns `true` → keep retrying recovery. |
| `OnConsumerUp` | `func(name string)` | Satisfies `grabbit.CallbackWhenUp`. |
| `OnConsumerReattempting` | `func(name string, retry int) bool` | Satisfies `grabbit.CallbackWhenRecovering`; returns `true` → keep retrying. |
| `OnNotifyPublish` | `func(confirm amqp.Confirmation, ch *grabbit.Channel)` | Satisfies `grabbit.CallbackNotifyPublish`; fires because the publisher channel runs in confirm mode. |
| `OnNotifyReturn` | `func(confirm amqp.Return, ch *grabbit.Channel)` | Satisfies `grabbit.CallbackNotifyReturn`. |
| `PublishMsg` | `func(publisher *grabbit.Publisher, start, end int)` | Publishes `end-start` messages. |
| `MsgHandler` | `func(rnd *rand.Rand, mu *sync.Mutex) grabbit.CallbackProcessMessages` | Builds the per-consumer message processor (`func(props *grabbit.DeliveriesProperties, messages []grabbit.DeliveryData, mustAck bool, ch *grabbit.Channel)`). |

**Module wiring** (`go.mod`): module path `consumer`, `go 1.21`; requires
`github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` and
`github.com/rabbitmq/amqp091-go v1.9.0`;
`replace github.com/LucaWolf/grabbit => ../../../grabbit` pins the demo to the **local repo
checkout**, never the published pseudo-version. `.gitignore` ignores the compiled binary `consumer`.

**grabbit API exercised** (identifiers verified against the library source):

- `NewConnection("amqp://guest:guest@localhost", amqp.Config{}, WithConnectionOptionContext(ctxMaster), WithConnectionOptionName("conn.main"), WithConnectionOptionNotification(events))`
- `DefaultPublisherOptions()` → `(*PublisherOptions).WithKey("workload")`, `.WithContext(ctxMaster)`, `.WithConfirmationsCount(20)` (library default is 10; a zero value disables confirm mode)
- `NewPublisher(conn, opt, WithChannelOptionName("chan.publisher.example"), WithChannelOptionTopology(topos), WithChannelOptionNotifyPublish(OnNotifyPublish), WithChannelOptionNotifyReturn(OnNotifyReturn))`
- `Publisher.AwaitStatus(true, 30*time.Second) bool` and `Publisher.Publish(amqp.Publishing) error`
- `DefaultConsumerOptions()` → `(*ConsumerOptions).WithName("consumer.one" | "consumer.two")`, `.WithPrefetchCount(1)` (QoS: how many unacknowledged messages the broker may send), `.WithQueue("auto_generated_will_take_over")`, `.WithPrefetchTimeout(7 * time.Second)`
- `NewConsumer(conn, opt, WithChannelOptionName("chan.consumer.one" | "chan.consumer.two"), WithChannelOptionTopology(topos), WithChannelOptionDown(OnConsumerDown), WithChannelOptionUp(OnConsumerUp), WithChannelOptionRecovering(OnConsumerReattempting), WithChannelOptionProcessor(MsgHandler(rnd, &mu)))`
- Inside the processor: `ch.Ack(msg.DeliveryTag, false)`, `ch.Nack(msg.DeliveryTag, false, true)`, `ch.Name()`, `props.ConsumerTag`; `ch.Queue()` is exercised only in the notify callbacks (`OnNotifyPublish`/`OnNotifyReturn`)
- Notifications: buffered `chan grabbit.Event` (`Event{SourceType, SourceName, TargetName, Kind, Err}`; kinds such as `EventUp`, `EventDataPartial`, `EventDataExhausted`)

## Invariants

1. **Live broker required** at exactly `amqp://guest:guest@localhost` with `guest`/`guest`
   credentials (hardcoded; default port, no vhost, no TLS).
2. **Queue-name override:** both consumers pass `WithQueue("auto_generated_will_take_over")`, but
   that literal is a placeholder (code comment: `// IsDestination above`). The channel's active
   queue is taken from the topology element marked `IsDestination`
   (`channel.go`: `if t.IsDestination { ch.queue = name }`), and the consume call overrides
   `ConsumerQueue` whenever `ch.queue` is non-empty (`channel_consume.go`). Both consumers
   therefore consume from the declared queue `workload`.
3. **Readiness gate:** if the publisher is not ready within 30 s, the program cancels `ctxMaster`,
   waits 7 s, logs `EXIT`, and returns — without starting consumers.
4. **Context-driven teardown:** recovery loops watch `ctxMaster.Done()`. The happy path never calls
   `Close()` on the connection/publisher/consumers (the `*Consumer` values are even discarded);
   termination relies on the deferred `ctxCancel()` plus a 3 s grace period. Library `Close()` is
   idempotent, so this is safe.
5. **Events can be silently dropped:** notifications travel on a buffered channel (capacity 10);
   `Event.raise` discards events when the buffer is full. The single drain goroutine is
   best-effort logging, not a reliable audit trail.
6. **Acknowledgment legality:** the processor only ACK/NAKs when `mustAck` is true (it is, because
   `ConsumerAutoAck` defaults to `false`). Across recovery transitions the library forces
   `mustAck = false`, so in-flight batches are left unacknowledged and are redelivered later.
7. **Shared RNG:** one `*rand.Rand` (seed `773274`) is shared by both consumer goroutines and
   guarded by a `sync.Mutex`, because `rand.Rand` values are not goroutine-safe (per the code
   comment).
8. **Unique consumer tags:** `consumer.one` / `consumer.two` are distinct consumer identifiers on
   the same connection (defaults would use `RandConsumerName()`; hand-picked names must be unique
   per connection).

## Side Effects

- **Network:** opens one managed AMQP connection to `amqp://guest:guest@localhost` plus three
  channels on it (publisher + two consumers); auto-reconnects with the library's
  exponential-backoff `DefaultDelayer` while `ctxMaster` is alive.
- **Broker topology:** declares the durable queue `workload` on the publisher channel and both
  consumer channels at initial establishment; during connection recovery the current code skips
  durable elements (`channel.go:336`; known deviation — see ADR-0003) — the queue persists
  server-side, so a normal broker restart leaves it in place (a fully reset broker would lose it
  until the intended recreate-all fix lands).
- **Message flow:** publishes 35 messages (`test number 0000`…`test number 0034`, set as the `Body`
  of a zero-value `amqp.Publishing{}` — no persistence flags or headers) to the default exchange
  with routing key `workload`; the two consumers pull them with random `Ack` vs
  `Nack(requeue=true)` (~50/50), so deliveries recirculate between the consumers until shutdown.
- **Goroutines:** event-drain goroutine (lives until `defer close(events)` at exit), signal
  watcher, plus library-internal manager/recovery goroutines.
- **Signals:** installs `SIGINT`/`SIGTERM` handlers; the first signal ends the main loop.
- **stderr via `log`** (exact format strings from `main.go`):
  - `callback: {%s} went down with {%s}` / `callback: {%s} went up` /
    `callback: connection establish {%s} retry count {%d}`
  - `callback: publish confirmed [%v] from  [%s][%s]` (note the double space after `from`) /
    `callback: publish returned from [%s][%s]`
  - `  [%s][%s] got message: %s -- [%s]` → e.g.
    `  [chan.consumer.one][consumer.one] got message: test number 0007 -- [ACK]` or
    `-- [NAK - expect this again later on]`
  - `publishing failed with: ` … / `notification: ` … / `publisher not ready yet` /
    `awaiting signal` / the received signal name / `EXIT`

## Verification

This module contains **no test files** (`*_test.go` count: 0), so the check is compile-only:

```sh
cd /home/luci/projects/go/grabbit/examples/consumer
go test ./...   # compiles the demo package; runs no tests
```

**Build prerequisite (verified statically against the current tree):** this module's pins predate
the current library checkout. Its `go.mod` requires `amqp091-go v1.9.0` while the replaced root
module now requires `v1.10.0` (module resolution selects the higher version), and its `go.sum` has
no entry for `go-notifying-latch v1.1.0`, which the library's non-test code now imports
(`connection.go`, `channel.go`, `utils.go`). Under Go's default read-only module mode,
`go test ./...` / `go build ./...` may therefore fail with *missing `go.sum` entry* errors until
`go mod tidy` (or `go mod download`) is run in this directory. The replaced module's `go 1.25.7`
directive also means a Go toolchain ≥ 1.25.7 is required, even though this module declares
`go 1.21`.

**Actually running the demo requires a live RabbitMQ broker** reachable at
`amqp://guest:guest@localhost` (default `guest`/`guest` account):

```sh
go run .
```

Expected behavior: ~35 messages cycling between `consumer.one` and `consumer.two` (roughly half
NACKed and requeued), ending with `EXIT` after `Ctrl-C`. Restarting the broker mid-run exercises
the recovery callbacks; the durable queue survives server-side (the current code skips durable
elements during recovery, `channel.go:336` — intended to change per ADR-0003).
