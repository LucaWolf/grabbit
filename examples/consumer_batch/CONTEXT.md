# `examples/consumer_batch` — Component Package Contract

> **Classification:** Component Package (Peanut) — leaf, standalone executable demo module.
> No subdirectories, no test files, no exported API: this module is reference material for
> humans showing batch consumption with grabbit's processor callback; it is not imported by any
> other package.

## Overview

A single-file demo (`main.go`, `package main`) of **batch consumption**: publish 500 messages
("msg-0" … "msg-499") to the durable queue `workload`, then consume them with four concurrent
consumers carrying different prefetch counts (8 / 3 / 4 / 7). A `CallbackProcessMessages`
handler receives each prefetched batch and either bulk-ACKs it, or — for batches of 5 or more —
randomly splits it, bulk-ACKing one part and bulk-NAcking (re-enqueueing) the other, so
messages get re-delivered. A registry goroutine records every ACK-ed body; on shutdown the
program verifies that all 500 messages were ACK-ed exactly once, flagging duplicates and
missing entries. The demo also shows topology declaration via `WithChannelOptionTopology` and
the publisher's returned-message callback.

Module facts (from `go.mod` / `go.sum` / `.gitignore`):

- Own Go module: `module consumer_batch`, `go 1.21` — this directory is **not** part of the
  root module's package graph; the root module's `go test ./...` does not compile it.
- Requires `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` with
  `replace github.com/LucaWolf/grabbit => ../../../grabbit` — the demo compiles against the
  **local working-tree copy** of the library, not the published version.
- Requires `github.com/rabbitmq/amqp091-go v1.9.0`; `go.sum` additionally pins
  `go.uber.org/goleak v1.2.1` (pulled in via the local grabbit tree).
- `.gitignore` contains `consumer_batch` (the compiled binary produced by `go build` in this
  directory).

## Interface

**Entrypoint:** `main()` in `main.go`. No CLI flags and no environment variables — all
configuration is hardcoded:

| Item | Value |
| --- | --- |
| Broker address | `amqp://guest:guest@localhost` (guest/guest, default vhost `/`), `amqp.Config{}` |
| Connection name | `conn.main`, bound to cancelable `ctxMaster` (`context.WithCancel(context.TODO())`) |
| Event channel | buffered `chan grabbit.Event`, capacity 10, `defer close(events)` |
| Publisher channel name | `chan.publisher.example` |
| Queue | `workload` — declared via `TopologyOptions{Name: "workload", IsDestination: true, Durable: true, Declare: true}` |
| Publish routing | default exchange (`""`), routing key `workload` (via `WithKey`), confirmation count 20 (default 10) |
| Message count | `MSG_COUNT = 500`, bodies `"msg-0"` … `"msg-499"`, zero-value `amqp.Publishing{}` (transient, no headers) |
| Consumers | four, names `consumer.one` … `consumer.four`, channel names `chan.cons.one` … `chan.cons.four`, prefetch counts 8 / 3 / 4 / 7, shared `WithPrefetchTimeout(5 * time.Second)`, queue `workload` |
| Split RNG | `rand.New(rand.NewSource(543752))` — deterministic seed, shared across handlers under a mutex |
| Registry channel | buffered `chan grabbit.DeliveryPayload`, capacity 300 |
| Shutdown signals | `SIGINT`, `SIGTERM` |

**grabbit API surface called by this demo:**

- `grabbit.NewConnection`, `grabbit.NewPublisher`, `grabbit.NewConsumer`
- Connection options: `WithConnectionOptionContext`, `WithConnectionOptionName`,
  `WithConnectionOptionNotification`
- Publisher options: `DefaultPublisherOptions()`, then `.WithKey(...)`, `.WithContext(...)`,
  `.WithConfirmationsCount(...)`
- Consumer options: `DefaultConsumerOptions()`, then `.WithQueue(...)`,
  `.WithPrefetchTimeout(...)`, `.WithPrefetchCount(...)`, `.WithName(...)`
- Channel options: `WithChannelOptionName`, `WithChannelOptionTopology`,
  `WithChannelOptionNotifyReturn`, `WithChannelOptionProcessor`
- Publisher methods: `(*Publisher).Publish(amqp.Publishing) error`,
  `(*Publisher).AwaitStatus(established bool, timeout time.Duration) bool`
- Types & callbacks: `grabbit.TopologyOptions`, `grabbit.Event`, `grabbit.DeliveryPayload`,
  `grabbit.DeliveryData` (`.Body`, `.DeliveryTag`), `grabbit.DeliveriesProperties`
  (`.ConsumerTag`), `grabbit.CallbackProcessMessages`, `grabbit.CallbackNotifyReturn`
- Channel methods used inside the processor/return callbacks:
  `(*Channel).Ack(tag, multiple)`, `(*Channel).Nack(tag, multiple, requeue)`,
  `(*Channel).Name()`, `(*Channel).Queue()`

**Program flow:**

1. Builds the connection with `ctxMaster` and the shared event channel; a goroutine logs every
   event. The events channel is closed by a deferred `close(events)` at `main` exit.
2. Configures the publisher: `DefaultPublisherOptions()` mutated to key `workload`, context
   `ctxMaster`, confirmation count 20; declares topology (the durable queue `workload`,
   `Declare: true`, `IsDestination: true`) via `WithChannelOptionTopology`; registers
   `OnNotifyReturn` via `WithChannelOptionNotifyReturn`.
3. Blocks up to 30 s on `publisher.AwaitStatus(true, ...)`; on timeout cancels `ctxMaster`,
   waits 7 s and exits.
4. `PublishMsg` publishes 500 zero-value `amqp.Publishing` messages with bodies `"msg-%d"`
   through `publisher.Publish`.
5. Starts the validation machinery: the `validate` channel (cap 300), the `msgRegistry` map
   guarded by `muReg`, and the `HandleMsgRegistry` goroutine that records each received body,
   logging a red `ERROR duplicate` if a body was already ACK-ed.
6. Creates four consumers (`consumer.one` … `consumer.four`, prefetch 8 / 3 / 4 / 7), each with
   `WithChannelOptionProcessor(MsgHandler(rnd, &mu, validate))`.
7. `MsgHandler` behavior (invoked by grabbit with each prefetched batch):
   - Returns immediately when `mustAck` is false (auto-ack mode guard; not active here since
     `DefaultConsumerOptions` leaves AutoAck false).
   - Batches of fewer than 5 messages: log the full body list, bulk-ACK the whole range with
     `ch.Ack(lastDeliveryTag, true)`.
   - Batches of 5 or more: split at 2/3 of the batch; a mutex-guarded `rnd.Int()%2` coin toss
     decides which part is bulk-ACKed; the other part is bulk-NAcked with `requeue = true`,
     forcing broker redelivery. `BodySlice` forwards every ACK-ed body into the `validate`
     channel and returns the comma-joined bodies for logging.
8. Blocks awaiting `SIGINT`/`SIGTERM` (comment: restarting the Rabbit engine should leave the
   topology available once channels recover); then cancels `ctxMaster`, waits 3 s, locks the
   registry mutex and reports: red `ERROR missing ACK <n> vs. <500>` on undercount (actual
   format string `\033[91mERROR\033[0m missing ACK %d vs. %d`), otherwise green
   `OK all messages were registered fine.`

## Invariants

- **Consumers require manual ack:** `DefaultConsumerOptions()` leaves AutoAck false, so the
  processor is always invoked with `mustAck == true`; the handler's `!mustAck` early-return is
  a defensive guard for auto-ack configurations.
- **Batching is prefetch-driven with a 5 s flush:** each consumer's batch size is its prefetch
  count (8 / 3 / 4 / 7). `WithPrefetchTimeout(5 * time.Second)` makes grabbit flush partial
  batches after message flow pauses (raising `EventDataPartial`, or `EventDataExhausted` when
  nothing is pending — grabbit `consumer_options.go:62-72`), so tail-end batches smaller than
  the prefetch count are still processed.
- **Nacked messages are requeued and must be redelivered:** every NAck uses `requeue = true`,
  so the split halves come back and are re-processed until eventually ACK-ed. The registry
  therefore must end with exactly 500 unique bodies — duplicates (double-ACK) and missing
  entries are both reported as errors.
- **One context rules everything:** the publisher and all four consumer channels get no
  channel-level context, so grabbit defaults each channel's context to the *connection's*
  context (`channel.go:81`, derived cancelable at `channel.go:86`). Cancelling `ctxMaster`
  tears down the publisher, all four consumers, and the connection in one shot.
- **The final report cannot deadlock only because the context is cancelled first:**
  `HandleMsgRegistry` holds `muReg` for its entire lifetime and only returns on
  `ctx.Done()` (or channel close). `main`'s final `muReg.Lock()` is therefore guaranteed to be
  free thanks to the `ctxCancel()` + 3 s wait that precede it.
- **Topology declaration semantics:** the queue `workload` has `Declare: true`, so grabbit
  declares it at initial channel establishment (`makeTopology()` via
  `NewChannel` → `reconnectLoop()`, `channel.go:101, 296`). During *recovery*, every declared
  element is re-declared, durable included
  ([ADR-0003: recovery re-declares all declared topology](../../docs/adr/0003-recovery-redeclares-all-declared-topology.md));
  the demo's own closing comment ("we should get all the topology recreated when … recovery has
  completed") matches that behavior. AMQP declares are idempotent for identical parameters, so
  the durable queue is recreated even after a fully reset broker.
- **Transient messages do not survive broker restarts:** published `amqp.Publishing{}` values
  leave DeliveryMode unset (non-persistent), so a broker restart mid-run drops undelivered
  messages; the final 500-message report can then legitimately report missing ACKs.
- **Deterministic split decisions, shared safely:** the RNG is seeded (`543752`) and shared
  by all four handlers under `mu`, so the ACK/NAck coin toss is reproducible and race-free.
- **Publisher readiness is gated, not assumed:** `AwaitStatus(true, 30 s)` (grabbit
  `publisher.go:201`) must succeed before publishing; the demo otherwise cancels and exits
  instead of publishing into a dead channel.
- **Bulk confirmation window is 20:** `WithConfirmationsCount(20)` raises the default (10)
  publisher confirmation count (`publisher_options.go:32`).
- **Returned messages are surfaced:** `OnNotifyReturn` matches grabbit's
  `CallbackNotifyReturn` (`general_options.go:78`) and logs any `basic.return` with the
  channel's name and queue.
- **Consumer and channel names are unique per connection:** `consumer.one` … `consumer.four`
  and `chan.cons.one` … `chan.cons.four` are all distinct, as required for independent
  supervision and cancellation.
- **Notification events are lossy under load:** the event channel is buffered (10) and
  grabbit's `raise` drops events when full (`events.go:62-67`).

## Side Effects

**External systems (requires live RabbitMQ broker):**

- Opens a TCP/AMQP connection to `localhost` (named `conn.main` on the server).
- Declares the durable queue `workload` (no exchange; routing is default-exchange → queue
  name).
- Publishes 500 transient messages (bodies `"msg-0"` … `"msg-499"`).
- Bulk-ACKs and bulk-NAcked ranges of delivery tags (`multiple = true`,
  `channel_safewrappers.go:67,78`); NAcked ranges are requeued, causing broker redelivery to
  the four consumers.

**Process effects:**

- Goroutines: one event-logging loop (terminates when `events` is closed at exit), the
  registry handler `HandleMsgRegistry` (terminates on `ctx.Done()`), one signal handler, plus
  grabbit-internal loops for the connection, the publisher channel and four consumer channels.
- Signal handling: `SIGINT` / `SIGTERM` end the run and trigger the final registry report.
- Output: `log` (stderr) throughout, with ANSI color codes — red `\033[91m` for duplicate /
  missing-message errors, green `\033[92m` for the final `OK all messages were registered
  fine.`; batch logs show consumer tag, delivery-tag ranges and comma-joined bodies.
- Writes no files; no HTTP or other I/O.

## Verification

- **Compile-only check:** `go test ./...` — there are no test files in this module, so the
  command only verifies that the package compiles (`ok consumer_batch [no test files]`). Run it
  **inside this directory**; being a standalone module, it is not covered by the root module's
  `go test ./...`.
- `go build ./...` compiles the `consumer_batch` binary (ignored via `.gitignore`).
- **Running the demo:** `go run .` requires a live RabbitMQ broker at
  `amqp://guest:guest@localhost` with guest/guest credentials. The program publishes and
  processes 500 messages, then waits for Ctrl-C; the expected observable outcome is the green
  `OK all messages were registered fine.` line after shutdown (any duplicate/missing report is
  a demo failure signal).
- **Dependency caveat:** the `replace` directive resolves grabbit to the local working tree,
  whose `go.mod` currently requires `amqp091-go v1.10.0` and `go-notifying-latch v1.1.0`,
  while this module's `go.sum` pins `amqp091-go v1.9.0` / `goleak v1.2.1` and lacks
  `go-notifying-latch` entirely. If the compile check reports missing `go.sum` entries, run
  `go mod tidy` here first. The replaced root module's `go 1.25.7` directive also means a Go
  toolchain ≥ 1.25.7 (or GOTOOLCHAIN auto-switching) is required, even though this module
  declares `go 1.21`.
