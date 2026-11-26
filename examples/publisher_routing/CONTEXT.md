# publisher_routing

**Classification:** Component Package (Peanut) — standalone `package main` demo module.
**Parent blueprint:** [../CONTEXT.md](../CONTEXT.md) (examples)

## Overview

`publisher_routing` is a self-contained Go module (module path `publisher_routing`, Go 1.21) demonstrating **AMQP topology declaration and routing-key dispatch** with the grabbit library. It declares two ephemeral exchanges (one `direct`, one `topic`) and two queues, binds each queue to both exchanges, publishes 30 messages through three distinct routing paths (direct exchange, topic exchange, default exchange), and finally attaches a consumer to each queue to prove the messages landed where expected.

It is one of four sibling publisher demo modules under `examples/` (alongside `pub_deferred`, `publisher`, and `publisher_await_delivery`), each an independent Go module with its own `go.mod`/`go.sum`. All four map the dependency `github.com/LucaWolf/grabbit` to the repository root via `replace github.com/LucaWolf/grabbit => ../../../grabbit`, so the demos always compile against the working tree.

File inventory:

| File | Role |
| --- | --- |
| `main.go` | The entire demo program: routing constants, topology construction, batch publisher, consumer processor, `main()`. |
| `go.mod` / `go.sum` | Module definition; requires `github.com/LucaWolf/grabbit` (replaced to working tree) and `github.com/rabbitmq/amqp091-go v1.9.0`. |
| `.gitignore` | Ignores the compiled binary `publisher_routing`. |

There are no test files and no README in this directory.

## Interface

### Module identity

- Module path: `publisher_routing`; `go 1.21`.
- Requires: `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` (with `replace github.com/LucaWolf/grabbit => ../../../grabbit`) and `github.com/rabbitmq/amqp091-go v1.9.0`.
- Entrypoint: executable binary. No command-line flags, arguments, or environment variables; all configuration is hard-coded (broker URL, names, keys, timings).

### Routing constants (declared in `main`)

- `KEY_ALERTS = "key.alert"` — routing key into the pagers queue.
- `KEY_INFO = "key.info"` — routing key into the emails queue.
- `KEY_TOPIC_ALERTS = "*.alerts"` — alerts key mask for the topic exchange.
- `KEY_TOPIC_INFO = "*.info"` — key mask for the topic exchange.
- `QUEUE_PAGERS = "queue.pagers"` — deposit for alert-routed messages.
- `QUEUE_EMAILS = "queue.emails"` — deposit for info-routed messages.
- `EXCHANGE_LOGS = "exch.direct.logs"` — direct key-dispatch exchange.
- `EXCHANGE_GATEWAY = "exch.topic"` — topic-dispatch exchange.

### Declared identifiers (package main)

- `func publishSomeLogs(publisher *grabbit.Publisher, opt grabbit.PublisherOptions, start, end int)` — publishes `end-start` messages with bodies `"msg ID %04d for target (%s, %s)"` (filling in `opt.Exchange`, `opt.Key`) via `publisher.PublishWithOptions(opt, message)`; publish errors are logged and the loop continues.
- `func MsgHandler(props *grabbit.DeliveriesProperties, messages []grabbit.DeliveryData, mustAck bool, ch *grabbit.Channel)` — grabbit `CallbackProcessMessages` processor: logs every message with `ch.Name()` and `props.ConsumerTag`, then acks each delivery individually with `ch.Ack(msg.DeliveryTag, false)` (multiple = false). Note: it acks unconditionally, regardless of the `mustAck` flag.
- `func main()` — orchestrates the demo (flow below).

### main() flow

1. `connStatusChan := make(chan grabbit.Event, 10)` with `defer close(connStatusChan)`; a goroutine logs every notification event.
2. `ctxMaster, ctxCancel := context.WithCancel(context.TODO())`.
3. Connection: `grabbit.NewConnection("amqp://guest:guest@localhost", amqp.Config{}, WithConnectionOptionContext(ctxMaster), WithConnectionOptionName("conn.main"), WithConnectionOptionNotification(connStatusChan))`.
4. Topology — six `grabbit.TopologyOptions` entries in order:
   - exchange `exch.direct.logs`: `Declare: true, IsExchange: true, IsDestination: false, Durable: false, Kind: "direct"`;
   - exchange `exch.topic`: same flags with `Kind: "topic"`;
   - queue `queue.pagers` with `Bind: grabbit.TopologyBind{Enabled: true, Peer: EXCHANGE_LOGS, Key: KEY_ALERTS}`;
   - queue `queue.pagers` (second entry, `Declare: true` — the source notes this is "a must for applying a bind") with `Bind{Peer: EXCHANGE_GATEWAY, Key: KEY_TOPIC_ALERTS}`;
   - queue `queue.emails` with `Bind{Peer: EXCHANGE_LOGS, Key: KEY_INFO}`;
   - queue `queue.emails` (second entry) with `Bind{Peer: EXCHANGE_GATEWAY, Key: KEY_TOPIC_INFO}`.
   The `exch.direct.logs` entry is commented as "ephemeral", as are both queue entries; the `exch.topic` entry carries no comment. The queue entries appear twice because `TopologyOptions.Bind` holds a single `TopologyBind` — the source remarks: "Shame we have not implemented 'bind' as array".
5. `opt := grabbit.DefaultPublisherOptions()`; then `opt.WithContext(ctxMaster).WithConfirmationsCount(20)` (builder methods mutate `opt` in place and return `*PublisherOptions`).
6. `publisher := grabbit.NewPublisher(conn, opt, WithChannelOptionName("chan.publisher"), WithChannelOptionTopology(topos))`.
7. Readiness gate: `publisher.AwaitStatus(true, 10*time.Second)`; on failure logs `"publisher not ready yet"`, calls `ctxCancel()`, waits 3 s, and exits.
8. Six publish batches of five messages each (30 total), retargeting `opt` before each batch:
   1. `opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_ALERTS)` → indices 0–5 (direct exchange → `queue.pagers`);
   2. `opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.alerts")` → 5–10 (topic mask `*.alerts` → `queue.pagers`);
   3. `opt.WithExchange("").WithKey(QUEUE_PAGERS)` → 10–15 (default exchange, straight onto `queue.pagers`);
   4. `opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_INFO)` → 15–20 (direct → `queue.emails`);
   5. `opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.info")` → 20–25 (topic mask `*.info` → `queue.emails`);
   6. `opt.WithExchange("").WithKey(QUEUE_EMAILS)` → 25–30 (default exchange → `queue.emails`).
9. Consumers — created only after all publishing completes: `optConsumer := grabbit.DefaultConsumerOptions()` with `WithPrefetchTimeout(7 * time.Second)`; then
   - `grabbit.NewConsumer(conn, *optConsumer.WithName("cons.emails").WithQueue(QUEUE_EMAILS), WithChannelOptionName("chan.emails/info"), WithChannelOptionProcessor(MsgHandler))`;
   - `grabbit.NewConsumer(conn, *optConsumer.WithName("cons.pagers").WithQueue(QUEUE_PAGERS), WithChannelOptionName("chan.pagers/alert"), WithChannelOptionProcessor(MsgHandler))`.
   The `*` dereference is required because the `ConsumerOptions` builder methods mutate and return `*ConsumerOptions`, while `NewConsumer` accepts the struct **by value**. The two returned `*Consumer` values are discarded (`_ =`); no `AwaitStatus` gate is applied to consumers.
10. Blocks for SIGINT/SIGTERM (`signal.Notify`), then `ctxCancel()`, 3 s pause, EXIT. There are no explicit `Close()` calls on publisher, consumers, or connection — context cancellation plus process exit terminate everything.

### grabbit API surface exercised

`NewConnection`, `WithConnectionOptionContext/.Name/.Notification`, `DefaultPublisherOptions`, `PublisherOptions.WithContext/.WithConfirmationsCount/.WithExchange/.WithKey`, `TopologyOptions` fields `Name/Declare/IsExchange/IsDestination/Durable/Kind/Bind`, `TopologyBind` fields `Enabled/Peer/Key`, `NewPublisher`, `WithChannelOptionName`, `WithChannelOptionTopology`, `Publisher.AwaitStatus`, `Publisher.PublishWithOptions`, `DefaultConsumerOptions`, `ConsumerOptions.WithPrefetchTimeout/.WithName/.WithQueue`, `NewConsumer`, `WithChannelOptionProcessor`, `Channel.Ack`, `Channel.Name`; types `Event`, `DeliveriesProperties`, `DeliveryData`. From amqp091-go: `amqp.Config`, `amqp.Publishing`.

## Invariants

- **Ephemeral topology:** both exchanges are `Durable: false` and the queue entries leave `Durable` unset (false). On every recovery all declared elements are re-declared ([ADR-0003](../../docs/adr/0003-recovery-redeclares-all-declared-topology.md)), so the demo topology survives grabbit reconnections — including after a broker restart, which wipes ephemeral definitions server-side and has them re-created on the subsequent recovery.
- **`Declare` is required for binds:** every topology entry sets `Declare: true`; the source marks it "a must for applying a bind" — entries with `Declare: false` would be skipped entirely, including their binding.
- **One bind per topology entry:** `TopologyOptions.Bind` is a single `TopologyBind`, not an array; `queue.pagers` and `queue.emails` each appear twice in the topology slice to acquire their two bindings (direct + topic exchanges).
- **Three routing paths per queue:** exact-key routing via the direct exchange (`key.alert`, `key.info`), wildcard-mask routing via the topic exchange (`*.alerts`, `*.info` matching `gw.alerts`, `gw.info`), and default-exchange delivery (empty `Exchange` with the queue name as routing key) all land in the same queues.
- **Publishing completes before consuming:** both consumers are attached only after all 30 messages are published, so messages accumulate in the queues and are drained at consumer start.
- **Per-message acknowledgment:** `MsgHandler` acks each delivery individually via `ch.Ack(msg.DeliveryTag, false)` (multiple = false) regardless of `mustAck`.
- **Consumer identity:** consumer names are explicit and unique (`cons.emails`, `cons.pagers`, overriding the random default); consumer channels are named `"chan.emails/info"` and `"chan.pagers/alert"`; default prefetch count is 1 with a 7 s prefetch timeout.
- **Readiness gate:** 10 s `AwaitStatus(true, …)` before the first publish; failure path cancels the context and exits.
- **Context governs lifetime:** `ctxCancel()` runs at shutdown; grabbit's recovery loops watch `ctxMaster.Done()`.
- **Notification buffer:** `connStatusChan` is buffered at 10; grabbit drops events when a notification buffer is full, so the logging goroutine may miss events under sustained churn.

## Side Effects

- **Network:** managed AMQP connection to `amqp://guest:guest@localhost`; one publisher channel plus two consumer channels.
- **Broker topology:** declares 2 ephemeral non-durable exchanges (`exch.direct.logs` direct, `exch.topic` topic), 2 non-durable queues (`queue.pagers`, `queue.emails`), and 4 bindings.
- **Broker traffic:** publishes 30 messages (6 batches × 5, bodies `msg ID 0000`…`0029`); consumes and individually acks all of them through the two consumers.
- **Goroutines:** notification-log goroutine; grabbit management goroutines for the connection and each channel; consumer dispatch goroutines.
- **Logging:** every notification event, every consumed message, and shutdown progress via the `log` package (stderr).
- **Process:** installs SIGINT/SIGTERM handlers; `connStatusChan` is closed by the deferred `close` when `main` returns.

## Verification

From this directory (`examples/publisher_routing`):

```sh
# Compile-only check — there are no test files, so this verifies the
# module builds against the replaced grabbit working tree:
go test ./...

# Actually running the demo requires a live RabbitMQ broker
# at amqp://guest:guest@localhost:
go run .
```

Expected runtime behavior with a broker: 30 messages routed in three ways (15 per queue), then both consumers drain and ack their queues while logging each delivery; the process idles until SIGINT/SIGTERM. Without a broker: `AwaitStatus` fails after 10 s, the context is cancelled, and the program exits.
