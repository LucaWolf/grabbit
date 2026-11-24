# publisher

**Classification:** Component Package (Peanut) — standalone `package main` demo module.
**Parent blueprint:** [../CONTEXT.md](../CONTEXT.md) (examples)

## Overview

`publisher` is a self-contained Go module (module path `publisher`, Go 1.21) providing the **baseline fire-and-forget publisher demo** of the grabbit library. It establishes a managed connection with the full set of lifecycle callbacks (down/up/recovering), declares the durable queue `workload`, waits for channel readiness, publishes five messages at a steady one-per-second pace, then closes the publisher and deliberately attempts five more publishes to demonstrate failure behavior on a closed channel.

It is one of four sibling publisher demo modules under `examples/` (alongside `pub_deferred`, `publisher_await_delivery`, and `publisher_routing`), each an independent Go module with its own `go.mod`/`go.sum`. All four map the dependency `github.com/LucaWolf/grabbit` to the repository root via `replace github.com/LucaWolf/grabbit => ../../../grabbit`, so the demos always compile against the working tree.

Relative to its sibling `publisher_await_delivery` (which shares the same skeleton, names, and shutdown choreography), this variant does **not** capture publish sequence numbers or await confirmations: `WithChannelOptionNotifyPublish`/`WithChannelOptionNotifyReturn` are wired purely for logging, making this the simplest end-to-end publishing example.

File inventory:

| File | Role |
| --- | --- |
| `main.go` | The entire demo program: lifecycle callbacks, publish loop, `main()`. |
| `go.mod` / `go.sum` | Module definition; requires `github.com/LucaWolf/grabbit` (replaced to working tree) and `github.com/rabbitmq/amqp091-go v1.9.0`. |
| `.gitignore` | Ignores the compiled binary `publisher`. |

There are no test files and no README in this directory.

## Interface

### Module identity

- Module path: `publisher`; `go 1.21`.
- Requires: `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` (with `replace github.com/LucaWolf/grabbit => ../../../grabbit`) and `github.com/rabbitmq/amqp091-go v1.9.0`.
- Entrypoint: executable binary. No command-line flags, arguments, or environment variables; all configuration is hard-coded (broker URL, names, timings).

### Declared identifiers (package main)

- `func OnPubDown(name string, err grabbit.OptionalError) bool` — grabbit `CallbackWhenDown`; logs the failure; returns `true` (recovery should continue).
- `func OnPubUp(name string)` — grabbit `CallbackWhenUp`; logs.
- `func OnPubReattempting(name string, retry int) bool` — grabbit `CallbackWhenRecovering`; logs the retry count; returns `true`.
- `func OnNotifyPublish(confirm amqp.Confirmation, ch *grabbit.Channel)` — grabbit `CallbackNotifyPublish`; logs `confirm.Ack` and `ch.Queue()`; keeps no state (contrast with `publisher_await_delivery`, where the equivalent is a factory feeding a confirmation channel).
- `func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel)` — grabbit `CallbackNotifyReturn`; logs returned (unroutable) publishes.
- `func PublishMsg(publisher *grabbit.Publisher, start, end int)` — for `i` in `[start, end)`: sleeps 1 s, builds the body `"test number %04d"`, calls `publisher.Publish(message)`; publish errors are logged and the loop continues.
- `func main()` — orchestrates the demo (flow below).

### main() flow

1. Local names: connection `"conn.main"`, channel `"chan.publisher.example"`, queue `"workload"`.
2. `ctxMaster, ctxCancel := context.WithCancel(context.TODO())`; `pubStatusChan := make(chan grabbit.Event, 32)` drained by a goroutine that logs every notification event.
3. `conn := grabbit.NewConnection("amqp://guest:guest@localhost", amqp.Config{}, grabbit.WithConnectionOptionContext(ctxMaster), grabbit.WithConnectionOptionName("conn.main"))`.
4. `pubOpt := grabbit.DefaultPublisherOptions()`; then `pubOpt.WithKey("workload").WithContext(ctxMaster).WithConfirmationsCount(20)` (builder methods mutate `pubOpt` in place and return `*PublisherOptions`).
5. Topology: one entry `grabbit.TopologyOptions{Name: "workload", IsDestination: true, Durable: true, Declare: true}`.
6. `publisher := grabbit.NewPublisher(conn, pubOpt, …)` with channel options: `WithChannelOptionContext(ctxMaster)`, `WithChannelOptionName("chan.publisher.example")`, `WithChannelOptionTopology(topos)`, `WithChannelOptionNotification(pubStatusChan)`, `WithChannelOptionDown(OnPubDown)`, `WithChannelOptionUp(OnPubUp)`, `WithChannelOptionRecovering(OnPubReattempting)`, `WithChannelOptionNotifyPublish(OnNotifyPublish)`, `WithChannelOptionNotifyReturn(OnNotifyReturn)`.
7. Readiness gate: `publisher.AwaitStatus(true, 30*time.Second)`; on failure logs `"publisher not ready yet"`, calls `ctxCancel()`, waits 7 s, and exits.
8. `PublishMsg(publisher, 0, 5)` — five steady-paced publishes.
9. Deferred shutdown: `publisher.Close()` (error logged), then `PublishMsg(publisher, 5, 10)` — five more publishes **expected to fail** because the channel is closed, then `conn.Close()` (error logged), 3 s pause, EXIT.
10. Blocks main on `signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)`; a goroutine closes `done` on signal receipt, releasing `main` to return (and run the deferred shutdown).

### grabbit API surface exercised

`NewConnection`, `WithConnectionOptionContext`, `WithConnectionOptionName`, `DefaultPublisherOptions`, `PublisherOptions.WithKey/.WithContext/.WithConfirmationsCount`, `TopologyOptions` fields `Name/IsDestination/Durable/Declare`, `NewPublisher`, `WithChannelOptionContext/.Name/.Topology/.Notification/.Down/.Up/.Recovering/.NotifyPublish/.NotifyReturn`, `Publisher.AwaitStatus`, `Publisher.Publish`, `Publisher.Close`, `Channel.Queue`, `Connection.Close`; types `Event`, `OptionalError`, `CallbackWhenDown`, `CallbackWhenUp`, `CallbackWhenRecovering`, `CallbackNotifyPublish`, `CallbackNotifyReturn`. From amqp091-go: `amqp.Config`, `amqp.Publishing`, `amqp.Confirmation`, `amqp.Return`.

## Invariants

- **Pacing:** exactly one message per second (a 1 s `<-time.After` precedes every publish attempt).
- **Readiness gate:** nothing is published until `AwaitStatus(true, 30 s)` confirms the channel is established; the failure path cancels the context and exits.
- **Durable destination:** queue `workload` is declared with `Durable: true, Declare: true, IsDestination: true`, so it persists across broker restarts server-side; on every recovery all declared elements are re-declared, durable included ([ADR-0003](../../docs/adr/0003-recovery-redeclares-all-declared-topology.md)), so a fully reset broker gets the queue recreated.
- **Confirmation mode enabled but unobserved:** `WithConfirmationsCount(20)` arms publisher confirmations and the `NotifyPublish`/`NotifyReturn` callbacks log broker responses, but the demo never blocks on a confirmation — publishing is fire-and-forget with observational callbacks (contrast `publisher_await_delivery`).
- **Recovery continues:** all down/recovering callbacks return `true`, instructing grabbit to keep retrying; the callbacks are purely observational (logging only).
- **Publish-after-close fails:** the second `PublishMsg` (indices 5–9) runs after `publisher.Close()`; each attempt is expected to fail and be logged — this is an intentional demonstration of closed-channel behavior, not a bug.
- **Notification buffer:** `pubStatusChan` is buffered at 32; grabbit drops events when a notification buffer is full, so the logging goroutine may miss events under sustained churn.
- **Shutdown ordering:** the publisher closes before the connection (the channel depends on the connection); context cancellation is the library's loop-termination mechanism, applied here only on the not-ready failure path.

## Side Effects

- **Network:** opens a managed (auto-reconnecting) AMQP connection to `amqp://guest:guest@localhost` and a publisher channel with confirmations enabled.
- **Broker topology:** declares the durable queue `workload`.
- **Broker traffic:** publishes 5 messages (`"test number 0000"` … `"test number 0004"`) pre-close; attempts 5 more post-close (expected failures).
- **Goroutines:** starts a notification-log goroutine (drains `pubStatusChan`) and a signal-watch goroutine; grabbit runs its own connection/channel management goroutines.
- **Logging:** all feedback via the `log` package (stderr): up/down/retrying callbacks, notification events, each publish attempt, shutdown progress.
- **Process:** installs SIGINT/SIGTERM handlers; on signal the deferred block closes the publisher, then the connection, and exits after ~3 s.

## Verification

From this directory (`examples/publisher`):

```sh
# Compile-only check — there are no test files, so this verifies the
# module builds against the replaced grabbit working tree:
go test ./...

# Actually running the demo requires a live RabbitMQ broker
# at amqp://guest:guest@localhost:
go run .
```

Expected runtime behavior with a broker: five steady-paced publishes with confirmation callbacks firing, then five logged publish failures after close; the process idles until SIGINT/SIGTERM. Without a broker: `AwaitStatus` fails after 30 s, the context is cancelled, and the program exits.
