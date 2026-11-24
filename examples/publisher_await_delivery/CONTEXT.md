# publisher_await_delivery

**Classification:** Component Package (Peanut) — standalone `package main` demo module.
**Parent blueprint:** [../CONTEXT.md](../CONTEXT.md) (examples)

## Overview

`publisher_await_delivery` is a self-contained Go module (module path `publisher_await_delivery`, Go 1.21) demonstrating **synchronous publisher-confirmation handling** on top of the grabbit library. It publishes five numbered messages (one per second) to the durable queue `workload` and, for each publish, blocks up to 7 seconds awaiting the broker's confirmation, matching the pre-publish sequence number (`Publisher.Channel().GetNextPublishSeqNo()`) against the confirmed `DeliveryTag` to detect out-of-sync confirmations.

It is one of four sibling publisher demo modules under `examples/` (alongside `pub_deferred`, `publisher`, and `publisher_routing`), each an independent Go module with its own `go.mod`/`go.sum`. All four map the dependency `github.com/LucaWolf/grabbit` to the repository root via `replace github.com/LucaWolf/grabbit => ../../../grabbit`, so the demos always compile against the working tree.

File inventory:

| File | Role |
| --- | --- |
| `main.go` | The entire demo program: `ConfirmationOutcome`/`ConfirmationData` types, lifecycle callbacks, confirmation-await helper, publish loop, `main()`. |
| `confirmationoutcome_string.go` | `stringer`-generated `String()` for `ConfirmationOutcome`; carries a compile-time guard; DO NOT EDIT. |
| `go.mod` / `go.sum` | Module definition; requires `github.com/LucaWolf/grabbit` (replaced to working tree) and `github.com/rabbitmq/amqp091-go v1.9.0`. |
| `.gitignore` | Ignores the compiled binary `publisher_await_delivery`. |

There are no test files and no README in this directory.

## Interface

### Module identity

- Module path: `publisher_await_delivery`; `go 1.21`.
- Requires: `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` (with `replace github.com/LucaWolf/grabbit => ../../../grabbit`) and `github.com/rabbitmq/amqp091-go v1.9.0`.
- Entrypoint: executable binary. No command-line flags, arguments, or environment variables; all configuration is hard-coded (broker URL, names, timings).

### Declared identifiers (package main)

- `type ConfirmationOutcome int` — result enum, declared with `//go:generate stringer -type=ConfirmationOutcome -linecomment`:
  - `ConfirmationTimeOut` (iota) — "no timely response"
  - `ConfirmationClosed` — "data channel is closed"
  - `ConfirmationACK` — "publish confirmed by server"
  - `ConfirmationNAK` — "publish negative acknowledgement"
- `confirmationoutcome_string.go` — generated `func (i ConfirmationOutcome) String() string`; its guard array (`x[ConfirmationTimeOut-0]`, …) turns any drift of the constant values into a compile error, forcing a `stringer` re-run.
- `type ConfirmationData struct` — fields: `Outcome ConfirmationOutcome` (acknowledgment received stats), `ReqSequence uint64` (DeliveryTag of our request), `RspSequence uint64` (DeliveryTag of the notification).
- `func OnPubDown(name string, err grabbit.OptionalError) bool` — grabbit `CallbackWhenDown`; logs the failure; returns `true` (recovery should continue).
- `func OnPubUp(name string)` — grabbit `CallbackWhenUp`; logs.
- `func OnPubReattempting(name string, retry int) bool` — grabbit `CallbackWhenRecovering`; logs the retry count; returns `true`.
- `func OnNotifyPublish(confCh chan amqp.Confirmation) grabbit.CallbackNotifyPublish` — factory returning a `CallbackNotifyPublish` closure that logs each confirmation (`confirmation.DeliveryTag`, `confirmation.Ack`, `ch.Name()`, `ch.Queue()`) and forwards it into `confCh`; when `confCh` is full it logs `"confirmation channel out of bandwidth"` and **drops** the confirmation (non-blocking `select` with `default`).
- `func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel)` — grabbit `CallbackNotifyReturn`; logs returned (unroutable) publishes.
- `func AwaitPublishConfirmation(seqNo uint64, confCh chan amqp.Confirmation, tmr time.Duration) ConfirmationData` — records `ReqSequence = seqNo`, then blocks on `confCh` until: `tmr` elapses → `ConfirmationTimeOut`; the channel is closed → `ConfirmationClosed`; or a confirmation arrives → `RspSequence = confirmation.DeliveryTag` and `ConfirmationACK`/`ConfirmationNAK` per `confirmation.Ack`.
- `func PublishMsg(confCh chan amqp.Confirmation, publisher *grabbit.Publisher, start, end int)` — for `i` in `[start, end)`: sleeps 1 s, builds the body `"test number %04d"`, captures `nextSeqNo := publisher.Channel().GetNextPublishSeqNo()`, calls `publisher.Publish(message)`; on success awaits the confirmation with `AwaitPublishConfirmation(nextSeqNo, confCh, 7*time.Second)` and logs the outcome; logs `"FIXME! how do we recover from this?"` when `ReqSequence != RspSequence`.
- `func main()` — orchestrates the demo (flow below).

### main() flow

1. Local names: connection `"conn.main"`, channel `"chan.publisher.example"`, queue `"workload"`.
2. `ctxMaster, ctxCancel := context.WithCancel(context.TODO())`; `pubStatusChan := make(chan grabbit.Event, 32)` drained by a goroutine that logs every notification event.
3. `conn := grabbit.NewConnection("amqp://guest:guest@localhost", amqp.Config{}, grabbit.WithConnectionOptionContext(ctxMaster), grabbit.WithConnectionOptionName("conn.main"))`.
4. `pubOpt := grabbit.DefaultPublisherOptions()`; then `pubOpt.WithKey("workload").WithContext(ctxMaster).WithConfirmationsCount(20)` (builder methods mutate `pubOpt` in place and return `*PublisherOptions`).
5. Topology: one entry `grabbit.TopologyOptions{Name: "workload", IsDestination: true, Durable: true, Declare: true}`.
6. `confCh := make(chan amqp.Confirmation, 10)` — the confirmation mailbox.
7. `publisher := grabbit.NewPublisher(conn, pubOpt, …)` with channel options: `WithChannelOptionContext(ctxMaster)`, `WithChannelOptionName("chan.publisher.example")`, `WithChannelOptionTopology(topos)`, `WithChannelOptionNotification(pubStatusChan)`, `WithChannelOptionDown(OnPubDown)`, `WithChannelOptionUp(OnPubUp)`, `WithChannelOptionRecovering(OnPubReattempting)`, `WithChannelOptionNotifyPublish(OnNotifyPublish(confCh))`, `WithChannelOptionNotifyReturn(OnNotifyReturn)`.
8. Readiness gate: `publisher.AwaitStatus(true, 30*time.Second)`; on failure logs `"publisher not ready yet"`, calls `ctxCancel()`, waits 7 s, and exits.
9. `PublishMsg(confCh, publisher, 0, 5)` — five confirmed publishes.
10. Deferred shutdown: `publisher.Close()` (error logged), then `PublishMsg(confCh, publisher, 5, 10)` — five more publishes **expected to fail** because the channel is closed, then `conn.Close()` (error logged), 3 s pause, EXIT.
11. Blocks main on `signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)`; a goroutine closes `done` on signal receipt, releasing `main` to return (and run the deferred shutdown).

### grabbit API surface exercised

`NewConnection`, `WithConnectionOptionContext`, `WithConnectionOptionName`, `DefaultPublisherOptions`, `PublisherOptions.WithKey/.WithContext/.WithConfirmationsCount`, `TopologyOptions` fields `Name/IsDestination/Durable/Declare`, `NewPublisher`, `WithChannelOptionContext/.Name/.Topology/.Notification/.Down/.Up/.Recovering/.NotifyPublish/.NotifyReturn`, `Publisher.AwaitStatus`, `Publisher.Publish`, `Publisher.Close`, `Publisher.Channel`, `Channel.GetNextPublishSeqNo`, `Channel.Name`, `Channel.Queue`, `Connection.Close`; types `Event`, `OptionalError`, `CallbackWhenDown`, `CallbackWhenUp`, `CallbackWhenRecovering`, `CallbackNotifyPublish`, `CallbackNotifyReturn`. From amqp091-go: `amqp.Config`, `amqp.Publishing`, `amqp.Confirmation`, `amqp.Return`.

## Invariants

- **Confirmation before progression:** each message's confirmation (or timeout) is awaited before the next publish; the pipeline is strictly sequential.
- **Bounded wait:** every await is capped at 7 s; a missing broker response yields `ConfirmationTimeOut` ("no timely response"), never a hang.
- **Sequence matching:** `ReqSequence` (captured via `GetNextPublishSeqNo()` before `Publish`) is expected to equal `RspSequence` (the confirmed `DeliveryTag`). A mismatch is detected and logged as `"FIXME! how do we recover from this?"` — this demo deliberately does **not** implement recovery for the mismatch case.
- **Confirmation mailbox capacity:** `confCh` is buffered at 10. If confirmations arrive faster than `AwaitPublishConfirmation` drains them, `OnNotifyPublish` drops the overflow (logged as `"confirmation channel out of bandwidth"`); a dropped confirmation surfaces later as a timeout on the awaiting publish.
- **Closed channel short-circuits:** when `confCh` is closed, the await returns `ConfirmationClosed` ("data channel is closed") instead of blocking.
- **Readiness gate:** nothing is published until `AwaitStatus(true, 30 s)` confirms the channel is established.
- **Topology durability:** queue `workload` is declared with `Durable: true, Declare: true, IsDestination: true`, so it persists across broker restarts server-side. On every recovery all declared elements are re-declared, durable included ([ADR-0003](../../docs/adr/0003-recovery-redeclares-all-declared-topology.md)), so a broker that loses its durable definitions (e.g. fully reset) gets the queue recreated.
- **Publish-after-close fails:** the second `PublishMsg` (indices 5–9) runs after `publisher.Close()` and is expected to log publish failures — this demonstrates closed-channel behavior, not a bug.
- **Context governs lifetime:** cancelling `ctxMaster` is what terminates grabbit's reconnect/retry loops; the demo cancels it only on the not-ready failure path.
- **Generated-code guard:** `confirmationoutcome_string.go` must be regenerated (`go generate`) whenever the `ConfirmationOutcome` constants change; the guard array makes silent drift a compile error.

## Side Effects

- **Network:** opens a managed (auto-reconnecting) AMQP connection to `amqp://guest:guest@localhost` and a publisher channel.
- **Broker topology:** declares the durable queue `workload`.
- **Broker traffic:** publishes 5 messages (`"test number 0000"` … `"test number 0004"`) pre-close; attempts 5 more post-close (expected failures).
- **Goroutines:** starts a notification-log goroutine (drains `pubStatusChan`) and a signal-watch goroutine; grabbit runs its own connection/channel management goroutines.
- **Logging:** all feedback via the `log` package (stderr): up/down/retrying callbacks, notification events, each publish attempt, each confirmation outcome, shutdown progress.
- **Process:** installs SIGINT/SIGTERM handlers; on signal the deferred block closes the publisher, then the connection, and exits after ~3 s.

## Verification

From this directory (`examples/publisher_await_delivery`):

```sh
# Compile-only check — there are no test files, so this verifies the
# module builds against the replaced grabbit working tree:
go test ./...

# Actually running the demo requires a live RabbitMQ broker
# at amqp://guest:guest@localhost:
go run .
```

Expected runtime behavior with a broker: five publishes each confirmed with `ConfirmationACK` and matching sequences, then five logged publish failures after close; the process idles until SIGINT/SIGTERM. Without a broker: `AwaitStatus` fails after 30 s, the context is cancelled, and the program exits.
