# `examples/pub_deferred` — Component Package (Peanut)

> EGM contract for `/home/luci/projects/go/grabbit/examples/pub_deferred`.
> **Classification:** Component Package (Peanut) — leaf `package main` demo within the root module; nothing imports it.
> **Files:** `main.go` (the only source file). No `*_test.go` files exist.
> **Generated:** 2026-11-22 by the EGM bootstrap worker. Parent blueprint: [`../CONTEXT.md`](../CONTEXT.md).

## Overview

A self-contained `package main` demo (import path
`github.com/LucaWolf/grabbit/examples/pub_deferred`), part of the root module, whose single
`package main` program demonstrates **deferred publish confirmations** in grabbit: end-to-end broker
acknowledgement of each published message via
`Publisher.PublishDeferredConfirm` → `*DeferredConfirmation` →
`Publisher.AwaitDeferredConfirmation`. Flow of `main.go`:

1. Creates `ctxMaster` (`context.WithCancel(context.TODO())`) and a buffered notification channel
   `pubStatusChan` (`make(chan grabbit.Event, 32)`) drained by a logging goroutine.
2. Opens connection `conn.main` to **`amqp://guest:guest@localhost`** (context + name options).
3. Builds publisher options
   `DefaultPublisherOptions().WithKey("workload").WithContext(ctxMaster).WithConfirmationsCount(20)`
   and a topology declaring the durable queue `workload`
   (`TopologyOptions{Name: "workload", IsDestination: true, Durable: true, Declare: true}`).
4. Creates publisher `chan.publisher.example` (channel context, name, topology, notification
   channel, `Down`/`Up`/`Recovering` and `NotifyPublish`/`NotifyReturn` callbacks), gated by
   `AwaitStatus(true, 30*time.Second)`; on timeout it cancels the context, waits 7 s, logs `EXIT`,
   and exits early.
5. `PublishMsgOneByOne(publisher, 7)` — publishes 7 messages
   (`one-by-one test number %04d`), awaiting each confirmation immediately
   (`AwaitDeferredConfirmation(conf, CONF_DELAY)` with `CONF_DELAY = 7 * time.Second`) and logging
   the outcome.
6. `PublishMsgBulk(publisher, 5)` — publishes 5 messages (`bulk test number %04d`) collecting their
   `*DeferredConfirmation`s, then awaits them in publish order (code comment: "tests that
   deliveries are received orderly").
7. On `SIGINT`/`SIGTERM`: the deferred cleanup closes the publisher, then deliberately calls
   `PublishMsgOneByOne(publisher, 5)` again — all 5 attempts must fail because the channel is
   already closed — logs a separator, closes the connection, waits 3 s, logs `EXIT`.

Confirmation outcomes are distinguished by `switch` on `conf.Outcome`:
`ConfirmationPrevious` (red — a lower sequence number than awaited, "keep waiting"),
`ConfirmationDisabled` (channel not in confirm mode), `ConfirmationACK` (green),
`ConfirmationNAK` (red); anything else — `ConfirmationTimeOut` (the 7 s timer expired) or
`ConfirmationClosed` (the publisher context died) — falls to the yellow `default` branch. Log lines
embed ANSI color escapes and render the request sequence vs. broker delivery tag in hex
(`[%04X]`).

## Interface

| Symbol | Signature | Role |
| --- | --- | --- |
| `main` | `func main()` | Demo entrypoint. No CLI flags, no arguments, no environment variables are read. |
| `OnPubDown` | `func(name string, err grabbit.OptionalError) bool` | Satisfies `grabbit.CallbackWhenDown`; returns `true` → keep retrying recovery. |
| `OnPubUp` | `func(name string)` | Satisfies `grabbit.CallbackWhenUp`. |
| `OnPubReattempting` | `func(name string, retry int) bool` | Satisfies `grabbit.CallbackWhenRecovering`; returns `true` → keep retrying. |
| `OnNotifyPublish` | `func(confirmation amqp.Confirmation, ch *grabbit.Channel)` | Satisfies `grabbit.CallbackNotifyPublish`. |
| `OnNotifyReturn` | `func(confirm amqp.Return, ch *grabbit.Channel)` | Satisfies `grabbit.CallbackNotifyReturn`. |
| `PublishMsgOneByOne` | `func(pub *grabbit.Publisher, records int)` | Publish, then await each confirmation individually. |
| `PublishMsgBulk` | `func(pub *grabbit.Publisher, records int)` | Publish all, then await confirmations in publish order. |
| `CONF_DELAY` | `const CONF_DELAY = 7 * time.Second` | Per-confirmation await timeout. |

**Module wiring:** part of the root module `github.com/LucaWolf/grabbit`; import path
`github.com/LucaWolf/grabbit/examples/pub_deferred`. No separate `go.mod`, `go.sum`, or per-example
`.gitignore`; dependencies come from the root module.

**grabbit API exercised** (identifiers verified against the library source):

- `NewConnection("amqp://guest:guest@localhost", amqp.Config{}, WithConnectionOptionContext(ctxMaster), WithConnectionOptionName("conn.main"))`
- `DefaultPublisherOptions()` → `(*PublisherOptions).WithKey("workload")`, `.WithContext(ctxMaster)`, `.WithConfirmationsCount(20)`
- `NewPublisher(conn, pubOpt, WithChannelOptionContext(ctxMaster), WithChannelOptionName("chan.publisher.example"), WithChannelOptionTopology(topos), WithChannelOptionNotification(pubStatusChan), WithChannelOptionDown(OnPubDown), WithChannelOptionUp(OnPubUp), WithChannelOptionRecovering(OnPubReattempting), WithChannelOptionNotifyPublish(OnNotifyPublish), WithChannelOptionNotifyReturn(OnNotifyReturn))`
- `Publisher.AwaitStatus(true, 30*time.Second) bool`
- `Publisher.PublishDeferredConfirm(amqp.Publishing) (*DeferredConfirmation, error)` — returns `nil, amqp.ErrClosed` when the channel is already closed
- `Publisher.AwaitDeferredConfirmation(d *DeferredConfirmation, tmr time.Duration) *DeferredConfirmation` — sets `d.Outcome` to exactly one of:
  `ConfirmationTimeOut` (timer expired), `ConfirmationClosed` (publisher context done),
  `ConfirmationDisabled` (`d.DeferredConfirmation == nil`, i.e. channel not in confirm mode),
  `ConfirmationPrevious` (`RequestSequence > DeliveryTag` — lower sequence than expected),
  `ConfirmationACK` (`d.Acked()`), `ConfirmationNAK` (otherwise)
- `DeferredConfirmation` fields read by the demo: `Outcome` (`ConfirmationOutcome`), `RequestSequence`
  (`uint64`), `ChannelName`, `Queue`, and the embedded `DeliveryTag` from `*amqp.DeferredConfirmation`
- `Publisher.Close() error`, `Connection.Close() error`
- Notifications: buffered `chan grabbit.Event` (capacity 32)

## Invariants

1. **Live broker required** at exactly `amqp://guest:guest@localhost` with `guest`/`guest`
   credentials (hardcoded).
2. **Confirm mode requires `ConfirmationCount > 0`:** the demo sets 20 (library default is 10; a
   zero value *disables* the confirmation mechanism per `WithConfirmationsCount`). Without confirm
   mode, every `AwaitDeferredConfirmation` returns `ConfirmationDisabled`.
3. **Await bounds:** each confirmation gets at most `CONF_DELAY = 7 s`; cancelling `ctxMaster`
   aborts awaits with `ConfirmationClosed` (the publisher's `Context` is `ctxMaster` via
   `.WithContext`).
4. **Readiness gate:** if the publisher is not ready within 30 s, the program cancels the context,
   waits 7 s, logs `EXIT`, and returns early (the publish phases never run).
5. **Post-close publishes must fail:** after `publisher.Close()`, `PublishDeferredConfirm`
   short-circuits with `amqp.ErrClosed` (verified in `publisher.go`); the 5 post-close attempts
   therefore log `publishing failed with: ` and never reach the broker.
6. **Teardown is explicit, not context-driven** (contrast with the `consumer` demo): the happy-path
   cleanup calls `publisher.Close()` and `conn.Close()`; `ctxCancel()` is only invoked on the
   early-exit path.
7. **Durable queue `workload`** is declared via topology; `IsDestination` makes `ch.Queue()` return
   `workload`, which is why confirmation lines read `[chan.publisher.example][workload]`.
8. **Orderly bulk confirms:** a confirm-mode broker returns confirmations in publish order; the
   demo awaits them in that order and treats any lower-than-expected sequence as
   `ConfirmationPrevious`.
9. **Events can be silently dropped:** the notification channel has capacity 32; `Event.raise`
   discards events when the buffer is full.

## Side Effects

- **Network:** one managed AMQP connection to `amqp://guest:guest@localhost` plus one publisher
  channel; declares the durable queue `workload`.
- **Messages:** publishes **12 messages** in total before shutdown — bodies
  `one-by-one test number 0000`…`0006` and `bulk test number 0000`…`0004`, set as the `Body` of a
  zero-value `amqp.Publishing{}` (no persistence flags or headers) — to the default exchange with
  routing key `workload`. The 5 post-close attempts are rejected locally (`amqp.ErrClosed`) and
  never reach the broker.
- **Goroutines:** notification-drain goroutine (its channel is never closed — it lives until
  process exit), signal watcher, plus library-internal manager/recovery goroutines.
- **Signals:** installs `SIGINT`/`SIGTERM` handlers; the first signal triggers the deferred cleanup.
- **stderr via `log`, with ANSI color escapes** (`\033[91m` red, `\033[92m` green, `\033[93m`
  yellow). Exact format strings from `main.go`:
  - `callback_down: {%s} went down with {%s}` / `callback_up: {%s} went up` /
    `callback_redo: {%s} retry count {%d}`
  - `callback: post [%04d] confirmed status [%v] from [%s][%s]` /
    `callback: publish returned from queue [%s]`
  - outcome lines, e.g. `[chan.publisher.example][workload] \033[92m<Outcome>\033[0m with request [0001] vs. response [0001]`
    (green `ConfirmationACK` / red `ConfirmationNAK` / yellow `default`) — `<Outcome>` is the
    stringified `conf.Outcome`, e.g. request `[0001]` vs. response `[0001]` for the first message
  - `\033[91m previous \033[0m message confirmed request [%04X] vs.response [%04X]. TODO: keep waiting.` — Previous branch, one-by-one variant; the bulk variant prints `\033[91mprevious\033[0m …` without the surrounding spaces
  - `Not in confirmation mode (no DeferredConfirmation available).` (Disabled branch)
  - `publisher.notification: ` / `=========================================` separators /
    `publisher not ready yet` / `app closing connection and dependencies` /
    `cannot close publisher: ` / `cannot close conn: ` /
    `done: 12 messages confirmed (7 one-by-one, 5 bulk); awaiting SIGINT/SIGTERM to shut down` / `EXIT`

## Verification

This package contains **no test files** (`*_test.go` count: 0), so the check is compile-only:

```sh
go build ./examples/pub_deferred
go test ./examples/pub_deferred   # compiles the demo package; runs no tests
```

**Actually running the demo requires a live RabbitMQ broker** reachable at
`amqp://guest:guest@localhost`:

```sh
go run ./examples/pub_deferred
```

Expected: 7 one-by-one confirmations, then 5 bulk confirmations (green `ACK` outcome lines against
a healthy broker), then `done: 12 messages confirmed (7 one-by-one, 5 bulk); awaiting
SIGINT/SIGTERM to shut down`; `Ctrl-C` triggers the close sequence (5 expected
post-close publish failures) and finally `EXIT`.
