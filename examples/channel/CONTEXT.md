# `examples/channel` — Component Package Contract

> **Classification:** Component Package (Peanut) — leaf, standalone executable demo module.
> No subdirectories, no test files, no exported API: this module is reference material for
> humans showing how to drive a managed grabbit `Channel`; it is not imported by any other package.

## Overview

A single-file demo (`main.go`, `package main`) of the **lower-level channel lifecycle** of the
grabbit library: it creates one managed auto-reconnecting RabbitMQ connection ("conn.main") and
one managed data channel ("chan.data"), wires up both notification-event channels and
up/down/recovering lifecycle callbacks, (re-)declares a demo queue from inside the channel's
event handler every time the channel comes up, and shuts everything down gracefully on
SIGINT/SIGTERM.

Module facts (from `go.mod` / `go.sum` / `.gitignore`):

- Own Go module: `module channel`, `go 1.21` — this directory is **not** part of the root
  module's package graph; the root module's `go test ./...` does not compile it.
- Requires `github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` with
  `replace github.com/LucaWolf/grabbit => ../../../grabbit` — the demo compiles against the
  **local working-tree copy** of the library, not the published version.
- Requires `github.com/rabbitmq/amqp091-go v1.9.0`.
- `.gitignore` contains `channel` (the compiled binary produced by `go build` in this directory).

## Interface

**Entrypoint:** `main()` in `main.go`. No CLI flags and no environment variables — all
configuration is hardcoded:

| Item | Value |
| --- | --- |
| Broker address | `amqp://guest:guest@localhost` (guest/guest, default vhost `/`), `amqp.Config{}` |
| Connection name | `conn.main` |
| Connection notification channel | buffered `chan grabbit.Event`, capacity 32 |
| Channel name | `chan.data` |
| Channel notification channel | buffered `chan grabbit.Event`, capacity 5 |
| Channel context | cancelable child of `context.TODO()` (`ctxData` / `ctxDataCancel`) |
| Queue declared on channel-up | `grabbit_demo_data` — durable `false`, autoDelete `true`, exclusive `false`, noWait `false`, empty `amqp.Table` |
| Shutdown signals | `SIGINT`, `SIGTERM` |

**grabbit API surface called by this demo:**

- `grabbit.NewConnection(address, amqp.Config, ...opts)`
- `grabbit.NewChannel(conn, ...opts)`
- Connection options: `WithConnectionOptionName`, `WithConnectionOptionNotification`,
  `WithConnectionOptionDown`, `WithConnectionOptionUp`, `WithConnectionOptionRecovering`
- Channel options: `WithChannelOptionContext`, `WithChannelOptionName`,
  `WithChannelOptionNotification`, `WithChannelOptionDown`, `WithChannelOptionUp`,
  `WithChannelOptionRecovering`
- Event types: `grabbit.Event` (fields `SourceType`, `SourceName`, `TargetName`,
  `Kind`, `Err`) and `grabbit.EventUp`
- Channel methods: `(*Channel).QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)`,
  `(*Channel).Close()`; connection method `(*Connection).Close()`
- Mentioned in comments but **not** exercised here: `WithChannelOptionDelay`,
  `WithChannelOptionNotifyPublish`, `WithChannelOptionNotifyReturn`,
  `WithChannelOptionProcessor`, `WithChannelOptionTopology`, `WithChannelOptionUsageParams`

**Program flow:**

1. Creates the connection event channel (cap 32) and a drain goroutine; the received events
   are deliberately ignored (`_ = event`) — the loop is a placeholder for application logic
   (comment: react to `.SourceType` / `.SourceName` / `.Kind` combinations).
2. Creates the managed connection with the three lifecycle callbacks
   (`ConnDown`/`ConnReattempting` silent and returning `true`; `ConnUp` is silent — the
   `CallbackWhenUp` type has no return value).
3. Creates the channel event channel (cap 5), a cancelable `ctxData`, and the managed channel
   with `WithChannelOptionContext(ctxData)` and three logging callbacks
   (`DataChanDown`/`DataChanReattempting` returning `true`; `DataChanUp` has no return value).
4. A goroutine ranges over the channel events; on every `grabbit.EventUp` it calls
   `dataCh.QueueDeclare("grabbit_demo_data", false, true, false, false, make(amqp.Table))`
   and logs the resulting pending-message count. The adjacent comment warns that an erroring
   `QueueDeclarePassive` "throws and kills your channel" — hence the active declare here.
5. Deferred shutdown (runs when `main` returns): prints `app closing connection and
   dependencies`; calls `dataCh.Close()` **and** `ctxDataCancel()` (demo comment: either/or
   suffices, both are safe to call multiple times, and context cancellation pushes no
   `EventDown`/`EventClosed`); waits 3 s; calls `conn.Close()` (logging any error); waits 3 s.
6. Blocks awaiting `SIGINT`/`SIGTERM`; on signal, prints the signal and returns, letting the
   deferred shutdown run.

## Invariants

- **Connection lifecycle is `Close()`-only:** the connection gets no context option, so it
  defaults to `context.Background()` (grabbit `connection.go:72`); its reconnect loop can only
  be terminated by `conn.Close()` in the deferred cleanup.
- **Channel lifecycle is either/or:** the channel is bound to `ctxData`; cancelling it or
  calling `dataCh.Close()` each terminate the channel (both done here; the demo comment notes
  both are idempotent). Grabbit derives each channel's cancelable context from its
  connection's context unless overridden (`channel.go:81,86`) — this demo overrides it.
- **Queue re-declared on every channel-up:** the declare lives in the `EventUp` branch of the
  notification goroutine, so `grabbit_demo_data` is (re-)declared on the initial connect *and*
  after every channel recovery. This is the manual-topology pattern; it does **not** use
  `WithChannelOptionTopology`.
- **Reconnection is endless while callbacks say so:** every `CallbackWhenDown` /
  `CallbackWhenRecovering` returns `true` (grabbit honors `false` to abort via
  `callbackAllowedRecovery`, `general_options.go:157-159`, and `callbackAllowedDown`,
  `general_options.go:165-167`). Backoff uses grabbit's default exponential delayer: 200 ms base, doubling,
  retry cap 7 (clamped to 7–10), +10% jitter (`general_options.go:25-52`).
- **Notification events are lossy under load:** event channels are buffered (32 / 5) and
  grabbit's `raise` drops events when the buffer is full (`events.go:62-67`); the demo drains
  both channels continuously to minimize drops.
- **Demo requires a reachable broker to be meaningful:** without a RabbitMQ at
  `amqp://guest:guest@localhost` the program still runs, but stays in the reconnect loop
  (logging down/reattempting callbacks) and never declares the queue.

## Side Effects

**External systems (requires live RabbitMQ broker):**

- Opens a TCP/AMQP connection to `localhost` (named `conn.main` on the server).
- (Re-)declares the queue `grabbit_demo_data` (non-durable, auto-delete) on every channel-up;
  nothing is published or consumed, no exchanges or bindings are touched.

**Process effects:**

- Goroutines: two notification-drain loops (connection + channel), one signal-handler goroutine,
  plus grabbit-internal connection reconnect and channel reconnect/manage goroutines. The two
  drain loops run until process exit (the event channels are never closed).
- Signal handling: `SIGINT` / `SIGTERM` terminate the program.
- Output: stdout prints `awaiting signal`, the received signal, and
  `app closing connection and dependencies`; the `log` package (stderr) emits channel
  callbacks, `cmd.notification:` events, and the queue pending count.
- Writes no files; no HTTP or other I/O.

## Verification

- **Compile-only check:** `go test ./...` — there are no test files in this module, so the
  command only verifies that the package compiles (`ok channel [no test files]`). Run it
  **inside this directory**; being a standalone module, it is not covered by the root module's
  `go test ./...`.
- `go build ./...` compiles the `channel` binary (ignored via `.gitignore`).
- **Running the demo:** `go run .` requires a live RabbitMQ broker at
  `amqp://guest:guest@localhost` with guest/guest credentials. Start it, watch the queue
  declaration on channel-up, interrupt with Ctrl-C to trigger the deferred shutdown path.
- **Dependency caveat:** the `replace` directive resolves grabbit to the local working tree,
  whose `go.mod` currently requires `amqp091-go v1.10.0` and `go-notifying-latch v1.1.0`,
  while this module's `go.sum` pins `amqp091-go v1.9.0` and lacks `go-notifying-latch`
  entirely. If the compile check reports missing `go.sum` entries, run `go mod tidy` here first.
  The replaced root module's `go 1.25.7` directive also means a Go toolchain ≥ 1.25.7 (or
  GOTOOLCHAIN auto-switching) is required, even though this module declares `go 1.21`.
