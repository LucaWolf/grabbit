# `examples/connection` — Component Package Contract

> **Classification:** Component Package (Peanut) — leaf `package main` demo within the root module.
> No subdirectories, no test files, no exported API: this package is minimal reference material
> for humans showing how to create and supervise a managed grabbit `Connection`; it is not
> imported by any other package.

## Overview

The smallest grabbit demo (`main.go`, `package main`): create one managed auto-reconnecting
RabbitMQ connection ("conn.main"), observe its lifecycle through a notification-event channel
and Down/Up/Recovering callbacks, then close it cleanly when SIGINT/SIGTERM arrives. No
channels, no topology, no messages — this is the connection-lifecycle baseline that the other
examples build upon.

Module facts:

- Part of the root module `github.com/LucaWolf/grabbit`; import path
  `github.com/LucaWolf/grabbit/examples/connection`.
- No separate `go.mod`, `go.sum`, or per-example `.gitignore`; dependencies come from the
  root module (Go 1.25.7, `amqp091-go` v1.10.0, `go-notifying-latch` v1.1.0).

## Interface

**Entrypoint:** `main()` in `main.go`. No CLI flags and no environment variables — all
configuration is hardcoded:

| Item | Value |
| --- | --- |
| Broker address | `amqp://guest:guest@localhost` (guest/guest, default vhost `/`), `amqp.Config{}` |
| Connection name | `conn.main` (package-level `ConnectionName` var) |
| Connection notification channel | buffered `chan grabbit.Event`, capacity 32 |
| Shutdown signals | `SIGINT`, `SIGTERM` |

**grabbit API surface called by this demo:**

- `grabbit.NewConnection(address, amqp.Config, ...opts)`
- Connection options: `WithConnectionOptionName`, `WithConnectionOptionNotification`,
  `WithConnectionOptionDown`, `WithConnectionOptionUp`, `WithConnectionOptionRecovering`
- `grabbit.Event` (logged via `log.Print` — processed by its `.SourceType` / `.SourceName` /
  `.Kind` combination per the in-code comment)
- `(*Connection).Close()`

**Program flow:**

1. Creates the connection event channel (cap 32) and a drain goroutine that `log`s every
   event received (the loop is the template for application-specific reactions).
2. Creates the managed connection with the three lifecycle callbacks:
   - `Down(name, err) bool` — logs the failure, returns `true` (keep reconnecting).
   - `Up(name)` — logs the (re-)establishment.
   - `Reattempting(name, retry) bool` — logs the retry counter, returns `true`.
3. Deferred shutdown (runs when `main` returns): prints `app closing connection and
   dependencies`; calls `conn.Close()` (logging any error); waits 15 s — the comment notes the
   reconnect loop should be dead by then.
4. Blocks awaiting `SIGINT`/`SIGTERM`; on signal, prints the signal and returns, letting the
   deferred shutdown run.

## Invariants

- **Connection lifecycle is `Close()`-only:** no context option is passed, so the connection
  defaults to `context.Background()` (grabbit `connection.go:72`); its reconnect loop is
  terminated by `conn.Close()` (idempotent in grabbit), not by context cancellation.
- **Reconnection is endless while callbacks say so:** `Down` and `Reattempting` both return
  `true`; grabbit aborts a reconnect loop only when such a callback returns `false`
  (`callbackAllowedRecovery`, `general_options.go:157-159`; `callbackAllowedDown`,
  `general_options.go:165-167`). Backoff uses grabbit's default exponential delayer:
  200 ms base, doubling, retry cap 7 (clamped to 7–10), +10% jitter
  (`general_options.go:25-52`).
- **Every lifecycle event is mirrored twice:** once on the notification channel (drained and
  logged by the goroutine) and once via the matching callback — the demo shows both hooks.
- **Notification events are lossy under load:** the event channel is buffered (32) and
  grabbit's `raise` drops events when the buffer is full (`events.go:62-67`); the demo drains
  it continuously to minimize drops.
- **Shutdown leaves a 15-second tail:** the deferred cleanup deliberately sleeps 15 s after
  `Close()` so the grabbit reconnect loop finishes dying before the process exits.

## Side Effects

**External systems (requires live RabbitMQ broker):**

- Opens a TCP/AMQP connection to `localhost` (named `conn.main` on the server). No topology is
  declared and no messages are published or consumed. Without a reachable broker the program
  still runs, but only emits down/reattempting logs while retrying forever.

**Process effects:**

- Goroutines: one notification-drain loop, one signal-handler goroutine, plus grabbit's
  internal connection reconnect loop. The drain loop runs until process exit (the event
  channel is never closed).
- Signal handling: `SIGINT` / `SIGTERM` terminate the program.
- Output: stdout prints `done: managed connection is up; awaiting SIGINT/SIGTERM to shut down`,
  the received signal, and `app closing connection and dependencies`; the `log` package (stderr) emits every
  notification event and the three callbacks' messages.
- Writes no files; no HTTP or other I/O.

## Verification

- **Compile-only check:** from the repository root run `go build ./examples/connection` and
  `go test ./examples/connection`; there are no test files, so the test command only verifies
  compilation.
- `make examples` builds every demo into `bin/`.
- **Running the demo:** `go run ./examples/connection` requires a live RabbitMQ broker at
  `amqp://guest:guest@localhost` with guest/guest credentials. Start it, watch the up/down
  notifications (e.g. stop/start the broker to exercise reconnecting), then Ctrl-C to exercise
  the close path.
- **Dependencies:** inherited from the root `go.mod`; no per-example `go mod tidy` is needed.
