# `examples/contexts` — Component Package (Peanut)

> EGM contract for `/home/luci/projects/go/grabbit/examples/contexts`.
> **Classification:** Component Package (Peanut) — standalone leaf demo module; nothing imports it.
> **Files:** `.gitignore`, `go.mod`, `go.sum`, `main.go` (the only source file). No `*_test.go` files exist.
> **Generated:** 2026-11-22 by the EGM bootstrap worker. Parent blueprint: [`../CONTEXT.md`](../CONTEXT.md).

## Overview

A self-contained demo module (module path `contexts`, `go 1.21`) whose single `package main`
program demonstrates **context-tree control of channel lifecycles** in grabbit, a managed,
auto-reconnecting RabbitMQ client. It opens one connection (`conn.main`) and two managed channels
— `chan.alpha` bound to `ctxAlpha`, and `chan.beta` bound to `ctxBeta`, where `ctxBeta` is a
**child of** `ctxAlpha` — then asserts two properties:

1. Closing `alphaCh` must **not** close `betaCh`. Per the code comments, each `Channel.Close()`
   cancels only that channel's own derived inner context (a sibling of `ctxBeta` under `ctxAlpha`),
   so channels bound to sibling or child contexts survive.
2. Cancelling the parent `ctxAlpha` **must** close `chan.beta` — descendant contexts die with
   their parent.

Each assertion is enforced with `log.Fatal` (immediate process exit, code 1) on violation, and
pass/fail is printed as numbered status lines. Because a context-driven ("sudden") death emits no
`EventClosed` notification (WARN comment in `main.go`), status changes are detected by polling
`Channel.IsClosed()` through the local helper `AwaitDown`. The program terminates by itself — no
signal handling, no long-running loop.

## Interface

| Symbol | Signature | Role |
| --- | --- | --- |
| `main` | `func main()` | Demo entrypoint. No CLI flags, no arguments, no environment variables are read. |
| `AwaitDown` | `func(ch *grabbit.Channel, timeout, pollFreq time.Duration) bool` | Polls `ch.IsClosed()` every `pollFreq` until `timeout`; returns `true` iff closure was observed in time. Zero arguments default to `timeout = 7500ms`, `pollFreq = 330ms`. |

**Module wiring** (`go.mod`): module path `contexts`, `go 1.21`; requires
`github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34` and
`github.com/rabbitmq/amqp091-go v1.9.0`;
`replace github.com/LucaWolf/grabbit => ../../../grabbit` pins the demo to the **local repo
checkout**. `.gitignore` ignores the compiled binary `contexts`.

**grabbit API exercised** (identifiers verified against the library source):

- `NewConnection("amqp://guest:guest@localhost", amqp.Config{}, WithConnectionOptionName("conn.main"))`
  — note: **no connection context and no connection notification channel** are given; the demo is
  entirely about *channel* contexts.
- `NewChannel(conn, WithChannelOptionContext(ctxAlpha | ctxBeta), WithChannelOptionName("chan.alpha" | "chan.beta"), WithChannelOptionNotification(alphaStatusChan | betaStatusChan))`
- `context.WithCancel(context.TODO())` → `ctxAlpha`; `context.WithCancel(ctxAlpha)` → `ctxBeta`
  (child of the former).
- `Channel.Close() error` — closes the base AMQP channel, resets it, and cancels the channel's
  derived inner context (`ch.opt.cancelCtx()`); documented as idempotent.
- `Channel.IsClosed() bool`
- Notifications: two buffered `chan grabbit.Event` (capacity 5 each); the demo reads
  `event.Kind` and compares against `grabbit.EventUp` — the first `EventUp` per channel releases
  the scenario (`close(chSignalAlphaUp)` / `close(chSignalBetaUp)`).

## Invariants

1. **Live broker required** at exactly `amqp://guest:guest@localhost` with `guest`/`guest`
   credentials (hardcoded).
2. **Unbounded startup wait:** the scenario blocks until both channels have emitted their first
   `EventUp`. With no reachable broker, recovery keeps retrying and the program stays blocked on
   `<-chSignalAlphaUp` / `<-chSignalBetaUp`.
3. **No `EventClosed` on context death** (per the demo's WARN comment) — closure via context
   cancellation must be observed by polling `IsClosed()`; that is why `AwaitDown` exists.
4. **Assertion 1 — sibling isolation:** after a hardcoded 1 s settling wait
   (`<-time.After(1 * time.Second)`), `alphaCh.Close()` is called; `betaCh` must still be
   open within 7 s (`AwaitDown(betaCh, 7*time.Second, 250*time.Millisecond)` returning
   `true` — i.e. closure observed — triggers `log.Fatal`).
5. **Assertion 2 — parent cancellation:** after `ctxAlphaCancel()`, `betaCh` must close within
   7 s (failure to observe closure triggers `log.Fatal`).
6. **Post-close `Close()` is allowed:** the demo calls `betaCh.Close()` after context-driven
   closure and logs any returned error as expected (`Expected: cannot close chan.beta`) —
   `Close` is idempotent per the library contract.
7. **No AMQP topology:** no queue or exchange is declared, and no message is published or
   consumed; effects are limited to connection/channel lifecycle.
8. **The connection itself is never closed** and receives no context option — teardown of the
   connection is left to process exit.
9. **Redundant shutdowns are exercised deliberately:** `ctxBetaCancel()` is called at the end even
   though `ctxBeta` is already dead ("test we can call shutdowns willy-nilly").

## Side Effects

- **Network:** one managed AMQP connection to `amqp://guest:guest@localhost` and two channels
  (`chan.alpha`, `chan.beta`); no topology, no messages.
- **Goroutines:** two notification-drain goroutines (`for event := range alphaStatusChan |
  betaStatusChan`) that are never terminated — their channels are never closed, so they live until
  process exit; one short-lived timeout goroutine per `AwaitDown` call (`time.Sleep(timeout)` then
  `close(done)`, which can outlive the poll loop by up to the timeout); library manager/recovery
  goroutines.
- **stderr via `log`** (exact strings from `main.go`):
  - `beta.notification: ` / `alfa.notification: ` (note: the alpha drain misspells it `alfa`)
  - `1. chan.beta status test: pass|failed` — `pass` iff `betaCh` survived `alphaCh.Close()`
  - `2. chan.beta status test: pass|failed` — `pass` iff `betaCh.IsClosed()` after `ctxAlphaCancel()`
  - `Expected: cannot close chan.beta` + error, when the post-cancel `Close()` returns an error
  - `...looking good!` (final success line)
  - fatal (exit code 1): `Error: closing alphaCh should not have affected betaCh` /
    `Error: cancelling ctxAlpha should have closed betaCh`
- **Process exit:** normal completion returns exit code 0; any failed assertion exits immediately
  with code 1 via `log.Fatal`.

## Verification

This module contains **no test files** (`*_test.go` count: 0), so the check is compile-only:

```sh
cd /home/luci/projects/go/grabbit/examples/contexts
go test ./...   # compiles the demo package; runs no tests
```

**Build prerequisite (verified statically against the current tree):** this module's pins predate
the current library checkout. Its `go.mod` requires `amqp091-go v1.9.0` while the replaced root
module now requires `v1.10.0` (module resolution selects the higher version), and its `go.sum`
has no entry for `go-notifying-latch v1.1.0`, which the library's non-test code now imports
(`connection.go`, `channel.go`, `utils.go`). Under Go's default read-only module mode,
`go test ./...` / `go build ./...` may therefore fail with *missing `go.sum` entry* errors until
`go mod tidy` (or `go mod download`) is run in this directory. The replaced module's `go 1.25.7`
directive also means a Go toolchain ≥ 1.25.7 is required, even though this module declares
`go 1.21`.

**Actually running the demo requires a live RabbitMQ broker** reachable at
`amqp://guest:guest@localhost`:

```sh
go run .
```

The scenario runs for a few seconds and exits by itself (no `Ctrl-C` needed). A successful run
prints `1. chan.beta status test: pass`, then `2. chan.beta status test: pass`, then
`...looking good!`. A failing assertion aborts immediately with the corresponding
`Error: …` line and exit code 1.
