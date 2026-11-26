# `internal/traceutils` — Component Package Contract

**Module:** `github.com/LucaWolf/grabbit` (Go 1.25.7)
**Import path:** `github.com/LucaWolf/grabbit/internal/traceutils` (package `traceutils`, imported by the root package as `trace`)

| File | Build tag | Role |
|---|---|---|
| `trace_handler.go` | none (always compiled) | Event model, validator types, channel-key constants, consumer harness `ConsumeTracesContext` |
| `trace_parameters.go` | none (always compiled) | The 17 `Params*` payload structs |
| `trace_params_nop.go` | `//go:build !test_env` | The 23 trace functions as empty no-op bodies (production build) |
| `trace_params_testing.go` | `//go:build test_env` | The 23 trace functions as channel emitters (test build) |

## Overview

`traceutils` lets tests assert **exactly which parameters grabbit passes to each AMQP
channel operation** without a real broker. The root package (`channel.go`,
`channel_safewrappers.go`) calls one trace function next to every wrapped AMQP
operation, always passing its `ctx`. Behavior is selected at compile time by the
`test_env` build tag:

- **`test_env` active** (CI runs `go test -tags test_env`): each trace function builds a
  `ParamsTrace{Name, Tag, Value}` and sends it on the channel found in the context under
  key `TraceChannelParamsName`.
- **`test_env` inactive** (default, production): the same functions are empty bodies —
  no channel operations, no allocations, no observable effect.

Tests such as `consumer_test.go` and `publisher_options_test.go` call
`ConsumeTracesContext(validators)`, which installs the channels into a context and
starts a goroutine that checks every received trace against `TraceValidator`
expectations and reports failures as `ErrorTrace` on a results channel.

Because the package lives under `internal/`, none of this is part of grabbit's public
API (Go toolchain rule: importable only from within `github.com/LucaWolf/grabbit`).

## Interface

### Context channel keys

```go
type TraceChannelName string

const (
    TraceChannelParamsName  TraceChannelName = "params_channel"  // holds chan ParamsTrace
    TraceChannelResultsName TraceChannelName = "results_channel" // holds chan ErrorTrace
)
```

### Event and validation model (`trace_handler.go`, always compiled)

```go
type ParamsTrace struct {
    Name  string // kind of params, e.g. "QueueDeclare"
    Tag   string // instance of same params, e.g. queue/exchange name; may be ""
    Value any    // one of the Params* structs below
}
func (p ParamsTrace) String() string // p.Name + "." + p.Tag (empty Tag yields a trailing dot)

type ErrorTrace struct {
    Name string
    Tag  string
    Err  error
}

type FieldExpectation struct {
    Field string          // field name within ParamsTrace.Value to test
    Value reflect.Value  // expected value
}

type TraceValidator struct {
    Name         string
    Tag          string
    Expectations []FieldExpectation
}
```

The single exported harness entry point:

```go
// Starts the consumer goroutine itself — call directly from tests, without a 'go' wrapper.
func ConsumeTracesContext(validators []TraceValidator) (context.Context, context.CancelFunc)
```

It creates `chan ParamsTrace` (buffer 32) and `chan ErrorTrace` (buffer 32), stores both
in a fresh `context.Background()` under the two keys above, wraps it with
`context.WithCancel`, launches the unexported `doConsumeTraces` goroutine, and returns
the cancelable context plus its `CancelFunc`.

### Payload structs (`trace_parameters.go`, always compiled — 17 total)

| Struct | Fields |
|---|---|
| `ParamsConsume` | `Queue`, `Consumer string`; `AutoAck`, `Exclusive`, `NoLocal`, `NoWait bool`; `Args amqp.Table` |
| `ParamsQoS` | `PrefetchCount`, `PrefetchSize int`; `Global bool` |
| `ParamsExchangeDeclare` | `Name`, `Kind string`; `Durable`, `AutoDelete`, `Internal`, `NoWait bool`; `Args amqp.Table` |
| `ParamsQueueDeclare` | `Name string`; `Durable`, `AutoDelete`, `Exclusive`, `NoWait bool`; `Args amqp.Table` |
| `ParamsQueueBind` | `Queue`, `Key`, `Exchange string`; `NoWait bool`; `Args amqp.Table` |
| `ParamsExchangeBind` | `Destination`, `Key`, `Source string`; `NoWait bool`; `Args amqp.Table` |
| `ParamsPublishWithContext` | `Exchange`, `Key string`; `Mandatory`, `Immediate bool`; `Msg amqp.Publishing` |
| `ParamsQueuePurge` | `Queue string`; `NoWait bool` |
| `ParamsQueueDelete` | `Queue string`; `IfUnused`, `IfEmpty`, `NoWait bool` |
| `ParamsExchangeDelete` | `Exchange string`; `IfUnused`, `NoWait bool` |
| `ParamsTx`, `ParamsTxCommit`, `ParamsTxRollback` | (empty structs) |
| `ParamsFlow` | `Active bool` |
| `ParamsConfirm` | `NoWait bool` |
| `ParamsRecover` | `Requeue bool` |
| `ParamsGet` | `Queue string`; `AutoAck bool` |

### Trace functions (23; identical signatures in both build modes, behavior differs)

Every function takes `ctx context.Context` first, then mirrors the parameters of the
corresponding `amqp091-go` channel method:

```go
func Confirm(ctx context.Context, noWait bool)
func Consume(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table)
func ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table)
func ExchangeBind(ctx context.Context, destination, key, source string, noWait bool, args amqp.Table)
func ExchangeDeclare(ctx context.Context, name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table)
func ExchangeDelete(ctx context.Context, name string, ifUnused, noWait bool)
func Flow(ctx context.Context, active bool)
func Get(ctx context.Context, queue string, autoAck bool)
func Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing)
func PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing)
func PublishWithDeferredConfirm(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing)
func PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing)
func Qos(ctx context.Context, prefetchCount, prefetchSize int, global bool)
func QueueBind(ctx context.Context, queue, key, exchange string, noWait bool, args amqp.Table)
func QueueDeclare(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table)
func QueueDeclarePassive(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table)
func QueueDelete(ctx context.Context, name string, ifUnused, ifEmpty, noWait bool)
func QueuePurge(ctx context.Context, name string, noWait bool)
func QueueUnbind(ctx context.Context, name, key, exchange string, args amqp.Table)
func Recover(ctx context.Context, requeue bool)
func Tx(ctx context.Context)
func TxCommit(ctx context.Context)
func TxRollback(ctx context.Context)
```

Emitted `ParamsTrace` per function (verified from `trace_params_testing.go`; validators
must match on these exact `Name` and `Tag` values):

| Function | `Name` | `Tag` | `Value` struct |
|---|---|---|---|
| `Confirm` | `Confirm` | *(empty)* | `ParamsConfirm` |
| `Consume` | `Consume` | `consumer` | `ParamsConsume` |
| `ConsumeWithContext` | `ConsumeWithContext` | `consumer` | `ParamsConsume` *(reused)* |
| `ExchangeBind` | `ExchangeBind` | `destination` | `ParamsExchangeBind` |
| `ExchangeDeclare` | `ExchangeDeclare` | `name` | `ParamsExchangeDeclare` |
| `ExchangeDelete` | `ExchangeDelete` | `name` | `ParamsExchangeDelete` |
| `Flow` | `Flow` | *(empty)* | `ParamsFlow` |
| `Get` | `Get` | `queue` | `ParamsGet` |
| `Publish` | `Publish` | `exchange + "." + key` | `ParamsPublishWithContext` |
| `PublishWithContext` | `PublishWithContext` | `key` *(no exchange prefix)* | `ParamsPublishWithContext` |
| `PublishWithDeferredConfirm` | `PublishWithDeferredConfirm` | `exchange + "." + key` | `ParamsPublishWithContext` |
| `PublishWithDeferredConfirmWithContext` | `PublishWithDeferredConfirmWithContext` | `key` *(no exchange prefix)* | `ParamsPublishWithContext` |
| `Qos` | `Qos` | *(empty)* | `ParamsQoS` |
| `QueueBind` | `QueueBind` | `queue` | `ParamsQueueBind` |
| `QueueDeclare` | `QueueDeclare` | `name` | `ParamsQueueDeclare` |
| `QueueDeclarePassive` | `QueueDeclarePassive` | `name` | `ParamsQueueDeclare` *(reused)* |
| `QueueDelete` | `QueueDelete` | `name` | `ParamsQueueDelete` |
| `QueuePurge` | `QueuePurge` | `name` | `ParamsQueuePurge` |
| `QueueUnbind` | `QueueUnbind` | `name` | `ParamsQueueBind` *(reused; `NoWait` left at zero value `false`)* |
| `Recover` | `Recover` | *(empty)* | `ParamsRecover` |
| `Tx` | `Tx` | *(empty)* | `ParamsTx` |
| `TxCommit` | `TxCommit` | *(empty)* | `ParamsTxCommit` |
| `TxRollback` | `TxRollback` | *(empty)* | `ParamsTxRollback` |

> Note: the doc comments in `trace_params_nop.go` mention a `ParamsConsumeWithContext`
> and a `ParamsQueueUnbind` — those struct types do not exist. The testing implementation
> reuses `ParamsConsume` and `ParamsQueueBind` as shown in the table. Trust the table.

## Invariants

1. **Compile-time gating.** Exactly one of `trace_params_nop.go` / `trace_params_testing.go`
   is compiled (`!test_env` / `test_env`). The 23 function signatures are identical in
   both files, so all call sites compile unchanged.
2. **Zero overhead in production.** Without `test_env`, every trace function body is
   empty; there are no channel operations, no allocations, and no context lookups.
3. **Channel opt-in.** In `test_env` mode a trace function emits only if
   `ctx.Value(TraceChannelParamsName)` is non-nil. A context without the channel key
   makes the call a silent no-op even under the tag.
4. **Blocking sends.** Emission is a plain channel send (`paramsCh <- p`), guarded only
   by the buffer (32 when created by `ConsumeTracesContext`). If the buffer fills and
   nothing drains it, the traced production call blocks. Keep the consumer running
   while AMQP operations execute.
5. **`ConsumeTracesContext` builds a fresh context** from `context.Background()` — it is
   NOT derived from any caller context. The only way to stop the consumer goroutine is
   the returned `CancelFunc` (observed as `ctx.Done()` inside the loop). It starts the
   goroutine itself; do not wrap the call in `go`.
6. **Validator matching** is exact on both `Name` and `Tag`; the first matching
   validator wins. A trace matching no validator produces no result event (accepted
   silently).
7. **Silence is the pass signal.** `ErrorTrace` is sent only for a missing field
   (`fmt.Errorf("field %s not found. Adjust your FieldExpectation(s)", ...)`) or a
   mismatch (`fmt.Errorf("field %s mismatch: Expected %#v, got %#v", ...)`). A trace
   that satisfies all expectations sends nothing.
8. **Comparison rules** (`doConsumeTraces`): the expected field is located with
   `reflect.Value.FieldByName` on `trace.Value`. Map-typed fields (e.g. `Args amqp.Table`)
   are compared with `reflect.DeepEqual` ("maps are not comparable and require special
   handling", per the source); all other fields with `reflect.Value.Equal`. Build
   `FieldExpectation.Value` via `reflect.ValueOf` of the same kind as the traced field.
9. **Consumer goroutine teardown.** The params receive is `select`-guarded by
   `ctx.Done()`, so cancellation always exits the loop — but only after the current
   iteration completes. `ErrorTrace` sends to the results channel are NOT select-guarded:
   if more than 32 failures pile up with no reader, the goroutine blocks on the send.
10. **Unchecked type assertions** in `doConsumeTraces` (`ctx.Value(...).(chan ParamsTrace)`)
    assume both channels are present. This holds because `doConsumeTraces` is unexported
    and is only ever given the context constructed by `ConsumeTracesContext`.
11. **Not public API.** Under Go's `internal/` rule, `traceutils` is importable only from
    within `github.com/LucaWolf/grabbit`; its surface may change without notice.

## Side Effects

**`test_env` active:**
- One blocking channel send of an allocated `ParamsTrace` (plus its `Params*` payload)
  per trace-function call, when the channel key is present in the context.
- `ConsumeTracesContext`: allocates two buffered channels (capacity 32 each), stores two
  context values, and spawns one goroutine; the goroutine exits when the context is
  cancelled.
- `doConsumeTraces`: reflection-based field access on each received payload; blocking
  sends of `ErrorTrace` on failed expectations.
- No file, network, or AMQP I/O; no global mutable state; no metrics.

**`test_env` inactive (production):**
- None. All trace-function bodies are empty.

## Verification

```bash
# Package tests (this package currently has no *_test.go files of its own;
# the command compiles the package under the tag and runs any future tests):
go test -tags test_env ./internal/traceutils/...

# Both build modes must compile:
go build ./internal/traceutils/...              # no-op variant (!test_env)
go build -tags test_env ./internal/traceutils/... # emitter variant (test_env)

# Whole-module (local, repository root; CI runs the same flags WITHOUT a
# package pattern, so it covers only the root package):
go test -tags test_env -coverprofile coverage.out -v ./...
```
