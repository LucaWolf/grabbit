# grabbit — Repository Root Blueprint

> **Module:** `github.com/LucaWolf/grabbit` (Go 1.25.7) — a managed, auto-reconnecting
> RabbitMQ client library wrapping `github.com/rabbitmq/amqp091-go` v1.10.0, with
> `github.com/LucaWolf/go-notifying-latch` v1.1.0 for state latches and
> `github.com/michaelklishin/rabbit-hole/v3` v3.2.0 for management API access;
> `go.uber.org/goleak` v1.3.0 is a test-only dependency (goroutine-leak verification
> in `container_setup_test.go`).
>
> This file is the machine-oriented module-boundary contract (`CONTEXT.md`). The
> human-maintained `README.md` at the repository root is a separate document and is
> deliberately untouched by contract generation.

## System Overview

grabbit turns raw amqp091-go connections and channels into **managed objects** that
survive broker outages. Each of the four constructors (`NewConnection`, `NewPublisher`,
`NewConsumer`, `NewChannel`) returns an object owning a background recovery goroutine;
the goroutine re-dials / re-opens the underlying AMQP resource with exponential backoff,
re-registers AMQP notifier channels, re-declares declared topology (durable and
ephemeral), and reports
every lifecycle transition both through optional user callbacks (Down / Recovering,
which can veto further retries by returning `false`, plus the no-return Up) and through
a lossy `chan Event` notification channel. State is tracked with `NotifyingLatch`
objects (`connected`, `managed`, `blocked`, `paused`), and the supported way to observe
it is `AwaitStatus` / `AwaitManager` rather than `IsClosed` polling.

Publishers get broker confirmations two ways: channel-level `NotifyPublish` callbacks
armed by a positive `ConfirmationCount`, and per-message `PublishDeferredConfirm` +
`AwaitDeferredConfirmation` with a `ConfirmationOutcome` matrix. Consumers choose one
of two per-channel dispatch models fixed at construction: the default sequential model
feeds prefetch-batched deliveries to `CallbackProcessMessages`, while a positive
`ProcessorConcurrency` switches the channel to per-record worker dispatch through
`CallbackProcessRecord`; both default to manual acknowledgments. The root package's direct source files
implement all of this; `internal/traceutils` provides a build-tag-gated tracing harness
the test suite uses to assert exact AMQP call parameters without a broker, and
`examples/` holds ten standalone demo modules.

## Local Interface & Invariants

The root directory contains the library itself (~22 non-test `.go` files, package
`grabbit`). Everything below is grounded in those files.

### Interface — constructors and client types

| Symbol | Signature (from source) |
| --- | --- |
| `NewConnection` | `(address string, config amqp.Config, optionFuncs ...func(*ConnectionOptions)) *Connection` |
| `NewPublisher` | `(conn *Connection, opt PublisherOptions, optionFuncs ...func(*ChannelOptions)) *Publisher` |
| `NewConsumer` | `(conn *Connection, opt ConsumerOptions, optionFuncs ...func(*ChannelOptions)) *Consumer` |
| `NewChannel` | `(conn *Connection, optionFuncs ...func(*ChannelOptions)) *Channel` — lower-level; most users should use Publisher/Consumer instead |

`Connection` methods: `Close() error`, `Channel() (*amqp.Channel, error)` (raw channel),
`Connection() *SafeBaseConn`, `IsBlocked() bool`, `IsClosed() bool`,
`AwaitStatus(connected bool, timeout time.Duration) bool`, `AwaitManager(active bool,
timeout time.Duration) bool`, `AwaitAvailable(timeout time.Duration) bool` (deprecated).

`Publisher` methods: `Publish(amqp.Publishing) error`, `PublishWithOptions(opt
PublisherOptions, msg amqp.Publishing) error`, `PublishDeferredConfirm(amqp.Publishing)
(*DeferredConfirmation, error)`, `PublishDeferredConfirmWithOptions(...)`,
`AwaitDeferredConfirmation(d *DeferredConfirmation, tmr time.Duration)
*DeferredConfirmation`, `Channel() *Channel`, `Available() (bool, bool)`,
`AwaitStatus`, `AwaitManager`, `AwaitAvailable` (deprecated), `Close() error`.

`Consumer` methods: `Channel() *Channel`, `Cancel() error`, `Available() (bool, bool)`,
`AwaitStatus`, `AwaitManager`, `AwaitAvailable` (deprecated), `Close() error`.

`Channel` methods (safe wrappers around the underlying `amqp.Channel`):
acknowledgment — `Ack`, `Nack`, `Reject`; consuming — `Consume`, `ConsumeWithContext`,
`Get`, `Qos`, `Recover`; publishing — `Publish`, `PublishWithContext`,
`PublishWithDeferredConfirm`, `PublishWithDeferredConfirmWithContext`,
`GetNextPublishSeqNo`; topology — `QueueDeclare`, `QueueDeclarePassive`,
`QueueDeclareWithTopology`, `QueueBind`, `QueueUnbind`, `QueuePurge`, `QueueDelete`,
`QueueInspect` (deprecated), `ExchangeDeclare`, `ExchangeDeclareWithTopology`,
`ExchangeDelete`; transactions/flow — `Tx`, `TxCommit`, `TxRollback`, `Flow`, `Confirm`;
notifier plumbing — `NotifyClose`, `NotifyCancel`, `NotifyFlow`, `NotifyReturn`,
`NotifyPublish`, `NotifyConfirm`; state — `Name()`, `Queue()`, `Channel() *SafeBaseChan`,
`IsPaused()`, `IsClosed()`, `AwaitStatus`, `AwaitManager`, `AwaitAvailable`
(deprecated), `Close()`, `Cancel(consumer string, noWait bool) error`.
(There is no `ExchangeBind` method — exchange bindings are applied only via topology
declaration.)

### Interface — options, events, and supporting types

- **Connection options** (`connection_options.go`, all struct fields unexported):
  `WithConnectionOptionDown`, `WithConnectionOptionUp`, `WithConnectionOptionRecovering`,
  `WithConnectionOptionContext`, `WithConnectionOptionPassword(SecretProvider)`,
  `WithConnectionOptionDelay(DelayProvider)`, `WithConnectionOptionName`,
  `WithConnectionOptionNotification(chan Event)`.
- **Channel options** (`channel_options.go`): `WithChannelOptionDown`, `WithChannelOptionUp`,
  `WithChannelOptionRecovering`, `WithChannelOptionContext`, `WithChannelOptionDelay`,
  `WithChannelOptionName`, `WithChannelOptionNotification`, `WithChannelOptionTopology([]*TopologyOptions)`,
  `WithChannelOptionNotifyPublish`, `WithChannelOptionNotifyReturn`,
  `WithChannelOptionProcessor(CallbackProcessMessages)`,
  `WithChannelOptionProcessRecord(CallbackProcessRecord)`,
  `WithChannelOptionUsageParams(ChanUsageParameters)`.
- **Publisher options** (`publisher_options.go`): `PublisherOptions` embeds
  `PublisherUsageOptions{ConfirmationCount, ConfirmationNoWait, IsPublisher}` — the half
  `NewPublisher` forwards to its channel via `WithChannelOptionUsageParams`;
  `DefaultPublisherOptions()` and the fluent builders `WithContext`, `WithExchange`,
  `WithKey`, `WithMandatory`, `WithImmediate`, `WithConfirmationsCount`,
  `WithConfirmationNoWait`.
- **Consumer options** (`consumer_options.go`): `ConsumerOptions` embeds
  `ConsumerUsageOptions` — its `IsConsumer: true` default is what makes a channel start
  consuming (spawning the `gobble` delivery loop); `DefaultConsumerOptions()`,
  `RandConsumerName()`, and the fluent builders `WithName`, `WithPrefetchTimeout`,
  `WithPrefetchCount`, `WithPrefetchSize`, `WithProcessorConcurrency`, `WithQosGlobal`,
  `WithQueue`, `WithAutoAck`, `WithExclusive`, `WithNoLocal`, `WithNoWait`, `WithArgs`.
- **Topology** (`topology_options.go`): `TopologyOptions{Name, IsDestination,
  IsExchange, Bind TopologyBind, Kind, Durable, AutoDelete, Exclusive, Internal, NoWait,
  Passive, Declare, Args}` with `GetRouting()` deriving binding direction from
  `IsDestination`; `TopologyBind{Enabled, Peer, Key, NoWait, Args}`.
- **Events** (`events.go`, `eventtype_string.go`): `Event{SourceType ClientType,
  SourceName, TargetName string, Kind EventType, Err OptionalError}`; 15 `EventType`
  constants (`EventUp` … `EventDataPartial`); `ClientType` with `CliConnection`/`CliChannel`.
- **Confirmations** (`publisher.go`, `confirmationoutcome_string.go`):
  `DeferredConfirmation{ *amqp.DeferredConfirmation; Outcome ConfirmationOutcome;
  RequestSequence uint64; ChannelName, Queue string }`; `ConfirmationOutcome` constants
  `ConfirmationTimeOut`, `ConfirmationClosed`, `ConfirmationDisabled`,
  `ConfirmationPrevious`, `ConfirmationACK`, `ConfirmationNAK`.
- **Delivery processing** (`general_options.go`): sequential batches use
  `CallbackProcessMessages(props *DeliveriesProperties, messages []DeliveryData,
  mustAck bool, ch *Channel)`; the concurrent path uses
  `CallbackProcessRecord(props *DeliveriesProperties, message DeliveryData)
  RecordOutcome`, which deliberately receives no `*Channel` and no `mustAck`.
  `RecordOutcome` has `RecordAck`, `RecordNackRequeue` (the zero value, so an
  undecided outcome redelivers), and `RecordNackDrop`. Supporting types:
  `DeliveriesProperties` / `DeliveryPropsFrom`, `DeliveryData` / `DeliveryDataFrom`,
  `DeliveryPayload`; callback types `CallbackWhenDown`, `CallbackWhenUp`,
  `CallbackWhenRecovering`, `CallbackNotifyPublish`, `CallbackNotifyReturn`.
- **Delay & secrets** (`general_options.go`): `DelayProvider` interface, `DefaultDelayer`
  (via `NewDefaultDelayer()`), `SecretProvider` interface for `WithConnectionOptionPassword`.
- **Concurrency primitives & utilities**: `SafeBaseConn` / `SafeBaseChan`
  (`connection_safebase.go` / `channel_safebase.go`) exposing `IsSet`, `Super`, and
  manual `Lock`/`Unlock`/`RLock`/`RUnlock`; `SafeBool` (`events.go:9-12`) — an exported
  concurrency-safe bool wrapper, currently referenced by nothing else in the package;
  `OptionalError` (`error.go`) with `SomeErrFromError`, `SomeErrFromString`, `Or`,
  `Error`, `IsSet`; `ClampInt`, `ClampInt64`, `ClampDuration` (`utils.go`).

### Invariants

- **Encapsulation:** every field of `ConnectionOptions` / `ChannelOptions` is
  unexported; the `With*` option funcs are the only mutation path.
- **Constructor defaults & inheritance:** `NewConnection` defaults to an unbuffered
  `make(chan Event)` notifier, name `conn.default`, and `context.Background()`
  (`connection.go:68-73`); `NewChannel` inherits the connection's `notifier`,
  `delayer`, and `ctx` unless overridden, defaulting its own name to `chan.default`
  (`channel.go:74-82`) — a channel therefore silently shares its connection's
  notification channel and context.
- **Defaults:** `NewDefaultDelayer()` = 200 ms base, `RetryCap` 7 (clamped to 7–10),
  `delay = base * 2^(attempt-1)` + ~10% jitter, `retry` clamped to `[1, retryCap]`.
  `DefaultPublisherOptions()`: `ConfirmationCount` 10, `IsPublisher` true,
  `Context` `context.TODO()`, empty exchange/key, `Mandatory`/`Immediate` false.
  `DefaultConsumerOptions()`: `IsConsumer` true (gates consume-mode behavior on the
  channel), `PrefetchCount` 1, `PrefetchSize` 0, `ProcessorConcurrency` 0 (the
  sequential batch model), `ConsumerAutoAck` **false**, random `ConsumerName`
  (8 crypto-random bytes hex-encoded — losing the server-assigned name would make
  `Cancel` impossible).
- **Consumers default to manual ack:** `mustAck = !ConsumerAutoAck`; before processing
  any batch, grabbit ANDs `mustAck` with a 100 ms `AwaitStatus(true, ...)` check and a
  connected-latch transition check — across a recovery transition the batch is
  processed with `mustAck=false` (cannot ack on a dead channel); on ctx cancellation or
  consumer-channel close, a pending batch is flushed with `mustAck=false`. The
  concurrent path has no sticky `mustAck`: the settle helper re-derives ack-worthiness
  per record, re-checks the connected latch non-blockingly immediately before the wire
  call, and skips the write when the channel is down or the generation changed, leaving
  the record unacknowledged for redelivery
  ([ADR-0008](./docs/adr/0008-per-channel-dispatch-model-sequential-by-default-and-concurrent-by-option.md)).
  Every record is settled with `multiple=false`, so an ack or nack covers exactly one
  delivery tag.
- **Sequential batching:** deliveries accumulate up to `PrefetchCount`;
  `PrefetchTimeout` flushes partial batches (`EventDataPartial`, or `EventDataExhausted`
  when nothing pending); `DeliveriesProperties` are taken from the first delivery of
  each batch. QoS errors only raise `EventQos` (consume proceeds anyway), consume errors
  raise `EventConsume` and abort.
- **Concurrent dispatch:** a positive `ProcessorConcurrency` selects the per-record
  model fixed at construction. Workers pull deliveries directly from the feed (no
  dispatcher goroutine, no work channel); each record gets its own
  `DeliveriesProperties` from its own delivery, and `props.Headers` is that delivery's
  map by reference (not deep-copied) and must be treated as read-only. There is no
  batch fill, no `PrefetchTimeout` timer, no `EventDataPartial`/`EventDataExhausted`,
  and the batch processor is ignored; at zero the record processor is unused. Effective
  in-flight concurrency is `min(N, PrefetchCount)` under manual ack (for a positive
  prefetch) and the pool size `N` under `ConsumerAutoAck`; `PrefetchCount` 0 or
  negative removes the broker window (`0` means unlimited) while the pool still caps
  callbacks at `N`. `QosGlobal(true)` is per-channel, is deprecated and disabled by
  default on RabbitMQ 4.3+, and does not widen the bound because each grabbit consumer
  owns its channel. No queue-drain event exists on this path — completion is observed
  through user-side registries.
- **Queue selection:** `Channel.Queue()` is written only by topology elements marked
  `IsDestination` (including server-assigned names); a non-empty `ch.queue` overrides
  `ConsumerQueue` at consume time.
- **Doc warnings in the option builders:** `WithPrefetchSize` is not supported by
  RabbitMQ and seems to block consumers — avoid it; `WithExclusive(true)` blocks all
  other consumers on the queue; `WithImmediate(true)` is no longer supported by recent
  RabbitMQ servers; `WithConfirmationNoWait(true)` seems to block the `Confirm`
  operation on the channel. Under manual ack, `WithProcessorConcurrency` is inert at
  the default `PrefetchCount` of 1 (auto-ack ignores prefetch); it retires
  `PrefetchTimeout` and the flush events when set, and
  treats zero/negative as sequential. `PrefetchCount <= 0` is outside the supported
  concurrent contract: `0` means unlimited to the broker, so the worker pool alone caps
  callbacks at `N`; its record processor must be concurrency-safe and must not rely on
  processing order.
- **Confirmation mode:** armed only when `ConfirmationCount > 0`; a zero count disables
  it entirely (every `AwaitDeferredConfirmation` then yields `ConfirmationDisabled`).
  `PublishDeferredConfirm*` captures `RequestSequence` via `GetNextPublishSeqNo()`
  before publishing; `AwaitDeferredConfirmation` resolves exactly one outcome:
  timer expired → `ConfirmationTimeOut`, publisher ctx done → `ConfirmationClosed`,
  `RequestSequence > DeliveryTag` → `ConfirmationPrevious`, else `Acked()` →
  `ConfirmationACK`, else `ConfirmationNAK`.
- **Publish preconditions:** `Publisher.Publish*` check `channel.IsClosed()` first and
  short-circuit with `amqp.ErrClosed` without touching the base channel.
- **Topology lifecycle:** an element is declared only when `Declare: true`. Every
  declared element is (re-)declared on recovery, durable and ephemeral alike
  ([ADR-0003](./docs/adr/0003-recovery-redeclares-all-declared-topology.md)):
  AMQP declarations are idempotent for identical parameters, so re-declaring is safe
  and covers a broker that lost durable definitions. A drifted durable definition
  surfaces as an AMQP 406 precondition failure instead of silently continuing.
  Topology runs on an isolated short-lived channel (amqp
  channels are not safe to share); an empty `Kind` defaults to `"direct"` (mutating the
  caller's `TopologyOptions` in place); a queue declaring with an empty returned name is
  an error.
- **Recovery control flow:** all recovery loops watch `ctx.Done()` — cancelling the
  context terminates them and closes the object. A Down/Recovering callback returning
  `false` aborts the loop. `recover()` reconnects only when the close error `IsSet()`
  (a graceful close terminates); a consumer-Cancel notification always re-enters the
  reconnect loop. The retry counter wraps modulo 0xFFFF. On notifier-acquisition
  failure the connection's manage loop waits an interruptible `delayer.Delay(3)`
  backoff (`connection.go`, `delayerCompleted`) so cancellation always exits; the
  channel's manage loop waits a flat `delayer.Delay(3)` (`channel.go:159`) after its
  own context-aware `recover` call. Both re-arm a 120-second anti-hot-loop
  `time.After` timer per iteration (`connection.go:229`, `channel.go:197`).
  `RetryCap` clamps the delay exponent, not the attempt count
  (see [ADR-0006](./docs/adr/0006-connection-notifier-retry-is-interruptible.md)).
- **Events are lossy:** `raise` uses a non-blocking send with `default` — events are
  silently dropped when the user's notification channel buffer is full.
- **`AwaitStatus` is the supported state probe:** the connected latches track state
  that "flow wraps (after)" the raw amqp status change, making them safer than
  `IsClosed()`. `timeout == 0` is replaced by 5 ms internally; awaiting the *down*
  state deliberately uses `context.Background()` so a tripping master context cannot
  fake a latch result.
- **Idempotency:** `Close()` is idempotent for Connection / Channel / Publisher /
  Consumer. `Cancel` is documented as NOT idempotent; `Consumer.Cancel()` hard-codes
  `noWait=false` so the server redistributes outstanding messages.
- **Notifier wrapper edge case:** when the base channel is unset, `NotifyClose` /
  `NotifyFlow` / `NotifyReturn` / `NotifyCancel` / `NotifyConfirm` / `NotifyPublish`
  **close the caller-supplied channel(s)** instead of returning them open
  (`channel_safewrappers.go:300-361`).
- **Raw access contract:** `SafeBaseChan.Super()` / `SafeBaseConn.Super()` only briefly
  RLock; direct amqp use must be bracketed with the provided `Lock/Unlock` (or
  `RLock/RUnlock`) or recovery is locked out.
- **Constructor appends:** `NewPublisher`/`NewConsumer` copy the caller's option funcs
  before appending `WithChannelOptionUsageParams` **after** them, so the caller-owned
  variadic backing array is never mutated; usage params stay last so they override any
  user-supplied ones.
- **Default callbacks:** the default batch processor discards message bodies and, when
  `mustAck`, acks only the last delivery tag with `multiple=true`, raising
  `EventMessageReceived` with `Err` set only if that ack fails; the default record
  processor (concurrent path) returns `RecordAck` per record and raises
  `EventMessageReceived` with no error; the default publish callback raises
  `EventMessagePublished` per confirmation with `Err` set only on a failed
  confirmation, while the default return callback raises `EventMessageReturned`
  unconditionally per return.
- **Stringer guards:** `clienttype_string.go`, `eventtype_string.go`,
  `confirmationoutcome_string.go`, and `recordoutcome_string.go` carry guard arrays that turn
  enum drift into compile errors (2 `ClientType` values, 15 `EventType` values,
  6 `ConfirmationOutcome` values, 3 `RecordOutcome` values).
- **Separate connections advised:** `NewConsumer`'s doc comment (consumer.go:51)
  recommends separate connections for publish and consume workloads.

### Side Effects

- **Goroutines:** every constructor spawns a background recovery/manage goroutine that
  outlives the call (managed-latch `Unlock()` on entry, deferred `Lock()`); each
  sequential consumer deliveries channel gets one `gobble` loop goroutine, while a
  concurrent consumer channel spawns `ProcessorConcurrency` worker goroutines per
  recovery generation, all joined before that generation's consume loop returns (old
  and new generations may briefly overlap at 2N workers); timers (`time.After`) are
  created per backoff, per `AwaitDeferredConfirmation`, and per sequential consume
  iteration only.
- **Network:** `amqp.DialConfig` on (re)connect; every safe-wrapper channel operation
  performs broker I/O **while holding** `baseChan.mu`; `refreshCredentials` calls the
  user's `SecretProvider.Password()` and rewrites the connection address.
- **In-place mutations of caller-owned values:** the `amqp.Config` passed to
  `NewConnection` (client connection name is set on it), `TopologyOptions.Kind`
  (defaulted to `"direct"`), and
  `DeferredConfirmation.Outcome` (set by `AwaitDeferredConfirmation` before return).
- **Locking / latches:** `sync.RWMutex` on `SafeBaseConn`/`SafeBaseChan`;
  `NotifyingLatch` Lock/Unlock notifies the go-notifying-latch watchers.
- **Entropy & clock:** `crypto/rand` for `RandConsumerName`; `time.Now().UnixNano()`
  for backoff jitter.
- **Events:** non-blocking sends on the user-provided `chan Event` (drops possible);
  confirm/return callbacks are invoked from the channel's manage goroutine.
- **Tracing hooks:** the safe wrappers call `internal/traceutils` functions beside the
  AMQP operations that have trace counterparts (declare/bind/purge/delete, the publish
  variants, consume, Qos, flow, confirm, tx, get, recover); `Close`, `Cancel`, `Ack`,
  `Nack`, and `Reject` have no trace hooks. Trace calls compile to empty no-ops unless
  the `test_env` build tag is active.

## Subdirectories & System Boundaries

- [examples](./examples/CONTEXT.md) — ten standalone demo modules (own `go.mod` each,
  `replace`d to this working tree), outside the root module's package graph: connection
  and channel lifecycle, context trees, baseline/routing/confirmed publishing, competing
  and batch consuming, and per-record concurrency (`per_record`). Compile-only
  verification per module;
  running requires a live RabbitMQ (the demos use `localhost`, except `per_record`'s
  `127.0.0.1`).
- [internal](./internal/CONTEXT.md) — non-public support code; currently one child:
  [internal/traceutils](./internal/traceutils/CONTEXT.md), the build-tag-gated AMQP
  call-tracing harness used by the root package and its tests.

## Global Invariants

- The **root module** (`github.com/LucaWolf/grabbit`, go 1.25.7) is the library. The
  ten **example modules** are separate Go modules pinned via `replace` directives;
  the root `go test ./...` does not compile them. Most of their `go.sum` files predate
  the root's current dependency pins (`go mod tidy` may be required before they build);
  `per_record` is pinned to the current root dependencies.
- Everything under `internal/` is invisible to external importers (Go `internal` rule)
  and may change freely; nothing there is part of the public API.
- The `test_env` build tag is part of the testing contract: CI runs
  `go test -tags test_env ...`, and trace instrumentation is a no-op in production
  builds. Trace-sensitive tests fail without it.
- Topology durability is a cross-boundary semantic: the code re-declares every
  declared element on recovery, durable included, per
  [ADR-0003](./docs/adr/0003-recovery-redeclares-all-declared-topology.md) — safe
  because AMQP declares are idempotent for identical parameters. A drifted durable
  definition fails loudly with AMQP 406 rather than being silently reused.
- Notification events are advisory, not reliable: every component drops events on full
  buffers. Never build correctness-critical logic on `chan Event` delivery alone.
- Consumer acknowledgments are manual by default (`ConsumerAutoAck=false`) everywhere,
  including all shipped examples.

## Global Side Effects

- Any grabbit entrypoint opens real TCP/AMQP connections to the broker address passed
  to `NewConnection` and keeps retrying (exponential backoff, jittered) while Down /
  Recovering callbacks return `true` and the context is alive.
- Each managed object contributes background goroutines and latch-watcher
  notifications; process shutdown should cancel contexts and/or call the idempotent
  `Close()` to let recovery loops terminate.
- `go-notifying-latch` watchers fire on every connected/managed/blocked/paused
  transition; user notification channels receive best-effort `Event` values.

## System Verification

- **Root package (CI command, verbatim from `.github/workflows/coverage.yaml`):**
  `go test -tags test_env -coverprofile coverage.out -v` — note the workflow passes
  **no package pattern**, so CI runs only the current-directory root package's tests.
- **All packages (local):** `go test -tags test_env ./...`.
- **Race detector:** `go test -race -tags test_env ./...`.
- **Tracing harness subtree (local):** `go test -tags test_env ./internal/traceutils/...`
  — `internal/traceutils` has no test files of its own; CI does not target it.
- **Static analysis:** a second CI workflow, `.github/workflows/codeql.yml`, runs
  CodeQL Go analysis on push/pull-request to `main` and on a weekly schedule.
- **Examples:** compile-only, per module — see [examples/CONTEXT.md](./examples/CONTEXT.md)
  for the per-directory commands and the shared `go mod tidy` prerequisite.
