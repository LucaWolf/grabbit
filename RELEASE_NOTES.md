# Release notes

This document packages the two releases that follow the last published tag
(`v1.1.17`): the remediation batch and the fast-follow feature minor. Only the
minor is assigned a symbolic version here (`v1.2.0`); the remediation batch is
the corrections-only predecessor and is published separately.

## Two-column release map

| Area | Remediation batched release | Fast-follow minor (`v1.2.0`) |
| --- | --- | --- |
| Scope | Corrections only | Additive per-record consumer dispatch |
| Public API | No additions or removals | New consumer option, record callback, outcome enum |
| Consumer default | Unchanged: sequential batches, manual ack, prefetch 1 | Sequential default; new option is off (zero), negative also means sequential |
| Dispatch | Sequential batch fill and flush timer | Opt-in per-record worker pool; no batch fill, timer, or flush events |
| Ack settlement | Existing batch bulk-ack gate | Library-owned per-record settlement with `multiple=false` |
| Drain signal | `EventDataPartial` (flush) / `EventDataExhausted` (drain) | No queue-drain event; completion via user-side registries |
| Retirement set | n/a (nothing retired) | Batch fill, `PrefetchTimeout`, flush events, sticky `mustAck`, per-batch property sharing, batch processor |
| `QosGlobal` | Per-channel global QoS; default `false` | Unchanged; global QoS cannot widen the per-channel bound |
| Poison messages | Broker requeue policy; no library suppression | Same; detect `Redelivered` and return `RecordNackDrop` |
| Topology recovery | Durable and ephemeral declared elements re-declared ([ADR-0003](./docs/adr/0003-recovery-redeclares-all-declared-topology.md)) | Unchanged |
| Publisher callbacks | Default notify-publish sets `Err` only on failure | Unchanged |
| Notifier retry | Interruptible connection manage-loop wait ([ADR-0006](./docs/adr/0006-connection-notifier-retry-is-interruptible.md)) | Unchanged |
| Examples | None | Tenth module `examples/per_record/` |
| Migration | None; corrections preserve the API | None required; opt in via `WithProcessorConcurrency` |

## Remediation batched release (corrections only)

This release contains no new public surface and no feature work. It corrects
existing behavior:

- **Constructor option slices are copied before appending.** `NewPublisher` and
  `NewConsumer` no longer mutate a caller-owned variadic option slice.
- **Publisher default callback reports failure only.** The default notify-publish
  callback sets `Err` only when the confirmation fails; a successful confirmation
  raises a clean `EventMessagePublished`.
- **Durable topology is re-declared on recovery.** Every topology element marked
  `Declare: true` is re-declared on recovery, durable included. Re-declaration is
  idempotent for identical parameters; a drifted durable definition surfaces as an
  AMQP 406 precondition failure instead of being silently reused
  ([ADR-0003](./docs/adr/0003-recovery-redeclares-all-declared-topology.md)).
- **Connection notifier retry is interruptible.** Cancelling the context while the
  base connection is unset now exits the manage loop instead of sleeping through
  the retry window ([ADR-0006](./docs/adr/0006-connection-notifier-retry-is-interruptible.md)).

The remediation release does not include the per-record feature below; that is the
fast-follow minor's content.

## `v1.2.0` — per-record consumer dispatch

### Added public surface

- `ConsumerUsageOptions.ProcessorConcurrency int` — `0` or negative keeps the
  sequential batch model; a positive value `N` selects per-record dispatch with at
  most `N` workers.
- `(*ConsumerOptions).WithProcessorConcurrency(count int) *ConsumerOptions` — the
  fluent builder for the selector.
- `CallbackProcessRecord func(props *DeliveriesProperties, message DeliveryData)
  RecordOutcome` — the per-record user hook. It receives no `*Channel` and no
  `mustAck` flag; the library owns settlement.
- `WithChannelOptionProcessRecord(proc CallbackProcessRecord) func(*ChannelOptions)`
  — registers the record hook on a consumer channel.
- `RecordOutcome int` with the constants `RecordNackRequeue` (zero value),
  `RecordAck`, and `RecordNackDrop`.
- `(RecordOutcome).String() string` — generated stringer, covered by the repository's
  stringer guard test.

No new event kinds, error types, or trace parameters were added. Ack, nack, and
reject remain untraced per ADR-0005.

### Behavior on the concurrent path

- The dispatch model is fixed at construction. `ProcessorConcurrency > 0` ignores a
  registered batch processor; at `0` the record processor is unused.
- Deliveries go to `N` workers as they arrive. There is no batch accumulation, no
  `PrefetchTimeout` timer, and no `EventDataPartial`/`EventDataExhausted`. The
  sequential model's drain signal is retired with them: the concurrent path raises no
  `EventDataExhausted` when the queue empties, so completion must be observed through
  user-side registries.
- Effective in-flight concurrency is `min(N, PrefetchCount)` under manual
  acknowledgment with a positive prefetch, and `N` under `ConsumerAutoAck(true)`.
  Under auto-ack the library issues no settlement calls; the broker treats each
  delivery as acknowledged on dispatch, and the pool alone bounds concurrency.
  A default prefetch of 1 makes the option effectively inert under manual ack;
  `PrefetchCount` 0 means unlimited to the broker while the worker pool still caps
  callbacks at `N`.
- Every outcome is settled by the library as one wire call: `Ack(tag, false)`,
  `Nack(tag, false, true)`, or `Nack(tag, false, false)`. `multiple=true` is never
  issued on this path; it settles every outstanding delivery up to the given tag
  and could cascade beyond one record.
- Before settling, the library re-derives ack-worthiness per record and re-checks
  the connected latch without blocking. A down channel or a generation change
  skips the write and leaves the record for broker redelivery. The stale-tag window
  is narrowed, not closed; the documented worst case is a single misdirected tag
  ([ADR-0008](./docs/adr/0008-per-channel-dispatch-model-sequential-by-default-and-concurrent-by-option.md)).
- Workers are spawned per recovery generation and joined before the consume loop
  returns. In-flight user code is never aborted. Old and new generations can
  transiently overlap, so up to `2N` callbacks may run during recovery; the bound
  is per generation, not global.
- The default record processor returns `RecordAck` per record and raises
  `EventMessageReceived` without an error. The default batch processor also now sets
  its event `Err` only when the ack fails.

### Corrections included in this minor

Two corrections land with the feature but add no public surface:

- The default batch processor no longer sets a synthetic `Err` on every
  `EventMessageReceived`; it reports an error only when the ack fails.
- The channel's `NotifyClose` notifier is buffered. This removes a reproducible
  context-cancel shutdown deadlock in which the AMQP channel shutdown broadcast
  blocked while the manage loop was inside `Close`, with record workers waiting
  behind the channel mutex.

### Retirement set

Selecting the concurrent model retires, for that channel only:

- the batch accumulation slice and batch fill;
- the `PrefetchTimeout` timer arm;
- `EventDataPartial` and `EventDataExhausted`;
- the sticky `mustAck` variable;
- per-batch property sharing (each record gets its own properties, while
  `props.Headers` remains the delivery's map by reference and must be treated as
  read-only);
- the batch processor (ignored on this path).

The sequential dispatch remains the zero-value default: batch fill, flush timer,
flush events, batch processor, and bulk-ack pattern all stay. The one sequential-path
payload change is the default batch processor's `Err`-only-on-failure correction.

### `QosGlobal` caveat

`WithQosGlobal(true)` applies the prefetch window to all consumers on the channel;
RabbitMQ's global QoS is per-channel, not connection-wide, and is deprecated and
disabled by default from RabbitMQ 4.3. Each grabbit consumer owns its channel, so
this setting cannot widen the channel's bound. Leave it at the `false` default,
especially when mixing sequential and concurrent consumers on one connection.

### Poison-message guidance

A record returned with `RecordNackRequeue` can loop at wire speed when it can never
succeed. The library deliberately adds no retry counting, backoff, or requeue
suppression. Processors detect the loop through `DeliveryData.Redelivered` and
return `RecordNackDrop` (optionally after their own bounded retry bookkeeping), or
route the record to their own dead-letter handling. See
`examples/per_record/` for a runnable requeue-then-drop branch.

### Compatibility and migration

- Additive and non-breaking for existing users: the zero value keeps the sequential
  dispatch model, and no existing exported identifier changed shape. The only
  sequential-path payload change is the default batch processor's `Err` correction.
- Opt in per consumer:
  ```go
  opts := grabbit.DefaultConsumerOptions()
  opts.WithQueue("jobs").
      WithPrefetchCount(8).
      WithProcessorConcurrency(8)
  consumer := grabbit.NewConsumer(conn, opts,
      grabbit.WithChannelOptionProcessRecord(processRecord))
  ```
- Record processors must be concurrency-safe above one worker, must not rely on
  processing order, and must exit between records to observe cancellation.
- Advanced users constructing channels directly with `NewChannel` plus
  `WithChannelOptionUsageParams` receive the same selector; the dispatch model is
  still fixed at construction.
- Requires Go 1.25.7, matching the root module.

### Verification

The release candidate was validated with:

- `go build ./...` and `go vet -tags test_env ./...`;
- `go test -race -tags test_env ./...`;
- `go test -tags test_env -coverprofile coverage.out -v` (CI workflow, root
  package only);
- `go test -tags test_env -coverprofile coverage.out ./...` (all packages,
  local);
- `examples/per_record/`: `go build`, `go vet`, and a live LavinMQ run reporting
  `attempts=32 acks=8 requeues=8 drops=16 terminal=24/24 peak-overlap=8`.

## Release checklist

- [x] Symbolic version bump in `README.md`.
- [x] Two-column release map and release notes in this file.
- [ ] Create and push the release tag, then publish these notes on the project's
      release page.
