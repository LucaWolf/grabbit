# Per-channel dispatch model: sequential by default, concurrent by option

Every consumer channel now has one of two dispatch models, selected when the consumer
is constructed and fixed for that channel's lifetime. `ProcessorConcurrency` at zero or
below keeps the sequential batch model exactly as it was; a positive value `N` switches
the channel to per-record dispatch, where deliveries flow directly to a pool of at most
`N` worker goroutines, one message per worker, with no batch fill and no fill timer.
There is no runtime switching, and sequential and concurrent consumers coexist on one
connection as independent channels.

The concurrent model exists because the sequential model couples latency to fan-out: a
batch does not run until it fills or its timeout fires, and the callback then runs one
batch at a time. The selector moves that choice onto the channel and puts acknowledgment
discipline under library control instead of user convention.

## Record surface and wire mapping

The concurrent path uses `CallbackProcessRecord(props *DeliveriesProperties, message
DeliveryData) RecordOutcome`, registered with `WithChannelOptionProcessRecord`. The
callback deliberately receives no `*Channel` and no `mustAck` flag: excluding the
channel is what makes library-owned settlement enforceable rather than conventional.

`RecordOutcome` maps to exactly two AMQP primitives through one unexported settle site:

| Outcome | Wire call |
| --- | --- |
| `RecordAck` | `Ack(tag, false)` |
| `RecordNackRequeue` (zero value) | `Nack(tag, false, true)` |
| `RecordNackDrop` | `Nack(tag, false, false)` |

`RecordNackRequeue` is the zero value so an undecided or zero-initialized outcome
redelivers instead of silently acknowledging; unknown outcome values fall through the
same requeue default. A `Reject` is wire-equivalent to the drop Nack and is deliberately
not a third code path.

**`multiple=true` definition:** a consumer acknowledgment with `multiple=true` settles
every outstanding delivery up to and including the given delivery tag on that channel.
The concurrent path always passes `false`, so one wire call settles exactly one record;
bulk acknowledgment is structurally unavailable to record processors.

## Per-record acknowledgment gate and settle-time re-check

There is no sticky `mustAck` across records. Immediately before settling, the helper:

1. Re-derives ack-worthiness for that record from `!ConsumerAutoAck`. Under auto-ack it
   returns without touching the wire.
2. Re-checks the connected latch without blocking. If the channel is down, or its
   transition counter moved past the record's generation, the wire call is skipped and
   the record is left unacknowledged for broker redelivery.
3. Issues exactly one of the three wire calls above.

The one tolerated transition is the establishment `Unlock`: a record pulled while the
latch was still locked may settle if exactly one even-to-odd step landed afterwards, as
that is the same generation still coming up. Any other change means a different
generation.

This is the [ADR-0004](./0004-manual-acknowledgment-is-the-consume-default.md) duty
executed library-side: process the record, but never acknowledge it against a channel
that cannot honor the acknowledgment. The check narrows the stale-tag window; it does
not close it. A rebase can still land between the re-check and the wire call, and the
documented worst case is a single misdirected tag on the recovered channel. Because
`multiple=false` is enforced, that worst case cannot cascade to other deliveries.

## Lifecycle

- **Worker-pull, no dispatcher.** Workers read the AMQP deliveries feed directly. There
  is no internal work channel and no dispatcher goroutine; the in-flight bound holds by
  construction.
- **Pool per recovery generation.** A generation's pool is created with its consume
  loop. Recovery does not cancel the channel context (only `Close` does), so feed close
  is the normal generation-end signal; workers also exit on `ctx.Done()` between
  records.
- **Join before return.** The consume loop waits for its pool before returning. "Gobble
  returned implies no outstanding user code" therefore holds, delayed by at most one
  record per worker.
- **In-flight user code is never aborted.** A callback already running is allowed to
  finish; shutdown and recovery stop new dispatch, not running work.
- **Generation overlap.** Old-generation workers may still be finishing while the new
  generation's pool starts, so up to `2N` callbacks can transiently run. The bound is
  per generation, not global.

## Bounds and QoS

In-flight callbacks are at most `N` by construction. Under manual acknowledgment (the
default) and a positive prefetch, the broker's prefetch window additionally caps
unacknowledged deliveries, so effective parallelism is `min(N, PrefetchCount)`. Under
`ConsumerAutoAck(true)` the broker applies no prefetch throttle and the pool size is the
only bound. A default `PrefetchCount` of 1 makes the option effectively inert under
manual ack; auto-ack keeps the worker bound. `PrefetchCount` 0 means unlimited to the
broker, so it removes the prefetch cap while the pool still limits callbacks to `N`;
that combination is outside the supported concurrent contract.

`QosGlobal(true)` applies the prefetch window to all consumers on the channel.
RabbitMQ's global QoS is per-channel, not connection-wide, and is deprecated and
disabled by default from RabbitMQ 4.3. Since each grabbit consumer owns its channel,
the setting cannot widen a consumer's bound; leave it at the `false` default.

## Retirement set

Selecting the concurrent model retires, for that channel only:

- the batch accumulation slice and batch fill;
- the `PrefetchTimeout` timer arm;
- `EventDataPartial` and `EventDataExhausted`;
- the sticky `mustAck` variable;
- per-batch properties sharing: each record gets properties from its own delivery, but
  `props.Headers` is that delivery's map by reference (not deep-copied) and must be
  treated as read-only;
- the batch processor, which is ignored on the concurrent path and remains the only
  user hook used at zero.

There is also no queue-drain event on this path. Completion is observed through
user-side registries; the notifier events remain advisory and lossy per
[ADR-0002](./0002-advisory-events-are-lossy-by-design.md).

## Poison messages

A processor that returns `RecordNackRequeue` for a record that can never succeed can
loop at wire speed. The library adds no retry counting, backoff, or requeue suppression:
no new machinery competes with the broker's redelivery policy. Processors detect the
loop through `DeliveryData.Redelivered` and choose `RecordNackDrop` (optionally after
their own bounded retry bookkeeping) or route to their own dead-letter handling.

## Consequences

- The zero value preserves the sequential path byte-for-byte: batch fill, flush timer,
  flush events, batch processor, and bulk-ack pattern are unchanged.
- Delivery is at-least-once. Unsettled records — skipped writes on a transition,
  in-flight records at shutdown, or a crash — are redelivered by the broker.
- Record processors run concurrently and must be concurrency-safe; they receive no
  context and must exit between records to observe cancellation.
- Acknowledgments serialize on the channel mutex by design, so `N` workers create `N`
  waiters where the sequential path has one. That is a throughput/latency trade, not a
  safety gap.
- No new event kinds and no trace-harness growth; `Ack`/`Nack`/`Reject` remain untraced
  per [ADR-0005](./0005-build-tag-gated-call-tracing.md).
- Additive public surface; rollout is a normal minor release, and rollback means
  unsetting the option.

**Status**: accepted; implemented by the per-record API and dispatch stages, with the
behavioural suite covering recovery, shutdown, bounds, retirement, poison, and
generation overlap.
