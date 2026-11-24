# Concurrent Consumer Processing: Per-Record Dispatch

*   **Author:** LucaWolf
*   **Status:** Proposed
*   **Date:** 2026-11-23

--------------------------------------------------------------------------------

## Overview

grabbit is a Go library that wraps RabbitMQ (via `amqp091-go`) with automatic
reconnection, managed channels, publishers, and consumers. Its consumer today
delivers messages to user code in **batches**: the internal consume loop
(`gobble`) accumulates up to `PrefetchCount` deliveries (or until
`PrefetchTimeout` fires) and hands the whole batch to a single user callback,
which runs serially on the consume loop's goroutine.

Applications that need messages processed in parallel must build their own
concurrency inside that callback, without help from the library: no lifecycle
guard, no join, no context for workers, and full exposure to the acknowledgment
hazards that unserialized acks create.

This design adds a **per-channel dispatch-model selector**: a new option
(`WithProcessorConcurrency`) whose zero value keeps today's batch/sequential
behavior exactly as it is, and whose positive value switches that one consumer
channel to **per-record dispatch** — deliveries flow to a pool of that many
worker goroutines as they arrive, one message per worker, with no batch fill
and no fill timer. Each worker processes its message and returns a per-record
outcome (Ack, Nack-requeue, Nack-drop); the library executes the outcome as an
individual, non-bulk wire call from a single library-owned place. Batch
consumers and record-concurrent consumers coexist on one connection as
separate channels. The design is fully settled at the decision level in
[concepts_brief.md](./concepts_brief.md) (2026-11-23 edition, DR1–DR6); this
document turns it into an implementable plan.

## The Problem

Two structural costs come with the current batch/sequential consumer model.

**1. The batch-fill barrier.** User code does not run until the batch fills or
the flush timer fires (`channel_consume.go:85`, `:100`). Under sparse arrivals
with a long timeout (the shipped examples set 5s and 7s), messages sit in the
buffer for seconds before any application code sees them. Raising the timeout
widens the batch (more parallelism once processing starts) but delays the
first message further; lowering it improves latency but collapses the batch
size. **Batch width is fan-out width** — no timeout value gives both low
latency and parallelism.

**2. Serial processing.** The callback runs one batch at a time. One slow
message stalls every message behind it, bounded only by the channel's
prefetch window.

Today's workarounds each give something up:

*   *User-side goroutines inside the callback* work today, but every project
    reimplements the pool with no join, no shutdown guarantee, no context for
    workers — and the callback receives the channel handle, so an unsupervised
    `Ack(tag, multiple=true)` from a user goroutine silently acknowledges
    every lower-tagged delivery still being processed elsewhere (data loss),
    and a `Nack` of an already-settled tag closes the channel.
*   *Competing consumers* (more consumers, more connections) scale
    throughput but multiply connection and channel overhead, shift all
    failure handling to the application, and do nothing for per-message
    latency under sparse arrivals.

The feature removes the barrier and the serialization at the library level,
where the acknowledgment discipline can be enforced by construction rather
than by convention.

## Goals

*   **G1 — Per-channel concurrency knob.** A `ProcessorConcurrency` field on
    `ConsumerUsageOptions` with a `WithProcessorConcurrency` builder. Zero (or
    negative) selects the sequential path, byte-identical to today; a positive
    value N selects the concurrent path with at most N workers.
*   **G2 — Per-record callback with library-owned acknowledgment.** A new
    public hook `CallbackProcessRecord` (built via
    `WithChannelOptionProcessRecord`) receiving one message and returning a
    `RecordOutcome` (Ack / Nack-requeue / Nack-drop). The library executes
    every outcome as `Ack(tag, false)` / `Nack(tag, false, requeue)` from one
    place, with a non-blocking status re-check immediately before the wire
    call. Worker-issued bulk acks are structurally impossible on this path.
*   **G3 — Latency and parallelism decoupled.** On the concurrent path,
    processing starts at the first delivery's arrival (no batch fill, no fill
    timer) and in-flight concurrency is bounded by N (effectively
    `min(N, PrefetchCount)` under manual acks; the pool alone under
    auto-ack).
*   **G4 — Coexistence.** Batch and record-concurrent consumers run on the
    same connection as separate channels; the sequential path's code, events,
    and contracts are unchanged at zero value.
*   **G5 — Lifecycle guarantees.** The consume loop joins the worker pool
    before returning, preserving "gobble returned implies no outstanding user
    code" in delayed-by-one-record form; workers exit on feed close or context
    cancellation; recovery spawns a new pool per generation.
*   **G6 — Verified, not asserted.** A race-baseline gate (S0-R plus an early
    CI `-race` job) precedes the feature; the latch-semantics pin test lands
    as the feature's first staged item; max-concurrency, shutdown, recovery,
    and bound claims are proven under `-race` against the broker.
*   **G7 — Documented contracts.** ADR-0008 (dispatch model, settle rules,
    lifecycle, bounds, the `multiple=true` definition the repo lacks),
    glossary amendments, and CONTEXT/README/AGENTS syncs land with the code.

## Non-Goals

*   **No changes to the sequential path.** Batch fill, flush timer, flush
    events (`EventDataPartial`/`EventDataExhausted`), the batch hook, and its
    bulk-ack wire pattern stay byte-identical at zero value.
*   **No new event kinds.** The 15 existing `EventType` constants are
    unchanged; no stringer regeneration.
*   **No trace-harness growth for ack behavior.** Ack/Nack/Reject remain
    untraced per ADR-0005; the settle helper is a library-owned choke point,
    not a traced site.
*   **No library-side poison-message handling.** No retry counting, no
    backoff, no requeue suppression — documentation and the drop outcome only.
*   **No per-item error channels, result slices, or cancellation contexts.**
    The record callback receives no context; processors that must honor
    cancellation exit between records.
*   **No processing-order guarantee on the concurrent path.** Invocation order
    is runtime-scheduled; processors must be concurrency-safe above one.
*   **No dynamic pool resizing** (prefetch is static per channel, so there is
    no resize pressure), no priorities, and no runtime model switching (the
    dispatch model is fixed at construction).
*   **No returned-error callback API.** Error/requeue ownership stays with the
    processor via the outcome; a returned-error API remains a named future
    change.

## Open Questions

None open. Resolved during drafting:

*   **Q1 — Gate-block placement:** summarized precondition table plus
    detailed F0–F5 (see the Technical Plan's resolved-decision note).
*   **Q2 — `MsgHandlerSlow` disposition:** adopted as the sequential-path
    slow variant (target 9 in the Detailed Implementation Plan).
*   **Q3 — Example shape:** a new tenth module, `examples/per_record/`, with
    the "nine" count references updated to ten.

## Critical User Journeys (CUJs)

### CUJ 1: Parallel processing of a busy queue

*   **Given**: An application builds a consumer with
    `NewConsumer(conn, DefaultConsumerOptions().WithQueue("jobs").WithPrefetchCount(8), WithProcessorConcurrency(8))`
    — using the channel-level builder `WithChannelOptionProcessRecord` — and a
    record callback that decodes, processes, and returns `RecordAck` or
    `RecordNackRequeue`.
*   **When**: Deliveries arrive at the consumer channel.
*   **Then**: Processing starts at the first delivery's arrival (no batch
    fill, no fill timer); at most 8 records are processed concurrently; each
    record is settled by the library as an individual `multiple=false` wire
    call per its outcome; no `EventDataPartial`/`EventDataExhausted` events
    fire on this path.

### CUJ 2: Sequential consumer unchanged at zero value

*   **Given**: An application builds a consumer exactly as today (no
    concurrency option set, `ProcessorConcurrency == 0`).
*   **When**: Deliveries arrive.
*   **Then**: Behavior is byte-identical to the current release: batch fill,
    flush timer, `EventDataPartial`/`EventDataExhausted`, the batch hook, and
    the barrier-then-bulk-ack wire pattern.

### CUJ 3: Mixed models on one connection

*   **Given**: Two consumers share one `Connection` — a sequential batch
    consumer on topic A's queue, a record-concurrent consumer on topic B's
    queue — with `QosGlobal` left at its `false` default.
*   **When**: Both consume concurrently.
*   **Then**: Each channel runs its own dispatch model with no shared
    dispatch state; neither model's throughput depends on the other's
    callback runtime.

### CUJ 4: Broker connection loss mid-processing

*   **Given**: A record-consumer has records in flight when the broker
    connection drops.
*   **When**: The channel recovers (new generation spawns at
    `channel_notifiers.go:56`).
*   **Then**: Old-generation workers exit on feed close; the new generation's
    pool starts; records left unacked are redelivered by the broker
    (at-least-once); the settle-time re-check prevents old-generation tags
    from being acked against the new channel (narrowed, not closed — the
    worst case is a misdirected single-tag ack, documented in ADR-0008).

### CUJ 5: Shutdown with workers in flight

*   **Given**: A record-consumer is processing when the application calls
    `Close()` or cancels the context.
*   **When**: The consume loop tears down.
*   **When**: Workers exit between records (feed close or `ctx.Done()`);
    `gobble` joins the pool before returning — "gobble returned implies no
    outstanding user code" holds, delayed by at most one record. In-flight
    records are left unacked and redeliver.

## Technical Plan (Architecture)

### Dispatch-model selection (per channel, fixed at construction)

The selector is a new field `ProcessorConcurrency` on `ConsumerUsageOptions`
(`consumer_options.go`), reaching the channel through the existing wholesale
struct copy in `NewConsumer` (`consumer.go:53-55`) — zero constructor code
change. `0` (or negative) selects the sequential path: byte-identical
behavior. `> 0` selects the concurrent path with N workers. The model is
fixed at construction; there is no runtime switching. The selection branch
sits at the gobble spawn site (`channel_notifiers.go:56`): zero spawns
today's loop unchanged, positive spawns the concurrent loop.

### Public API surface (all additive)

| Addition | Location | Purpose |
| --- | --- | --- |
| `ProcessorConcurrency` field | `consumer_options.go` | Concurrency knob, zero = off |
| `WithProcessorConcurrency(n)` builder | `consumer_options.go` | Two-branch naming convention (plain field keeps its name) |
| `CallbackProcessRecord` type | `general_options.go` | `func(props *DeliveriesProperties, message DeliveryData) RecordOutcome` |
| `WithChannelOptionProcessRecord(cb)` builder | `channel_options.go` | Mirrors `WithChannelOptionProcessor` |
| `RecordOutcome` type + `RecordAck` / `RecordNackRequeue` / `RecordNackDrop` | `general_options.go` + generated stringer | Outcome enum; `RecordNackRequeue` is the zero value (fail-safe: unset outcome redelivers rather than acks) |

The record callback deliberately receives **no `*Channel`** and **no
`mustAck`**: excluding the channel is what makes library-owned acknowledgment
enforceable rather than conventional (workers cannot issue `multiple=true`
acks or bypass the settle path — there is no handle to do it with). The batch
hook stays intact for serial users and for anyone who needs channel access.

### Concurrent dispatch machinery

The concurrent loop replaces accumulation with a worker-pull pool:

```text
                     ┌────────────────────────────────────────────┐
                     │ Channel (concurrent mode, knob = N)        │
                     │                                            │
 amqp deliveries ───►│ deliveries feed ──┐                        │
 (≤ PrefetchCount    │                   │ worker-pull (N workers)│
  unacked,           │        ┌──────────┼──► worker 1 ─┐         │
  broker-enforced)   │        │          ├────────► worker 2 ─┤        │
                     │        │          └────────► …      │        │
                     │        │                            ▼        │
                     │        │              RecordOutcome          │
                     │        │                            │        │
                     │        │              ┌─────────────▼──────┐ │
                     │        │              │ settle helper      │ │
                     │        │              │ 1. re-derive       │ │
                     │        │              │    ack-worthiness  │ │
                     │        │              │ 2. non-blocking    │ │
                     │        │              │    latch re-check  │ │
                     │        │              │ 3. Ack/Nack        │ │
                     │        │              │    (tag, false)    │ │
                     │        │              └────────────────────┘ │
                     │  ctx.Done() ──┐                             │
                     └───────────────┼─────────────────────────────┘
                                     ▼
                        workers exit between records;
                        gobble joins the pool (WaitGroup)
                        before returning
```

*   **Worker-pull, no dispatcher.** Workers read the deliveries feed
    directly — no internal work channel, no dispatcher goroutine. In-flight
    concurrency is at most N by construction.
*   **Pool per generation, join before return.** The pool spawns with gobble
    per recovery generation and is joined with a WaitGroup before gobble
    returns, preserving "gobble returned implies no outstanding user code"
    in delayed-by-one-record form. Recovery does not cancel the channel
    context (only `Close` does) — feed close is the generation-end signal
    for workers. Old-generation workers exit asynchronously: two generations
    transiently run up to 2N workers (documented, per-generation bound).
*   **The settle helper (unexported, single).** The only place executing an
    outcome: (1) re-derives ack-worthiness per record from
    `!ConsumerAutoAck` — nothing sticky crosses records; (2) re-checks the
    connected latch non-blockingly immediately before the wire call — a
    transition skips the call and leaves the record unacked for redelivery
    (the ADR-0004 duty, executed library-side); (3) issues exactly one of
    `Ack(tag, false)` / `Nack(tag, false, true)` / `Nack(tag, false, false)`.
    All wrapper calls serialize on `baseChan.mu`, the same mutex recovery
    uses — concurrent acks are legal and serialized. Not traced (ADR-0005
    carve-out stands).
*   **Stale-tag status.** A rebase between re-check and wire call remains
    possible (TOCTOU); with `multiple=false` the worst case is a single
    misdirected tag — narrowed, not closed. A latch-semantics pin test lands
    before any code leans on the transition counter; a mu-scoped
    check-and-ack wrapper is recorded as optional hardening.

### Defaults, combination rules, and the bound

*   **Default record processor** (used when concurrency is on but no callback
    is registered): returns `RecordAck` per record; raises
    `EventMessageReceived` with `Err` set only on failure — the same
    Err-only-on-failure convention corrects the batch default processor in
    the same stage.
*   **Combination rules:** `ProcessorConcurrency > 0` ignores a registered
    batch hook on that channel; `0` leaves a registered record callback
    unused. Both builders' doc comments state the rule.
*   **Bound:** in-flight concurrency ≤ N by construction; under manual ack
    (the default) the broker's prefetch additionally caps unacked deliveries,
    so effective parallelism is `min(N, PrefetchCount)`; under
    `ConsumerAutoAck(true)` the broker applies no prefetch throttle and the
    pool is the only grabbit-owned bound. `Qos` is never auto-adjusted (a
    trace pin asserts its exact parameters).
*   **Retirement set on the concurrent path:** batch fill and the
    accumulation slice; the `PrefetchTimeout` timer arm; the flush events;
    the sticky `mustAck` variable; per-batch props sharing (each record gets
    fresh properties via `DeliveryPropsFrom`; `props.Headers` stays a
    shared-map reference, read-only). There is no queue-drain event on the
    concurrent path — completion is observed through user-side registries.

### What is deliberately not built

No dispatcher goroutine or work channel (the bound holds by construction
with worker-pull; a bounded channel adds a close protocol and leak surface
for a read-ahead guarantee the AMQP dependency's internal buffering blurs
either way). No worker-per-delivery spawning (concurrency would become
prefetch-bounded instead of N-bounded). No poison-message machinery (no
retry counts, backoff, or suppression — processors detect the loop via
`DeliveryData.Redelivered` and route to `RecordNackDrop`; ADR-0008 carries
the paragraph). No runtime model switching, no pool resizing.

### Resolved decision (Q1): gate block as summarized precondition

The implementation plan lists the gate block as a compact summary table (one
row per stage: Pre-S0, S0-R, S0, S1, S5, S4, observation-B guard, CI race
job) treated as a precondition, with the feature stages F0–F5 detailed in
full. Rationale: the design doc must present a single unified list of
everything that must happen (the gate block is part of that list), but
restating the remediation plan's detail would duplicate a living document and
drift; the summary table binds this document to the brief's stage list
without restaging it.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Batch fan-out over sub-slices

*   **Pros**: Reuses the existing batch hook and `DeliveryData` grouping; no
    new callback type; dispatch reuses the two existing flush points.
*   **Cons**: Batch width is fan-out width — a large batch delays the first
    item by the fill, a small batch collapses the fan-out, so no timeout
    value gives both latency and parallelism. Every invocation receives the
    channel handle and can issue `multiple=true` acks the library cannot
    intercept. Partial batches make the real bound
    `min(knob, len(batch))`.
*   **Reason for Rejection**: Replaced by per-record dispatch, which
    decouples concurrency (pool size) from latency (per delivery). The
    batch-fan-out machinery is deleted rather than built.

### Alternative B: User-side parallelism inside the existing batch callback

*   **Pros**: Zero new API; works today; the join already exists at callback
    return; the broker's prefetch bounds outstanding work.
*   **Cons**: No context accessor on `Channel`, so user workers have no clean
    abort signal; every project reimplements the pool; no lifecycle guard;
    the bulk-ack hazard remains reachable; the batch-fill barrier still
    delays the first message.
*   **Reason for Rejection**: Retained only as the documented fallback for
    users who need nothing more. The feature exists because acknowledgment
    discipline and lifecycle can be enforced by the library and not by
    convention in user code.

### Alternative C: Bounded work-channel dispatcher

*   **Pros**: Bounds read-ahead at N+1 under auto-ack; a single dispatcher
    makes fan-out policy explicit.
*   **Cons**: The promised bound (at most N concurrent callbacks) already
    holds by construction when workers pull the feed directly; a dispatcher
    adds a goroutine, a close protocol, and another channel to leak-check;
    the AMQP dependency's internal feed buffering blurs the read-ahead
    guarantee either way.
*   **Reason for Rejection**: Complexity without a contract the worker-pull
    design cannot already state. Recorded so it is not re-derived.

### Alternative D: Worker-per-delivery (spawn a goroutine per message)

*   **Pros**: Simplest spawn logic; maximum responsiveness.
*   **Cons**: Concurrency becomes prefetch-bounded — and unbounded under
    auto-ack — instead of N-bounded; contradicts the knob's documented
    contract; goroutine churn under bursts.
*   **Reason for Rejection**: Unbounded concurrency under auto-ack is the
    exact failure the knob exists to prevent.

### Alternative E: Record callback receives the channel handle

*   **Pros**: Publish-from-processor convenience; convention symmetry with
    the batch hook; a raw escape hatch.
*   **Cons**: Re-opens the cross-worker ack hazard the design exists to
    close; bulk acks become possible again and cannot be intercepted; raw
    `Super()` use inside workers becomes reachable.
*   **Reason for Rejection**: Exclusion is what makes library-owned
    acknowledgment enforceable rather than conventional. Closures capture
    whatever dependencies a processor needs (a publisher, a client); users
    who need channel access keep the batch hook.

### Alternative F: `mustAck` as a callback parameter

*   **Pros**: Parity with the batch hook; recovery state visible to the
    processor.
*   **Cons**: Drags the two contradictory in-repo `!mustAck` conventions
    into the new API (one set of tests abandons processing; the default
    processor processes and skips the ack; ADR-0004 mandates
    process-and-leave-unacked); puts a library decision onto every user.
*   **Reason for Rejection**: The duty moves library-side; users learn of
    transitions through `EventDown` on the notifier channel.

### Alternative G: Outcome as a bool, a struct of bools, or a Reject primitive

*   **Pros**: A bool is the smallest surface; a struct admits combinations;
    `Reject` is a distinct wire primitive.
*   **Cons**: A bool cannot express "drop" without escaping the library's
    ack authority; a struct of bools admits invalid states; `Reject(tag,
    false)` is wire-equivalent to `Nack(tag, false, false)` and would add a
    third code path.
*   **Reason for Rejection**: A three-constant `RecordOutcome` maps to
    exactly two wire primitives through one settle site.

### Alternative H: Plain constants without a stringer for `RecordOutcome`

*   **Pros**: No generated file; no guard-test row; smaller surface.
*   **Cons**: All three existing public enums carry stringer output and
    guard tests; a fourth without one would be a half-precedent; log lines
    would print integers.
*   **Reason for Rejection**: Follow the repository precedent; the cost is
    one generated file and one guard test.

### Alternative I: Trace parameters at the settle site

*   **Pros**: Ack traffic observable through the existing trace harness.
*   **Cons**: Requires new trace pairs, nop/testing twins, and traceutils
    contract rows; reverses a twice-settled non-goal (ADR-0005 excludes
    `Ack`/`Nack`/`Reject` from tracing by design).
*   **Reason for Rejection**: Observability is served by the advisory
    `EventMessageReceived` event and processor-side instrumentation.

### Alternative J: Validate `ProcessorConcurrency` and `PrefetchCount` at the builder

*   **Pros**: Fail fast on nonsensical values (negative concurrency, zero
    prefetch with concurrency on).
*   **Cons**: No builder in the option family validates; inconsistent with
    `WithPrefetchCount`, which accepts anything today; `ClampInt` exists but
    is not used for options anywhere.
*   **Reason for Rejection**: Document the semantics instead (negative = off;
    `PrefetchCount ≤ 0` unsupported on the concurrent path) rather than
    introduce the family's first validation.

### Sequencing alternatives (rejected)

*   **Ship before the remediation release**: Rejected — goroutines would land
    before any race verification exists and before the recovery/topology
    contracts settle, inverting the purpose of the gate block.
*   **Ride the remediation batched release**: Rejected — mixes a breaking
    corrections release with an additive feature, adds schedule pressure, and
    blurs the semver signal.
*   **Scale with competing consumers instead of a library pool**: Rejected
    for this feature — it multiplies connections and consumers, shifts all
    failure handling to the application, and does nothing for the
    batch-fill latency. It remains the right answer for whole-queue
    throughput and stays documented as such.

--------------------------------------------------------------------------------

## Non-Functional Requirements

*   **Performance and latency**: Processing concurrency is bounded at N by
    construction (`min(N, PrefetchCount)` under manual acks). First-record
    processing starts at arrival on the concurrent path, removing the
    batch-fill barrier. Acks serialize on `baseChan.mu` by design, so N
    workers create N waiters where the serial path has one — this is a
    throughput/latency trade, not a safety gap, and it is stated in
    ADR-0008. The sequential path is byte-identical and carries no
    measurable change.
*   **Reliability (at-least-once)**: Unsettled records — skipped writes on a
    recovery transition, in-flight records at shutdown, records lost to a
    crash — are redelivered by the broker. The stale-tag window is narrowed
    (settle-time re-check), not closed; the documented worst case is a
    single misdirected tag on the new channel. Poison records can loop at
    wire speed when requeued; processors detect this via
    `DeliveryData.Redelivered` and choose the drop outcome.
*   **Rollout and rollback**: Additive public surface; the zero value
    preserves existing behavior exactly. Rollout is a normal minor release.
    Rollback means unsetting the option — no schema, protocol, or data
    migration exists.
*   **Security**: No new network surface, credentials, or serialization
    format. Record callbacks receive the same delivery data the batch hook
    already receives.
*   **Observability**: No new event kinds and no trace-harness growth
    (ADR-0005 excludes ack/nack/reject tracing). Operators observe the
    concurrent path through `EventMessageReceived` (advisory, lossy) and
    their own processor instrumentation; completion gating in tests moves to
    user-side registries.
*   **Compatibility**: Go 1.25.7 and `amqp091-go` v1.10.0 as pinned today.
    No existing exported identifier changes shape; the batch hook and
    `ConsumerOptions` remain valid.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Additional Resources

*   `./concepts_brief.md` — the 2026-11-23 ideation brief (DR1–DR6,
    alternatives, staged order).
*   `./concepts_brief_remediation_audit.md` — the remediation plan the gate
    block comes from.
*   `./docs/adr/0003-recovery-redeclares-all-declared-topology.md` — the
    recovery/re-declaration contract (S4 gate).
*   `./docs/adr/0004-*.md` — the redelivery/process-and-leave-unacked
    contract the settle-time re-check follows.
*   `./docs/adr/0005-*.md` — the trace carve-out for ack/nack/reject.
*   `./CONTEXT.md`, `./GLOSSARY.md`, `./AGENTS.md` — the project's
    agent-facing contracts and controlled vocabulary.

## Future Improvements

*   **The idea**: a returned-error callback API, so processors can express
    failure without choosing an outcome constant. Deferred; the outcome enum
    covers the current need.
*   **The idea**: a mu-scoped check-and-ack wrapper to close (not just
    narrow) the stale-tag window. Adopt only if the pinned latch semantics
    reveal a live window in practice.
*   **The idea**: a contiguous-success watermark ack strategy — fewer wire
    frames, more code. Declined unless a user asks.
*   **The idea**: dynamic pool resizing. No pressure today: prefetch is
    static per channel.
*   **The idea**: library-side poison handling (retry counting, backoff,
    dead-letter helpers). Currently documentation and the drop outcome only.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Gate block (preconditions — summarized per Q1)

| Stage | What happens | Files |
| --- | --- | --- |
| Pre-S0 | Trigger `main`; add untagged build step; first CI run | `.github/workflows/coverage.yaml`, `.github/workflows/codeql.yml` (verify trigger) |
| S0-R | Local `-race` baseline against the container; races block the feature | none (recorded result) |
| S0 | Baseline build, vet, full suite | none |
| S1 | Copy-first option append in both constructors | `publisher.go`, `consumer.go`, `CONTEXT.md` |
| S5 | Publisher default-callback `Err` only on failure | `publisher.go`, `CONTEXT.md` |
| S4 | Durable topology re-declared on recovery (test-first) | `channel.go`, `channel_topology_test.go` |
| Observation-B | Manage-loop wait bounded via `delayerCompleted`; ADR-0006 | `connection.go`, `docs/adr/0006-*.md` (create) |
| CI `-race` job | Race-enabled suite runs in CI | `.github/workflows/` (new job or workflow) |

The feature stages below start only after S0-R, S1, S4, and the CI `-race`
job are green (per the approved Technical Plan and the brief's staged order).

### Stage order

*   **F0** — decisions recorded (already satisfied by the brief; no code).
*   **F1** — public API surface.
*   **F2** — concurrent dispatch machinery, latch-semantics pin test first.
*   **F3** — behavioural test suite under `-race`.
*   **F4** — documentation and ADR sync.
*   **F5** — tenth example module and release packaging.

### Target files to create or modify

#### 1. `./consumer_options.go` (Modify)

*   **Type / Scope**: Public option surface (Go library).
*   **Changes**: Add `ProcessorConcurrency int` to `ConsumerUsageOptions`
    (inside the embedded struct — a field on `ConsumerOptions` outside it
    never reaches the channel). Add the `WithProcessorConcurrency(n int)`
    builder following the two-branch naming convention. Fix the misleading
    `QosGlobal` field comment ("all future channels") to state that global
    QoS shares the window across the connection's channels. WARNING-grade
    doc comment on the builder: inert at default prefetch (1);
    `PrefetchTimeout` and flush events retire when set; negative means off;
    `PrefetchCount ≤ 0` is unsupported on the concurrent path.
*   **Why**: The knob and its documented semantics are the feature's entry
    point; the field must ride the embed to reach the channel through the
    existing wholesale struct copy.

#### 2. `./general_options.go` (Modify)

*   **Type / Scope**: Public callback and enum types.
*   **Changes**: Add
    `type CallbackProcessRecord func(props *DeliveriesProperties, message DeliveryData) RecordOutcome`
    beside the existing callback types. Add `type RecordOutcome` with
    `RecordAck`, `RecordNackRequeue` (zero value), `RecordNackDrop`.
    Document that `props.Headers` is a shared map and read-only. No `*Channel`
    and no `mustAck` parameters.
*   **Why**: Callback types live in this file by convention; the zero value
    must mean "redeliver" so a processor that returns without deciding never
    silently acks.

#### 3. `./channel_options.go` (Modify)

*   **Type / Scope**: Channel option plumbing.
*   **Changes**: Add a `cbProcessRecord CallbackProcessRecord` field to
    `ChannelOptions` and a `WithChannelOptionProcessRecord(cb)` builder
    mirroring `WithChannelOptionProcessor`. Document the combination rule:
    with `ProcessorConcurrency > 0` the batch hook is ignored; at 0 the record
    callback is unused.
*   **Why**: The record callback travels the existing callback path
    (optionFuncs into the channel), not the usage-params embed.

#### 4. `./channel.go` (Modify)

*   **Type / Scope**: Channel construction defaults.
*   **Changes**: In `NewChannel`, add the default
    `cbProcessRecord: defaultRecordProcessor` beside the existing default
    batch processor. Nothing else changes here.
*   **Why**: "Concurrency on, no callback registered" must still work and
    ack by default; the default belongs where the channel's other defaults
    are set.

#### 5. `./channel_notifiers.go` (Modify)

*   **Type / Scope**: Per-generation consume spawn.
*   **Changes**: At the existing spawn site, branch on
    `implParams.ProcessorConcurrency`: zero/negative spawns the existing
    `gobble` unchanged; positive spawns the concurrent loop. Pass the
    resolved record callback.
*   **Why**: This is the one place a consume loop is created per recovery
    generation; branching here keeps both models on the same lifecycle.

#### 6. `./channel_consume.go` (Modify)

*   **Type / Scope**: Core dispatch machinery.
*   **Changes**: Add the concurrent dispatch loop and the settle helper. The
    loop spawns N workers that pull the deliveries feed directly, selects on
    exactly the feed and `ctx.Done()`, and joins the pool with a WaitGroup
    before returning. The settle helper re-derives ack-worthiness per record,
    re-checks the connected latch non-blockingly immediately before the wire
    call, and issues exactly one of `Ack(tag,false)` / `Nack(tag,false,true)` /
    `Nack(tag,false,false)`. Leave the existing `gobble` function untouched.
*   **Why**: This is the feature's core; keeping `gobble` untouched is what
    makes "byte-identical at zero value" checkable.

#### 7. `./consumer.go` (Modify)

*   **Type / Scope**: Default processors and constructor hygiene.
*   **Changes**: Add `defaultRecordProcessor` (returns `RecordAck`; raises
    `EventMessageReceived` with `Err` only on failure) and correct
    `defaultPayloadProcessor` to set `Err` only on failure. Also the S1
    copy-first append lands here (gate stage).
*   **Why**: The default record processor serves the no-callback case; the
    batch default's false `Err` is the consumer-side defect folded into F2.

#### 8. `./recordoutcome_string.go` (Create) and `./recordoutcome_string_test.go` (Create)

*   **Type / Scope**: Generated stringer and guard test.
*   **Changes**: Generate the `String()` method for `RecordOutcome` with
    `stringer`, matching the three existing enum stringers. Add the guard
    test that fails when the generated file is stale, mirroring the existing
    stringer guard tests.
*   **Why**: All three public enums carry stringers and guards; a fourth
    without them would be a half-precedent, and log output would print
    integers.

#### 9. `./consumer_test.go` (Modify)

*   **Type / Scope**: Broker-backed behavioural tests.
*   **Changes**: Add the record-mode scenarios: tracer bullet (one worker,
    individual settle), N-worker bound, lifecycle/shutdown, recovery
    transition, retirement assertions (no flush events, no timer), poison
    loop, generation overlap. Adopt the dead `MsgHandlerSlow` helper as the
    sequential-path slow variant (Q2 decision). Fix the "chunk size" comment
    drift.
*   **Why**: Every claim in the Technical Plan needs a behavioural test under
    `-race`; completion gating moves to the registry pattern because the
    drain event retires on the concurrent path.

#### 10. `./container_setup_test.go` (Modify)

*   **Type / Scope**: Test infrastructure.
*   **Changes**: Add a max-concurrency tracker (`Enter`/`Exit` recording the
    peak, with a `MaxEquals(n)` predicate compatible with `ConditionWait`)
    and a join/drain helper that does not leak its waiter on timeout. Do not
    reuse `wgDoneOrTimeout`.
*   **Why**: The bound claim and the shutdown claim are unverifiable without
    them; the existing join helper self-leaks on timeout by its own comment.

#### 11. `./CONTEXT.md` (Modify)

*   **Type / Scope**: Project documentation.
*   **Changes**: Options list; delivery-batching interface (gains the record
    callback); defaults; ack-gate (gains the per-record rule); batching
    (splits per path); doc warnings (concurrency WARNINGs); default callbacks
    (consumer half, sequenced with S5's publisher half); stringer guards
    (`RecordOutcome` row); goroutine side effects (worker pool); example
    counts (nine → ten).
*   **Why**: CONTEXT.md is the project's agent-facing contract; every option
    and behavior it describes must match the code.

#### 12. `./GLOSSARY.md` (Modify)

*   **Type / Scope**: Controlled vocabulary.
*   **Changes**: Split "Processor" into batch and record shapes; add a
    "Processor concurrency" entry; scope "Delivery batch" to the sequential
    model; add an acknowledgment entry that finally defines `multiple`.
*   **Why**: The glossary lacks any ack definition, and the feature
    introduces the exact vocabulary users need to reason about it.

#### 13. `./README.md` (Modify)

*   **Type / Scope**: Public documentation.
*   **Changes**: Add the per-record goal beside the batch-processing goal;
    add a one-sentence carve-out to the non-goal about concurrency safety
    (the library now invokes user processor code concurrently — the non-goal
    remains about the client objects' own safety).
*   **Why**: The current non-goal reads as if the library never runs user
    code concurrently; that becomes false on the concurrent path.

#### 14. `./AGENTS.md` (Modify)

*   **Type / Scope**: Agent instructions.
*   **Changes**: Add `WithProcessorConcurrency` and
    `WithChannelOptionProcessRecord` to the consumer options list; add the
    auto-ack bound and the poison-requeue warning to the QoS/prefetch
    section; note that `PrefetchTimeout` and flush events do not apply on the
    concurrent path.
*   **Why**: Future agent sessions must not tune concurrency against a
    secret size-1 batch or expect flush events that never fire.

#### 15. `./docs/adr/0008-per-channel-dispatch-model-sequential-by-default-and-concurrent-by-option.md` (Create)

*   **Type / Scope**: Architecture decision record.
*   **Changes**: Record the two models and the selector; the record surface
    and wire mapping; the per-record mustAck rule and settle-time re-check
    (narrowed, not closed; cross-referencing ADR-0004); the lifecycle
    contract (per-generation pool, join-before-return, 2N overlap, no
    user-work abort); the bounds per ack mode; the `QosGlobal` caveat; the
    poison paragraph; the retirement set; and the one-sentence `multiple=true`
    definition.
*   **Why**: The behavior default and its consequences need a durable
    decision record; the repo's ADR set is one decision per file.

#### 16. `./docs/adr/0006-*.md` (Create — gate stage)

*   **Type / Scope**: Architecture decision record for the observation-B
    guard.
*   **Why**: The guard changes connection lifecycle behavior; ADR-0006 is
    the number the remediation plan reserved.

#### 17. `./examples/consumer_batch/CONTEXT.md` (Modify)

*   **Type / Scope**: Example documentation fix.
*   **Changes**: Correct the guard's mislabel — it is a recovery flush guard,
    not an "auto-ack mode guard".
*   **Why**: The mislabel teaches the wrong contract to anyone porting the
    example.

#### 18. `./examples/CONTEXT.md` (Modify)

*   **Type / Scope**: Example index.
*   **Changes**: Add the new tenth module row (name, what it shows, run
    command); update "nine examples" wording to ten.
*   **Why**: The examples index is the discovery surface for runnable
    demonstrations.

#### 19. `./examples/per_record/` (Create — module)

*   **Type / Scope**: New example module (`go.mod`, `go.sum`, `main.go`,
    `CONTEXT.md`).
*   **Changes**: A runnable demonstration of the record hook: a concurrent
    consumer on one queue, a record callback with explicit
    ack / nack-requeue / nack-drop branches (e.g., drop after a redelivery
    flag), and a deliberately slow handler so overlap is observable. Own
    module, matching the other nine.
*   **Why**: Q3 decision — a separate module keeps the batch example
    unchanged and gives the concurrent model a standalone entry point.

#### 20. `.github/workflows/coverage.yaml` (Modify — gate stage)

*   **Type / Scope**: CI.
*   **Changes**: Trigger `main` instead of `master`; add an untagged
    `go build ./...` step.
*   **Why**: The workflow never fires on the active branch today.

#### 21. `.github/workflows/` (Create — gate stage)

*   **Type / Scope**: CI.
*   **Changes**: Add the `-race` broker-suite job (own commit, per DR2).
*   **Why**: No workflow contains "race" today; the feature adds goroutines.

#### 22. `./publisher.go` (Modify — gate stages S1 and S5)

*   **Type / Scope**: Constructor hygiene and default callback.
*   **Changes**: Copy the variadic option slice before appending (S1); set
    `Err` only on failure in the default notify-publish callback (S5).
*   **Why**: Both are remediation-stage corrections that the feature's
    plumbing and documentation edits sit beside.

#### 23. `./channel_topology_test.go` (Modify — gate stage S4)

*   **Type / Scope**: Test.
*   **Changes**: Flip the topology test to expect durable re-declaration on
    recovery; watch it fail; then the `channel.go` fix turns it green.
*   **Why**: Test-first ordering pins the intended contract before the fix.

#### 24. `./connection.go` (Modify — gate stage observation-B)

*   **Type / Scope**: Connection lifecycle.
*   **Changes**: Replace the observation-B wait with the `delayerCompleted`
    idiom.
*   **Why**: Removes the pre-existing manage-loop leak so goleak verdicts in
    feature tests are attributable.

#### 25. `./channel_latch_test.go` (Create — F2's first item)

*   **Type / Scope**: Unit test (no broker).
*   **Changes**: Pin the `connected` latch's `Details()` transition-counter
    semantics that the settle helper's re-check relies on — including whether
    the establishment `Unlock` counts as a transition. The test documents the
    count observed at: fresh latch, establishment, and a forced
    disconnect/reconnect cycle (latch-only, no broker).
*   **Why**: The counter's behavior is internal to the latch dependency and
    pinned by nothing in this tree; no production code may lean on the
    transition check before this test exists.

### Explicitly not created

*   No dispatcher goroutine or internal work channel (Alternative C).
*   No new event kinds or stringer count changes.
*   No trace parameters for ack/nack.
*   No `CHANGELOG` file (none exists today); release notes are written to the
    release page, or a changelog can be introduced separately if desired.

### Verification plan

*   **Build**: `go build ./...` — library and all example modules
    (`for d in examples/*/; do (cd "$d" && go build ./...); done`).
*   **Vet**: `go vet ./...`.
*   **Format**: `gofmt -l .` returns nothing; `go fmt ./...`.
*   **Unit tests (no broker)**: `go test -tags test_env ./...` for the
    option/type/stringer layers.
*   **Race suite (broker required)**: `go test -race -tags test_env ./...`
    against the LavinMQ container — the authoritative gate.
*   **CI command**: `go test -tags test_env -coverprofile coverage.out -v ./...`
    (matches the existing workflow) plus the new `-race` job.
*   **Behavioural scenarios** (all under `-race`): tracer bullet; N-worker
    bound measured by the max-concurrency tracker; shutdown with workers in
    flight (goleak + `AwaitManager`); recovery transition with redelivery;
    retirement assertions (no flush events, no timer); poison loop finiteness
    and stable goroutine count; 2N generation overlap.
*   **Stringer guards**: `go test -run TestRecordOutcomeString ./...`
    (or the repo's equivalent naming) fails on stale generated output.
*   **Coverage**: the repo enforces no percentage; CI publishes the report.
    New public surfaces (option builder, callback builder, outcome enum,
    settle paths) must appear as covered in the report.

--------------------------------------------------------------------------------

*Drafted with EGM*
