# Ticket Breakdown — Concurrent Consumer Processing (Per-Record Dispatch)

*   **Source:** `concepts_brief.md` (2026-11-23, DR1–DR6 settled) and
    `design_draft.md` (Technical Plan approved).
*   **Author:** LucaWolf
*   **Date:** 2026-11-23
*   **Target release:** fast-follow minor after the remediation batched
    release (DR4).

Gate tickets (T1–T8) are preconditions. Feature tickets (T9–T15) are the
deliverable. `[B]` = requires the broker (LavinMQ container via `TestMain`).

## Dependency overview

```text
T1 ──► T2 ──► T8
 │
 ├──► T3 ──► T4 ──► T5
 │            └────► T10 ──► T11 ──► T12 ──► T13 ──► T14 ──► T15
 ├──► T6 ─────────────────────► T11
 └──► T7
```

T6 (S4) is a hard gate for T11/T12; T8 (CI race job) is waived in favor of
local race runs (2026-11-24). T2/T3 are closed on local evidence (T1 skipped by
owner decision).

---

## Gate block

### T1 — Pre-S0: repair the CI trigger and add an untagged build step

*   **Type:** Chore (CI). **Size:** S. **Depends on:** none.
*   **Files:** `.github/workflows/coverage.yaml`.
*   **Change:** trigger branch `master` → `main`; add an untagged
    `go build ./...` step before the test step.
*   **Acceptance:** the workflow runs on a push to `main` for the first time;
    the build step is green; runner/podman issues (engine is hard-coded
    `podman`) are recorded or fixed.
*   **Notes:** first-ever suite execution in CI — treat as a discovery step.

### T2 — S0-R: local race baseline

*   **Type:** Verification. **Size:** S–M. **Depends on:** T1. `[B]`
*   **Files:** none (record the baseline in the ticket).
*   **Change:** run `go test -race -tags test_env ./...` locally against the
    container; record the result.
*   **Acceptance:** baseline is green, or every discovered race is filed as a
    blocking remediation ticket before any feature work starts.
*   **Notes:** the feature adds worker goroutines to the latch-reading
    dispatch surface, which raises this stage's value.
*   **Status:** closed 2026-11-24 — `go test -race -tags test_env ./...` green
    locally (61.6s), no races found. Closed by owner decision without T1; CI
    verification remains T8.

### T3 — S0: baseline build/vet/test

*   **Type:** Verification. **Size:** S. **Depends on:** T1. `[B]`
*   **Files:** none.
*   **Change:** `go build ./... && go vet ./...` plus the full suite.
*   **Acceptance:** all green on the post-T1 tree.
*   **Status:** closed 2026-11-24 — `go build ./...`, `go vet ./...`, and the
    full tagged suite green locally (67.2s). Closed by owner decision without
    T1.

### T4 — S1: copy-first option append (plumbing hygiene)

*   **Type:** Bug fix. **Size:** S. **Depends on:** T3.
*   **Files:** `publisher.go` (line 82), `consumer.go` (line 56), `CONTEXT.md`
    (rows `:211-213`, `:234-237`).
*   **Change:** copy the variadic option slice before appending, in both
    constructors, so caller-owned slices cannot be mutated.
*   **Acceptance:** build/vet green; full suite green `[B]`; CONTEXT rows
    updated.
*   **Notes:** the feature's knob rides this exact append — sequence first.

### T5 — S5: publisher default-callback Err correction

*   **Type:** Bug fix. **Size:** S. **Depends on:** T4.
*   **Files:** `publisher.go` (lines 40–54), `CONTEXT.md` (row `:214-218`,
    publisher half only).
*   **Change:** set `Err` only on failure in the default notify-publish
    callback, matching the `makeTopology` precedent.
*   **Acceptance:** targeted publisher scenarios then full suite `[B]`;
    CONTEXT row's publisher half corrected.
*   **Notes:** the consumer half of that row belongs to T11 (sequenced).

### T6 — S4: re-declare durable topology on recovery (hard gate)

*   **Type:** Bug fix (recovery). **Size:** M. **Depends on:** T3. `[B]`
*   **Files:** `channel.go` (line 336 branch + dead-parameter fold-in),
    `channel_topology_test.go` (test at `:19`, failure at `:219`).
*   **Change:** test-first: flip the topology test to expect durable
    re-declaration, watch it fail, then remove the skip-durable branch so
    `makeTopology` re-declares everything declared.
*   **Acceptance:** the flipped test passes; full suite `[B]`; doc sweep for
    the ADR-0003 contract.
*   **Notes:** hard gate before T12's recovery tests (test isolation).

### T7 — Observation-B guard: bound the connection manage-loop wait

*   **Type:** Fix + ADR. **Size:** S–M. **Depends on:** T3. `[B]`
*   **Files:** `connection.go` (lines 198–204), `docs/adr/0006-*.md` (new).
*   **Change:** replace the observation-B wait with the `delayerCompleted`
    idiom; write ADR-0006; add a cancellation-before-establishment test.
*   **Acceptance:** test passes `[B]`; goleak verdict at suite exit is
    attributable to feature work or nothing.
*   **Notes:** upstream of the channel; rationale is leak attribution, not
    recovery behavior.

### T8 — CI `-race` job

*   **Type:** Chore (CI). **Size:** S. **Depends on:** T2. `[B]`
*   **Files:** `.github/workflows/` (new job or new workflow).
*   **Change:** add the race-enabled broker suite as its own commit/job.
*   **Acceptance:** job green on `main`; runtime recorded.
*   **Notes:** must land before T11/T12 develop.
*   **Status:** waived 2026-11-24 — the owner runs
    `go test -race -tags test_env ./...` locally; GitHub CI integration is
    deferred. The hard-gate role for T11/T12 is waived with it.

---

## Feature block

### T9 — F0: decision record (no code)

*   **Type:** Documentation. **Size:** XS. **Depends on:** none.
*   **Files:** `concepts_brief.md` (already carries DR1–DR6).
*   **Acceptance:** DR1/DR3 amended, DR5/DR6 added — satisfied by the
    2026-11-23 brief; close this ticket as a traceability stub.

### T10 — F1: option and callback surface

*   **Type:** Feature (API). **Size:** M. **Depends on:** T4.
*   **Files:** `consumer_options.go`, `general_options.go`,
    `channel_options.go`, `recordoutcome_string.go` (generated),
    `recordoutcome_string_test.go`, `consumer_options_test.go`,
    `CONTEXT.md` (stringer-guard row `:219-221`).
*   **Change:**
    *   `ProcessorConcurrency int` on `ConsumerUsageOptions` (must ride the
        embed — a field on `ConsumerOptions` outside it never reaches the
        channel).
    *   `WithProcessorConcurrency(n int) *ConsumerOptions` method; zero/negative = off;
        WARNING doc comment (inert at default prefetch 1; `PrefetchTimeout`
        and flush events retire when set; `PrefetchCount ≤ 0` unsupported on
        the concurrent path).
    *   `CallbackProcessRecord func(props *DeliveriesProperties, message DeliveryData) RecordOutcome`
        beside the other callback types.
    *   `WithChannelOptionProcessRecord(cb CallbackProcessRecord)` mirroring
        `WithChannelOptionProcessor`.
    *   `RecordOutcome` with `RecordAck`, `RecordNackRequeue` (zero value),
        `RecordNackDrop`; stringer via `stringer` plus the guard test
        following `ConfirmationOutcome`/`EventType` precedent.
*   **Acceptance:** `go build ./...`, `go vet ./...`, gofmt clean; stringer
    guard test passes; doc comments state the combination rules (batch hook
    ignored when knob > 0; record callback unused at 0).
*   **Notes:** no broker needed.
*   **Status:** closed 2026-11-24 — field, both builders, `CallbackProcessRecord`,
    `RecordOutcome` with generated stringer and guard test, builder tests.
    `go build`, `go vet`, gofmt clean; full and `-race` suites green.

### T11 — F2: concurrent dispatch machinery

*   **Type:** Feature (core). **Size:** L. **Depends on:** T6, T8, T10. `[B]`
*   **Files:** `channel_notifiers.go` (branch at the spawn site `:56`),
    `channel_consume.go` (new concurrent loop + settle helper),
    `consumer.go` (default record processor; batch default Err correction),
    `consumer_test.go`, `container_setup_test.go` (helpers as needed),
    `CONTEXT.md` (`:214-218` consumer half; `:227-231` goroutine side
    effects).
*   **Change, in order:**
    1.  **Latch-semantics pin test first** — pin `connected.Details()`
        transition-counter behavior, including whether the establishment
        `Unlock` counts. No production code may lean on the counter before
        this test exists.
    2.  Concurrent loop: workers pull the deliveries feed directly; select on
        exactly the feed and `ctx.Done()`; per-generation pool; WaitGroup
        join before gobble returns. No dispatcher goroutine, no work channel,
        no timer arm.
    3.  Settle helper: re-derive ack-worthiness per record; non-blocking
        connected-latch re-check immediately before the wire call (skip =
        leave unacked for redelivery); issue exactly one of
        `Ack(tag,false)` / `Nack(tag,false,true)` / `Nack(tag,false,false)`.
    4.  Default record processor: `RecordAck` per record; `EventMessageReceived`
        with `Err` only on failure. Apply the same Err-only correction to the
        batch default processor.
*   **Acceptance:** build/vet; new tests green under `-race` `[B]`; full
    `-race` suite `[B]`; no batch fill, no timer, no flush events on the
    concurrent path; gobble join observable (`AwaitManager`).
*   **Notes:** `Qos` call untouched (trace pin). Sequential path
    byte-identical at zero value.
*   **Status:** closed 2026-11-24 — latch pin test (`channel_latch_test.go`),
    per-generation worker pool with join, settle helper, default record/batch
    processors, and a tracer-bullet dispatch test. Full and `-race` suites
    green; the tracer bullet asserts no flush events on the concurrent path.

### T12 — F3: behavioural test suite

*   **Type:** Test. **Size:** L. **Depends on:** T11. `[B]`
*   **Files:** `consumer_test.go`, `container_setup_test.go` (max-concurrency
    tracker; non-leaking drain helper — never `wgDoneOrTimeout`), new
    `channel_consume_test.go` if the file grows beyond reason.
*   **Change / scenarios:**
    *   Concurrent completion across a forced recovery transition
        (`killConnections` mid-processing): workers exit via join, mustAck
        behavior holds, records redeliver (registry pattern).
    *   Shutdown with workers in flight (`Close` and ctx-cancel variants):
        goleak plus `AwaitManager`; per-test worker-exit assertions.
    *   Bound claim: max-concurrency tracker asserts ≤
        `min(ProcessorConcurrency, PrefetchCount)` under manual ack and ≤ N
        under auto-ack.
    *   Retirement assertions: no flush events and no timer on the concurrent
        path; the sequential flush-event scenario keeps its test.
    *   Poison scenario: a requeueing record loops finitely once the callback
        stops requeueing; goroutine count does not grow.
    *   Generation overlap: the 2N transient is observed and tolerated.
*   **Acceptance:** all scenarios green under `-race`; no leak verdict
    unattributed.
*   **Notes:** adopt the dead `MsgHandlerSlow` helper (`consumer_test.go:120`)
    as the slow-record variant instead of writing a new one. Completion
    gating is registry-based, not event-based.
*   **Status:** closed 2026-11-25 — behavioural suite in
    `channel_consume_test.go` (recovery completion, Close/channel-ctx shutdown,
    manual prefetch/worker and auto-ack bounds, no-flush plus a sequential
    partial-flush control, finite poison requeue, 2N generation overlap);
    concurrency/drain helpers in `container_setup_test.go`; `MsgHandlerSlow`
    adopted as the slow-record variant. Full `-race` suite green; shutdown
    subtests carry per-test goleak attribution. Also buffered the channel
    `NotifyClose` notifier to remove a reproducible context-cancel shutdown
    deadlock (unbuffered shutdown send racing `Close` behind the channel mutex).

### T13 — F4: documentation and ADR sync

*   **Type:** Docs. **Size:** M. **Depends on:** T11 (can start on final API).
*   **Files:** `CONTEXT.md` (options, delivery-batching interface, defaults,
    ack-gate, batching split, doc warnings, default callbacks, stringer
    guards, goroutine side effects), `GLOSSARY.md` (Processor split,
    Processor concurrency entry, Delivery batch scoping, acknowledgment entry
    defining `multiple`), `docs/adr/0008-per-channel-dispatch-model-sequential-by-default-and-concurrent-by-option.md`
    (new), `AGENTS.md` (consumer options list, QoS & prefetch section),
    `README.md` (sibling goal; non-goal carve-out), `consumer_options.go`
    (`QosGlobal` comment fix), `examples/consumer_batch/CONTEXT.md` (guard
    mislabel), `consumer_test.go` ("chunk size" comment drift).
*   **Acceptance:** every row above updated; ADR-0008 carries the
    `multiple=true` definition, bounds, lifecycle, settle rules (narrowed,
    not closed), retirement set, and the poison paragraph.
*   **Notes:** no broker needed.
*   **Status:** closed 2026-11-25 — CONTEXT/GLOSSARY/AGENTS/README synced
    (dispatch models, record surface, bounds, retirement, per-channel
    `QosGlobal` caveat); ADR-0008 added with the `multiple=true` definition,
    settle rules (narrowed, not closed), lifecycle, bounds, retirement set, and
    poison guidance; `QosGlobal` field/builder comments, the batch-example
    guard label, and the test comment drift corrected. Design docs marked
    implemented. Docs-only; CI suite green.

### T14 — F5: example demonstrating the record hook

*   **Type:** Example. **Size:** S–M. **Depends on:** T13.
*   **Decision (Q3, 2026-11-23):** a new tenth module, `examples/per_record/`;
    the batch example stays unchanged. This bumps the "nine" counts in
    `CONTEXT.md:34`, `:252`, `examples/CONTEXT.md:10`.
*   **Change:** one runnable demonstration: concurrent consumer on a queue,
    record callback with an explicit ack / nack-requeue / nack-drop branch,
    and a slow handler to show overlap.
*   **Acceptance:** example builds and runs against the container; count
    references consistent; no new event kinds.
*   **Status:** closed 2026-11-25 — new tenth module `examples/per_record/`
    (concurrent consumer, ack / requeue-then-drop / poison-drop branches, 200 ms
    handler, terminal-outcome and peak-overlap summary); example index and the
    three "nine" count references updated to ten. Module builds/vets clean and
    ran against LavinMQ (24/24 settled, peak overlap 8). No new event kinds.

### T15 — Release: minor-release packaging

*   **Type:** Release. **Size:** S. **Depends on:** T12, T13, T14.
*   **Files:** release notes / `CHANGELOG` if present; the two-column release
    map (surfaces moved by the remediation batched release vs this minor).
*   **Change:** ship as the fast-follow minor after the remediation batched
    release (DR4): symbolic version bump, release notes covering the new
    option, the callback surface, the retirement set, the drain-signal
    difference, the `QosGlobal` caveat, and the poison-message guidance.
*   **Acceptance:** release notes list every public surface added and every
    behavior difference on the concurrent path; the remediation release stays
    corrections-only.
*   **Status:** closed 2026-11-25 — symbolic `v1.2.0` bump in `README.md`;
    `RELEASE_NOTES.md` carries the two-column map, the corrections-only
    remediation section, every added public surface, and every concurrent-path
    behavior difference (retirement set, drain-signal loss, bounds, settlement,
    lifecycle, `QosGlobal`, poison). External tag/publish remains the only
    unchecked release-checklist item.

---

## Resolved items carried into tickets

| Item | Ticket | Status |
| --- | --- | --- |
| Gate-block placement in the design doc | resolved: summarized precondition table in `design_draft.md` | decided 2026-11-23 |
| `MsgHandlerSlow` adopt or delete | T12 | decided: adopt (scenario needs a slow handler) |
| Example: amend vs new module | T14 | decided 2026-11-23: new tenth module `examples/per_record/` (design_draft Q3) |
