# Goldfish Review Report

**Target:** `/home/luci/projects/go/grabbit/docs/designs/concurrent-consumer-processing.md` (`lines 1–842`)

**Stage:** EGM Review Stage 5 — Initial Zoom Out (architecture altitude, necessity of components/contracts, concealed complexity).

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-zoom-out` | completed |

Instance count: one (as requested). The review standard
(`.../resources/document_standards/zoom-out-standard.md`) could not be read: the
tool returned `permission.rejected: external_directory`. The reviewer and the
synthesizer therefore applied the zoom-out lens from the persona definition and
verified every code claim directly against the repository.

**Summary:** 6 Blockers, 11 Majors, 13 Nits.

---

## Prioritized Findings (Blockers -> Major -> Nits)

### Supervisor loop is a competing receiver on the deliveries feed - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 226–259` (*Concurrent dispatch machinery*, esp. `:257–259`), `lines 614–625` (target file 6), diagram `lines 249–254`
* **Discrepancy Description**: The document says *"The loop spawns N workers that pull the deliveries feed directly, selects on exactly the feed and `ctx.Done()`, and joins the pool with a WaitGroup before returning."* Grammatically and in the diagram, the supervisor (gobble) is the subject of **spawns, selects, and joins**. A Go channel has one queue: if the supervisor also receives from the deliveries feed, it competes with its own workers. Today `gobble` is the sole receiver (`channel_consume.go:68`, `case msg, ok := <-consumer:`), and the `ok == false` branch (`:69–75`) is the only teardown detector. A delivery won by the supervisor is removed from the feed, never handed to a worker, and never settled.
* **Rationale & Recommendation**: On a live channel the stolen delivery stays unacked and permanently consumes a `PrefetchCount` slot; with `PrefetchCount == N` the consumer can wedge at window exhaustion. Even a few steals silently reduce concurrency and can never be observed by a "max concurrency" or registry-length test until the queue stalls. State the ownership contract explicitly: **the supervisor performs no feed receive; it spawns N workers, waits on the WaitGroup, and exits. The two-case select (`feed`, `ctx.Done()`) lives inside each worker and nowhere else.** Delete "selects on exactly the feed" from the loop description. The same contradiction is carried in `concepts_brief.md:59` and `:183` and should be corrected there too.

### "Worker-issued bulk acks are structurally impossible" is false - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 76–82` (G2), `lines 218–222` (*Public API surface*), `lines 380–390` (Alternative E), `line 29` (Overview)
* **Discrepancy Description**: The document's central claim is that excluding `*Channel` from `CallbackProcessRecord` makes acknowledgment *"enforceable rather than conventional"* and that *"there is no handle to do it with"*. That is not true. `Consumer.Channel()` returns `*Channel` (`consumer.go:37–40`), `Publisher.Channel()` does the same (`publisher.go:71–73`), `Channel.Ack` is exported (`channel_safewrappers.go:67–75`), and `DeliveryData` hands the worker the tag (`general_options.go:116`, `DeliveryTag uint64`). A callback closure can capture the consumer or publisher after construction and call `Ack(tag, true)` from a worker:
  ```go
  var c *Consumer
  cb := func(p *DeliveriesProperties, m DeliveryData) RecordOutcome {
      if cond { c.Channel().Ack(m.DeliveryTag, true) }
      return RecordAck
  }
  c = NewConsumer(conn, opt, WithChannelOptionProcessRecord(cb))
  ```
  This compiles and runs. Excluding the parameter *discourages* the hazard; it does not make it structurally impossible.
* **Rationale & Recommendation**: The false guarantee is the stated reason DR1 exists and the stated reason Alternative B/E were rejected. As written, the design's differentiator against user-side goroutines ("library-owned ack authority") is not delivered. Either (a) weaken the claim to "the callback is not handed the channel; bulk acks are still reachable via `Consumer.Channel()`/`Publisher.Channel()` and remain the user's responsibility", or (b) actually enforce it (e.g., have the settle path the only writer and document that direct channel acks are unsupported, or stop exposing `Channel()` to that path — which would be a larger API change). Do not build the architecture on an unattainable property.

### "Byte-identical at zero value" is contradicted by the plan's own stages - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 72–75` (G1), `lines 88–90` (G4), `lines 103–107` (Non-Goals), `lines 154–161` (CUJ 2), `lines 283–291` (default processors), `lines 476–479` (rollout), `line 535` (S5), `lines 627–635` (target 7)
* **Discrepancy Description**: The document repeatedly promises that `ProcessorConcurrency == 0` is *"byte-identical"* to today, that the sequential path is untouched, and that "rollback means unsetting the option" with no behavior change. The same document then schedules changes to the zero-value sequential path:
  * target 7 and `line 290`: correct `defaultPayloadProcessor` to set `Err` only on failure. Today every default batch consumer raises `EventMessageReceived` with `Err: "default processor discards messages"` on every batch (`consumer.go:16–27`). Changing that is an observable zero-value behavior change.
  * row S5 and target 22: change `defaultNotifyPublish` to set `Err` only on failure (`publisher.go:44–54`) — zero-value publisher behavior change.
  * row S4 and target 23: flip durable topology re-declaration during recovery (`channel.go:336`) — zero-value recovery behavior change (and a breaking release item per the remediation audit).
* **Rationale & Recommendation**: The compatibility claim is the document's main safety argument for an additive minor release. It is only true for the dispatch code path (`gobble` untouched), not for the library's observable behavior at `ProcessorConcurrency == 0`. Scope the claim precisely ("the sequential dispatch loop is unchanged; the remediation stages S4/S5 and the default-processor correction change observable behavior independently of this feature") or move the unrelated corrections out of the feature. As written, a reviewer cannot tell what "byte-identical" is supposed to cover, and the rollback story is false once S4/S5 have shipped.

### Settle safety depends on an unpinned third-party counter, with no abort criterion - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 267–281` (*The settle helper*, *Stale-tag status*), `lines 469–475` (Reliability), `lines 513–515` (Future Improvements), `lines 797–807` (target 25), `lines 127–135` (Open Questions)
* **Discrepancy Description**: The settle helper is specified as: (1) re-derive ack-worthiness; (2) *"re-check the connected latch non-blockingly immediately before the wire call — a transition skips the call"*; (3) issue `Ack`/`Nack`. The mechanism that detects "a transition" is `connected.Details()`, whose semantics are dependency-internal (`go-notifying-latch v1.1.0`, `go.mod:6`). The document concedes it is *"pinned by nothing in this tree"* and stages a pin test as F2's first item, but never states what happens if the pin contradicts the assumption. The baseline is also taken at gobble spawn (`channel_consume.go:58`), and the spawn site (`channel_notifiers.go:56`) runs **before** `ch.connected.Unlock()` at `channel.go:152`; if the establishment `Unlock` counts as a transition, every generation's baseline is stale and the concurrent path skips every ack forever (infinite redelivery), with no test or fallback specified.
* **Rationale & Recommendation**: The design takes ownership of the ack write — the premise of the feature — and then delegates generation identity to an undocumented counter, adds a pin test, adds an ADR paragraph admitting a message-loss window, and defers the real fix to "Future Improvements". A strictly stronger and simpler mechanism is already in the tree and uses the same mutex the ack path already takes: capture the current `*amqp.Channel` at pool spawn, and in the settle helper `baseChan.mu.Lock()`, compare it to `baseChan.super`, and issue the wire call while holding the lock. Recovery replaces `super` via `set()` (`channel_safebase.go:61–66`, called from `channel.go:268`) and `Close` nils it under the same mutex (`channel_safewrappers.go:26–41`), so pointer identity is generation-exact and the TOCTOU disappears. This deletes target 25, the `Details()` dependency, the "narrowed, not closed" data-loss paragraph, and the follow-up wrapper — and makes "Open Questions: None open" honest. If the latch path is retained, the fallback decision must be written down before F2, not after.

### Auto-ack settle semantics are contradictory and one reading kills the channel - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 76–82` (G2), `lines 267–273` (settle steps), `lines 283–298` (bounds/defaults)
* **Discrepancy Description**: Step (1) says the helper *"re-derives ack-worthiness per record from `!ConsumerAutoAck`"*, but step (3) says it *"issues exactly one of `Ack(tag, false)` / `Nack(tag, false, true)` / `Nack(tag, false, false)`"* unconditionally. Under `ConsumerAutoAck(true)` the broker has already settled the delivery at send time; issuing `Ack`/`Nack` on that tag is an unknown-tag channel exception (the broker closes the channel). The batch path guards this explicitly with `if mustAck` (`consumer.go:24–26`; `mustAck := !ConsumerAutoAck` at `channel_consume.go:56`). The design never states the guard. Conversely, if the helper skips the wire call under auto-ack, every `RecordNackRequeue`/`RecordNackDrop` is silently inert — the processor's outcome is advisory but nothing says so.
* **Rationale & Recommendation**: Two failure modes, both unstated: (a) a literal implementation of step (3) closes the consumer channel on every auto-ack consumer; (b) the "safe" implementation silently discards failure outcomes, which is exactly the convention-vs-construction gap the feature claims to remove. Pick one contract and state it in the helper spec, the callback/outcome doc comments, the glossary, and ADR-0008. Preferred: under auto-ack, do not issue wire calls, and either reject `ProcessorConcurrency > 0 && ConsumerAutoAck(true)` at the builder or document that outcomes are advisory (no redelivery). Also state what happens for an out-of-range `RecordOutcome` value (no default branch is specified).

### The primary CUJ does not compile and the option placement is unresolved - Severity: 🔴 Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 140–152` (CUJ 1), `lines 210–216` (Public API table), `lines 554–568` (target 1), `lines 583–592` (target 3)
* **Discrepancy Description**: CUJ 1 writes:
  ```go
  NewConsumer(conn, DefaultConsumerOptions().WithQueue("jobs").WithPrefetchCount(8), WithProcessorConcurrency(8))
  ```
  This fails twice against the real API:
  1. `NewConsumer` takes `opt ConsumerOptions` **by value** (`consumer.go:52`), while the chain returns `*ConsumerOptions` (all consumer builders are pointer-receiver methods, `consumer_options.go:57–139`).
  2. `DefaultConsumerOptions()` returns a non-addressable value (`consumer_options.go:42`), so calling the pointer method `WithQueue` on it does not compile. Existing examples assign first (`optConsumer := DefaultConsumerOptions(); optConsumer.WithQueue(...)`).
  3. `WithProcessorConcurrency(8)` is passed where `NewConsumer` expects `...func(*ChannelOptions)`, but the document's own table (`line 213`) places it in `consumer_options.go` beside the `*ConsumerOptions` methods, i.e. it should be called as a method on the options value. If it were instead a channel option func, `WithChannelOptionUsageParams(useParams)` is appended **last** (`consumer.go:56`, `channel_options.go:123–128`) and would overwrite `implParams` wholesale, erasing the value it set. The two placements are mutually exclusive.
  4. The CUJ says "and a record callback…" but never passes `WithChannelOptionProcessRecord`, so the journey does not actually exercise the new hook.

* **Rationale & Recommendation**: The flagship user journey must be code that compiles. Resolve the placement explicitly (the only workable design is: field on `ConsumerUsageOptions`, builder method on `*ConsumerOptions`, applied before the wholesale copy), then rewrite CUJ 1 as:
  ```go
  opt := DefaultConsumerOptions()
  opt.WithQueue("jobs").WithPrefetchCount(8).WithProcessorConcurrency(8)
  c := NewConsumer(conn, *opt, WithChannelOptionProcessRecord(cb))
  ```
  Add a compile-checked example or a unit test that constructs the options in the documented shape; this class of defect is otherwise invisible until a user hits it.

---

### "2N" is an observation, not a bound; K generations give K·N - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 86–87` (G3), `lines 260–266` (*Pool per generation*), `line 833` (verification: "2N generation overlap")
* **Discrepancy Description**: G3 and the NFR state "in-flight concurrency is bounded by N". The design then says old-generation workers exit asynchronously and *"two generations transiently run up to 2N workers"*. Nothing waits for the old gobble before `refreshNotifiers` spawns the next (`channel_notifiers.go:56`, called from `channel.go:149` on every establishment), and the record callback has no context (`general_options.go:145`), so a worker parked in user code can outlive any number of recoveries. A flapping broker plus a slow processor yields K overlapping generations and up to K·N live workers. The verification plan's "2N" test will pass on one clean transition while the claim stays false.
* **Rationale & Recommendation**: State the bound as **per generation** and explicitly note that K overlapping generations yield up to K·N live worker goroutines; either add a test that forces two consecutive transitions with a blocked callback or stop advertising 2N. The headline contract users will read is process-wide; the document's contract is generation-local.

### The lifecycle guarantee is unbounded by user code, and it is the reason Alternative B was rejected - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 91–94` (G5), `lines 185–193` (CUJ 5), `lines 115–117` (Non-Goals), `lines 346–357` (Alternative B)
* **Discrepancy Description**: The document promises "gobble joins the worker pool before returning" and "workers exit on feed close or context cancellation". A worker inside user code does neither; it exits only after the callback returns. The join is therefore unbounded in wall-clock time: `Close()` can return while gobble is still parked in a user callback forever, and goleak-based tests will hang/fail. The document's own brief acknowledges this (`concepts_brief.md:62`), but the design document drops the caveat and presents CUJ 5 as a working shutdown journey. Worse, the absence of a context accessor is the stated reason Alternative B ("user-side parallelism, zero new API") was rejected (`lines 350–353`). Adding `Channel.Context()` is a few lines and would make the zero-new-API alternative viable; the document must either add it or justify a library-owned pool on grounds other than context access.
* **Rationale & Recommendation**: Either pass the channel context to the record callback (this is the channel context, not the declined per-item cancellation context) or rewrite G5/CUJ 5 as "join completes after in-flight callbacks return; shutdown latency is caller-controlled" and remove the lifecycle-guarantee framing. Then revisit Alternative B with the accessor priced in. A guarantee that is only true after user code returns is not a lifecycle guarantee; it is a non-leak guarantee.

### Unrelated remediation work is bundled into the feature, invalidating its scope and rollback story - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 527–541` (*Gate block*), `lines 476–479` (Rollout/rollback), target files 16, 17, 20–24, plus S1/S5 text inside 7 and 22
* **Discrepancy Description**: Roughly one third of the action surface is not this feature: CI trigger repair, untagged build, local and CI race jobs, the copy-first append fix in both constructors, the publisher `Err` correction, durable topology re-declaration, the observation-B connection guard, ADR-0006, an example mislabel, and comment drift. The document's own Q1 rationale (`lines 317–326`) argues that restating the remediation plan "would duplicate a living document and drift", then duplicates it in a table plus per-file items. It also makes failure attribution impossible: a reader cannot tell whether a red test belongs to dispatch, topology, or CI. The rollback claim ("unset the option, no migration") is false once the remediation changes have shipped.
* **Rationale & Recommendation**: Keep F0–F5 in this document. Replace the gate table with a one-line precondition reference to the remediation audit's stages (S0-R, S1, S4, CI race), and delete targets 16, 17, 20–24 and the S1/S5 text from targets 7 and 22. The feature document should be reviewable as a feature.

### The concurrency knob is silently inert at defaults, and its semantics depend on two other options - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 283–298` (*Defaults, combination rules, and the bound*), `lines 432–441` (Alternative J), `lines 560–565` (target 1), `lines 163–171` (CUJ 3)
* **Discrepancy Description**: `DefaultConsumerOptions` sets `PrefetchCount: 1` (`consumer_options.go:46`). With manual acks (the default), `ProcessorConcurrency(8)` yields one worker's worth of work until the user also raises prefetch — the builder is a no-op in the default configuration. The document addresses this only with a doc-comment WARNING and explicitly declines validation (Alternative J: "no builder in the option family validates"; `ClampInt` exists but is not used for options). Effective parallelism additionally changes under `QosGlobal(true)`, where the window is shared with sibling channels; the bound section states `min(N, PrefetchCount)` unconditionally and relegates the caveat to target 15. CUJ 1 hides the trap by setting prefetch explicitly.
* **Rationale & Recommendation**: A knob whose default use does nothing is a usability defect, not a documentation problem. Options, in ascending cost: make `WithProcessorConcurrency(n)` raise `PrefetchCount` when it is still at the unmodified default (document the precedence), emit an existing `EventType` warning (no new constants) when concurrency > 1 and prefetch <= 1, or return a builder error. Also state the bound's preconditions (`ConsumerAutoAck == false`, `QosGlobal == false`) in the bound paragraph itself, not only in an ADR bullet. The "Qos trace pin" is a test artifact driving public API behavior; it should not be the reason a footgun ships.

### The proposed `QosGlobal` correction states an unverified, engine-dependent protocol claim - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 560–565` (target 1), `lines 163–171` (CUJ 3)
* **Discrepancy Description**: The current comment is misleading (`QosGlobal bool // all future channels`, `consumer_options.go:19`), and correcting it is right. But the proposed replacement — "global QoS shares the window across the connection's channels" — is a broker-behavior claim. The brief's settled wording explicitly hedges it: *"the interaction is untested in this repo … and its exact scope is engine-dependent (LavinMQ-primary …), RabbitMQ best-effort"* (`concepts_brief.md:179`). The repo tests LavinMQ with a single consumer (`consumer_test.go:424`, `:469–485`). G6 says "Verified, not asserted"; this edit asserts.
* **Rationale & Recommendation**: Keep the hedged formulation: engine-dependent, untested in this repo, keep `QosGlobal(false)` when mixing models. If a definite claim is wanted, add the measurement to F3. Do not replace one misleading comment with an unverified one.

### The CI equivalence claim is false, and the gate omits the package pattern it needs - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 823–828` (Verification plan), `line 531` (Pre-S0), `lines 768–772` (target 21)
* **Discrepancy Description**: The document says the CI command `go test -tags test_env -coverprofile coverage.out -v ./...` "matches the existing workflow". It does not. `.github/workflows/coverage.yaml:17` is `go test -tags test_env -coverprofile coverage.out -v` — **no `./...`**. The remediation audit calls this out (`concepts_brief_remediation_audit.md:24`: a manual run "would still skip `internal/traceutils` and every non-root package"). The Pre-S0 row adds only the trigger fix and an untagged build; target 21 ("add the `-race` broker-suite job") specifies no package pattern. The authoritative race gate would therefore run only the root package — the one place where a dispatch-layer race is least likely to be caught.
* **Rationale & Recommendation**: Put `./...` into Pre-S0's scope and say so in the gate table; specify `go test -race -tags test_env ./...` for the new job; delete "(matches the existing workflow)". This also corrects the same false claim in `AGENTS.md:9`.

### The settle path has no error or observability contract - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 108–112` (Non-Goals), `lines 267–276` (settle helper), `lines 483–487` (Observability)
* **Discrepancy Description**: `Ack`/`Nack` return `error` (`channel_safewrappers.go:67–86`). The settle helper is specified as issuing the call, but the document never says what it does with the error: raise an event, drop it, count it, close? With ack tracing explicitly excluded (`docs/adr/0005:8–9`), the drain event retired on this path, and notifications lossy by design (`events.go:62–68`), the concurrent path has effectively zero diagnostics: an operator cannot tell whether a record was acked, nacked, skipped on a transition, or failed at the wire. The document calls `EventMessageReceived` "advisory, lossy" and moves completion gating to user registries, which is a test strategy, not a product observability story.
* **Rationale & Recommendation**: Specify the error contract in the helper: at minimum, define whether an `Ack`/`Nack` error is ignored (and why that is safe), or raise an existing event kind with `Err` set. Also specify the counter/metric surface (or state explicitly that there is none and that at-least-once is the only guarantee). The new central code path should not be the least observable part of the library.

### No performance acceptance criterion, despite latency/throughput being the entire rationale - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 461–468` (Performance and latency NFR), `lines 817–838` (Verification plan)
* **Discrepancy Description**: The feature exists to remove a latency barrier and increase parallelism. Every verification item is correctness-based (race, bound, shutdown, recovery, retirement, poison, overlap); there is no benchmark, no first-delivery latency measurement, and no throughput target. The document concedes that acks serialize on `baseChan.mu` and that *"N workers create N waiters where the serial path has one"*, and every safe wrapper performs the AMQP write while holding that mutex (`channel_safewrappers.go:67–75`). Under socket backpressure this makes the pool's settle throughput roughly one write at a time, which can dominate the processing gain. The document calls this "a throughput/latency trade" without ever measuring the trade.
* **Rationale & Recommendation**: Add acceptance criteria that match the claim: first-record latency at sparse arrival (no fill delay), achieved processing concurrency, and end-to-end throughput versus the sequential path at the same prefetch, with a stated workload. If those cannot be met because of the ack mutex, the design should say so up front and either narrow the claim (latency only, not throughput) or address the serialization (e.g., per-generation ack batching is out of scope, but the limitation must be quantified). A performance feature with no performance gate is unverifiable by construction.

### The gate table references undefined stages, and Q1's "F0–F5 detailed in full" is false - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 317–326` (Resolved decision Q1), `lines 527–550` (gate block and stage order), `lines 540–541` (precondition sentence)
* **Discrepancy Description**: The table uses `Pre-S0`, `S0-R`, `S0`, `S1`, `S4`, `S5`, and "observation-B" with no definitions in this document; they are defined only in `concepts_brief_remediation_audit.md`, which this document links with a broken relative path (see below). Q1 claims the plan presents the gate block as a summary table "with the feature stages F0–F5 detailed in full", but F0–F5 appear only as five one-line bullets (`lines 545–550`): no per-stage changes, exit criteria, dependency mapping, or test gate. The actual detail is organized per file, not per stage. The precondition sentence (`lines 540–541`) also says the feature starts after `S0-R, S1, S4, and the CI -race job`, omitting S5 and observation-B that the table lists, and elevating S4 from the brief's settled scope ("a hard gate before the feature's recovery-test work", `concepts_brief.md:19`, `:158`) to a gate for all feature work.
* **Rationale & Recommendation**: Either inline the S-stage definitions (one line each) or link them correctly; either detail F0–F5 with exit criteria or drop the "detailed in full" claim; and align the precondition sentence with the table and the brief's S4 scope. An implementer should not have to reconstruct the staging from a second document.

### Additional Resources links do not resolve, and ADR-0007 is silently skipped - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `line 33` (Overview link), `lines 496–506` (*Additional Resources*), `lines 713–731` (ADR targets)
* **Discrepancy Description**: The document lives in `docs/designs/`, but every link is written as if it lived at the repo root:
  * `./concepts_brief.md` and `./concepts_brief_remediation_audit.md` resolve to `docs/designs/...`; both files actually exist at the repo root.
  * `./docs/adr/0003-...`, `./docs/adr/0004-*.md`, `./docs/adr/0005-*.md` resolve to `docs/designs/docs/adr/...`; the real files are in `docs/adr/`.
  * `./CONTEXT.md`, `./GLOSSARY.md`, `./AGENTS.md` resolve to `docs/designs/...`; they are at the repo root.
  The document creates ADR-0006 and ADR-0008 while skipping 0007; 0007 is reserved by the remediation audit for the broker-engine decision (`concepts_brief_remediation_audit.md:137`) and is never mentioned here, so the numbering looks arbitrary.
* **Rationale & Recommendation**: Use paths relative to the document (`../../concepts_brief.md`, `../adr/0003-...`, `../../CONTEXT.md`) or repo-relative paths. Add one sentence explaining that 0007 is reserved for the remediation's broker-engine decision. These are mechanical fixes but they determine whether any reader can follow the design's evidence chain.

### The authoritative design is duplicated, and the ticket plan points at the other copy - Severity: 🟡 Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** Document-level; repo artifacts `design_draft.md`, `tickets.md`
* **Discrepancy Description**: `design_draft.md` at the repo root is the same 842-line document (identical opening and identical content at, e.g., `design_draft.md:800` vs this document `:800`). `tickets.md:1–4` declares its source as *"`concepts_brief.md` … and `design_draft.md` (Technical Plan approved)"* — not this file. There is no stated precedence between the two copies, and this document does not list `design_draft.md` or `tickets.md` in Additional Resources. Any review change applied to `docs/designs/concurrent-consumer-processing.md` leaves the ticket plan citing a divergent copy.
* **Rationale & Recommendation**: Keep one authoritative design file. Either move/rename `design_draft.md` to this path (and update `tickets.md`), or make this file the canonical copy and delete/replace the root draft, with a one-line pointer in both. Duplicate design artifacts are exactly the drift the document's Q1 rationale says it wants to avoid.

---

### "Fail-safe" zero value is a requeue storm - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `line 216` (*Public API surface*), `lines 473–475` (poison paragraph)
* **Discrepancy Description**: `RecordNackRequeue` as the zero value is labelled fail-safe, while the same document says a requeued record *"can loop at wire speed"*. The most likely user error (returning without deciding) becomes a hot redelivery loop.
* **Rationale & Recommendation**: Call it "no-loss, not no-storm", or make zero = `RecordAck` with requeue explicit. If the zero remains requeue, the poison paragraph must be repeated at the constant's doc comment, not only in the ADR.

### Registered hooks are silently ignored - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 290–292` (combination rules)
* **Discrepancy Description**: With `ProcessorConcurrency > 0`, a registered batch hook is ignored; at 0, a registered record callback is unused. Documentation-only is justified by "no builder validates" (`lines 432–441`), which is precedent, not a reason.
* **Rationale & Recommendation**: Emit an existing event kind with `Err` set, or return a builder error, when both hooks are registered. A user debugging an inert callback will not read both builders' comments.

### A tenth example module is scope for a demo - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 749–759` (target 19), `lines 741–747` (target 18), Q3 `lines 135–136`
* **Discrepancy Description**: The brief left F5 open ("amend `examples/consumer_batch` or add a module", `concepts_brief.md:211`); this document hardens the expensive branch: a new `go.mod`/`go.sum`/`main.go`/`CONTEXT.md`, an index row, and "nine → ten" count edits in at least three places. Every example module also carries pin drift (the existing nine are `go 1.21` / `amqp091-go v1.9.0`, e.g. `examples/consumer_batch/go.mod:3–7`), so the tenth adds another module to the S7 refresh burden.
* **Rationale & Recommendation**: Prefer amending `examples/consumer_batch` (or adding a second consumer to an existing `main.go`) unless a standalone module buys something concrete. If the module stays, state why and add its pin refresh to the plan.

### "Zero constructor code change" is narrow-true and contradicts the plan - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `line 201` (*Dispatch-model selection*), targets 7 and 22
* **Discrepancy Description**: The field does ride the wholesale copy (`consumer.go:53–55`), so the knob itself needs no constructor edit. But the same plan modifies both constructors for S1 (`consumer.go:56`, `publisher.go:82`, the append-aliasing bug recorded in `CONTEXT.md:213`). The sentence reads as a minimality claim for the whole change.
* **Rationale & Recommendation**: Say "the knob needs no constructor code; the separate S1 hygiene fix does". Precision here matters because "byte-identical" is already doing too much work.

### `props.Headers` "shared map" caveat is a batch-era vestige - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 299–303` (retirement set), `lines 570–581` (target 2)
* **Discrepancy Description**: The document says each record gets fresh properties via `DeliveryPropsFrom`, then warns that `props.Headers` "stays a shared-map reference, read-only". `DeliveryPropsFrom` copies `d.Headers` (`general_options.go:97–108`); with one delivery per record there is no cross-record sharing. The warning is inherited from the batch model, where props were shared across a batch.
* **Rationale & Recommendation**: Drop the caveat or restate it correctly (the map belongs to the delivery and must not be retained after the callback returns). As written it suggests a concurrency hazard that does not exist and obscures the real lifetime rule.

### The callback signature passes per-record data twice - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `line 214` / `lines 570–578` (callback type)
* **Discrepancy Description**: `func(props *DeliveriesProperties, message DeliveryData)` splits one delivery across two structures, allocates a props value per record, and gives the processor two objects to reconcile. The brief rejects a combined struct with "it would ripple into the batch path" (`concepts_brief.md:150`), but a *new* per-record struct cannot ripple into the batch path.
* **Rationale & Recommendation**: Consider a single `Record` struct embedding/reusing the fields a processor needs (body, tag, redelivered, headers, routing key, etc.). If the split is kept for parity with the batch hook, state that as the reason.

### Out-of-range `RecordOutcome` values are unspecified - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 216`, `lines 267–273`
* **Discrepancy Description**: The settle helper maps three constants to wire calls; nothing says what a `RecordOutcome(7)` does. With no builder validation and a Go enum that admits any int, this is reachable.
* **Rationale & Recommendation**: Specify the default branch: fail-safe requeue (consistent with the zero value), or ack, or skip with an event. One sentence prevents an implementer from inventing a fourth behavior.

### Settled decisions from the brief are dropped from this document - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 127–135` (*Open Questions*), `lines 173–183` (CUJ 4)
* **Discrepancy Description**: The brief settles two points this document omits: raw `NewChannel` + `WithChannelOptionUsageParams` users receive the concurrent model unconditionally (`concepts_brief.md:178`), and the two-column release map gains rows for the retirement set, drain-signal loss, QosGlobal, and poison (`:171`). A reader implementing from this document alone cannot know either.
* **Rationale & Recommendation**: Add both to the Technical Plan or to ADR-0008's scope. "Fully settled at the decision level in the brief" is only useful if the document carries the load-bearing decisions forward.

### "No existing exported identifier changes shape" is loose - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 488–490` (Compatibility)
* **Discrepancy Description**: Adding `ProcessorConcurrency` to the exported `ConsumerUsageOptions` struct (`consumer_options.go:13–26`) breaks unkeyed composite literals, which Go's compatibility guidance notes as a possible break. The feature may still qualify as a minor release, but the sentence asserts more than is true.
* **Rationale & Recommendation**: Say "no exported method or type changes signature; one exported struct gains a field (unkeyed literals are affected)". Or note that unkeyed literals are not supported by the project.

### `PrefetchCount <= 0` is declared unsupported but its runtime behavior is not - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 560–565` (target 1)
* **Discrepancy Description**: The builder warning says `PrefetchCount <= 0` is "unsupported on the concurrent path", but the plan rejects validation. What happens at 0 (broker treats it as unlimited) or negative (AMQP error) is therefore left to the implementer and the broker.
* **Rationale & Recommendation**: State the actual behavior or clamp it. "Unsupported" without a defined failure mode is an invitation to divergent implementations.

### "The `multiple=true` definition the repo lacks" overstates the gap - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 99–101` (G7), target 15
* **Discrepancy Description**: `CONTEXT.md:216` already documents that the default processor "acks only the last delivery tag with `multiple=true`"; what the repo lacks is a *definition* in the glossary, which the document does plan to add.
* **Rationale & Recommendation**: Say "the glossary lacks a definition of `multiple=true`" rather than "the repo lacks" it.

### CUJ 5 has two consecutive "When" bullets - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 189–190`
* **Discrepancy Description**: The journey reads "When: the consume loop tears down. When: workers exit between records…". One of these is a condition, the other a behavior.
* **Rationale & Recommendation**: Merge into a single **When** and move the worker-exit sentence into **Then**.

### Example modules pin older Go and amqp versions - Severity: 🟢 Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `lines 749–759` (target 19)
* **Discrepancy Description**: A new module "matching the other nine" would copy `go 1.21` and `amqp091-go v1.9.0` (`examples/consumer_batch/go.mod:3–7`) while the root module is `go 1.25.7` and `amqp091-go v1.10.0` (`go.mod:3–8`). That is pre-existing drift, but a new module entrenches it.
* **Rationale & Recommendation**: Create the new module at the current pins (`go 1.25.7`, `amqp091-go v1.10.0`) or fold the pin refresh into the same change.

---

## What Holds Up

* **The alternatives are substantive, not decorative.** Each rejected shape (batch fan-out, user-side goroutines, bounded dispatcher, worker-per-delivery, channel handle, `mustAck` parameter, bool/struct outcomes, no stringer, trace parameters, builder validation) carries a concrete rejection reason. The zoom-out-recommended "three plain constants, no stringer" is recorded as Alternative H rather than erased.
* **The retirement set is explicit and per path.** The batch fill, timer arm, flush events, sticky `mustAck`, and per-batch props are named one by one, and `gobble` is left untouched, which makes the sequential-path claim diff-checkable (though not "byte-identical" in behavior, see Blocker 3).
* **Citation discipline is high.** Every source line cited by the document resolved at the stated location in the current tree (see table below); the repo-state claims (15 `EventType` constants, three public enums with stringers, nine example modules, `master` trigger, no `race` in `.github`, dead `MsgHandlerSlow`, no validating builder, no `CHANGELOG`) are true. The failures are architectural and referential, not fabricated evidence.

---

## Citation Verification (spot-check)

| Citation | Held? | Observed |
| --- | --- | --- |
| `channel_consume.go:85` batch fill | ✅ | `if len(messages) == ch.opt.implParams.PrefetchCount` |
| `channel_consume.go:100` flush timer | ✅ | `case <-time.After(ch.opt.implParams.PrefetchTimeout)` |
| `channel_consume.go:56` `mustAck` init | ✅ | `mustAck := !ch.opt.implParams.ConsumerAutoAck` |
| `channel_consume.go:62–75` teardown flush | ✅ | pending batch processed with `mustAck=false` |
| `channel_notifiers.go:56` spawn site | ✅ | `go ch.gobble(ch.notifiers.Consumer)` inside `refreshNotifiers` |
| `channel.go:152` establishment `Unlock` | ✅ | runs **after** `refreshNotifiers` returns (pool already spawned) |
| `channel.go:336` skip-durable | ✅ | `if !t.Declare || (recovering && t.Durable) { continue }` |
| `consumer.go:53–55` wholesale copy | ✅ | `useParams := ChanUsageParameters{ConsumerUsageOptions: opt.ConsumerUsageOptions}` |
| `consumer.go:56` append | ✅ | `append(optionFuncs, WithChannelOptionUsageParams(useParams))` |
| `consumer.go:16–27` default batch processor | ✅ | raises on every batch; `if mustAck` acks last tag `multiple=true` |
| `consumer_options.go:46` prefetch default 1 | ✅ | `PrefetchCount: 1` |
| `consumer_options.go:19` stale `QosGlobal` comment | ✅ | `// all future channels` |
| `general_options.go:145` no-ctx batch callback | ✅ | 4 parameters, no `context.Context` |
| `general_options.go:116` `DeliveryTag` exposed | ✅ | `DeliveryTag uint64` |
| `channel_safewrappers.go:67–86` Ack/Nack | ✅ | take `baseChan.mu`, nil-check `super`, return `error` |
| `channel_safewrappers.go:26–41` Close | ✅ | holds `mu`, nils `super`, cancels ctx |
| `channel.go:268` fresh `super` | ✅ | `ch.baseChan.set(super)` |
| `consumer_test.go:120–130` `MsgHandlerSlow` | ✅ | definition only; no call sites |
| `consumer_test.go:469–485` Qos trace pin | ✅ | asserts exact count/size/global |
| `container_setup_test.go:196–209` helper | ✅ | `:206` "this still leaks the coroutine if wg was > 1" |
| `coverage.yaml:4` / `:17` | ✅ | `branches: [ master ]` / no `./...` |
| 15 `EventType` constants | ✅ | `events.go:28–44` |
| nine example modules | ✅ | 9 `go.mod` files; `examples/CONTEXT.md:10` says "Nine" |
| three enums with stringers | ✅ | `ClientType`, `EventType`, `ConfirmationOutcome` (+ generated tests) |
| `docs/adr` numbering | ⚠️ | holds 0001–0005; 0006 and 0008 planned, 0007 reserved elsewhere and unmentioned |
| ADR-0004 process-and-leave-unacked | ✅ | `docs/adr/0004:10–12` |
| ADR-0005 ack/nack/reject untraced | ✅ | `docs/adr/0005:8–9` |
| `GLOSSARY.md` lacks ack/`multiple` definition | ✅ | "ack" appears only as an Avoid entry |
| `./concepts_brief*.md`, `./docs/adr/...`, `./CONTEXT.md` | ❌ | files exist at repo root / `docs/adr/`; doc-relative links resolve into `docs/designs/` |
| duplicate design | ⚠️ | `design_draft.md` mirrors this document; `tickets.md:1–4` cites it |
| latch `Details()` semantics | ⚠️ | dependency `go-notifying-latch v1.1.0`; not pinned in-tree; module cache not readable here |

---

*Design document not modified. Findings compiled from the `egm-zoom-out` reviewer report and independent repository verification.*
