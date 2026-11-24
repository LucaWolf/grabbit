# Managed clients own their recovery loop

Every managed client (connection, channel, publisher, consumer) starts a background
recovery loop at construction and supervises its own AMQP resource for the rest of its
life: initial establishment, reconnection, listener re-registration, and topology
re-declaration. There is no Start/Stop pair — a client's lifecycle ends only
through context cancellation or its idempotent `Close`, so code that constructs one
must own one of those two shutdown paths.

## Considered Options

- **Rejected — explicit Start/Stop lifecycle**: callers would gain visible control, but
  every usage would grow bookkeeping and a constructed-but-not-started client would
  become a new failure mode.

## Consequences

- The status probes (`AwaitStatus`, `AwaitManager`) are the supported way to observe
  state; polling `IsClosed()` is not, because the latched state wraps around the raw
  AMQP status change.
- `Cancel` (consumer deregistration) is deliberately NOT idempotent, unlike `Close`.
- A publisher or consumer always owns a dedicated channel; using separate connections
  for publishing and consuming workloads is recommended so each side recovers
  independently.
