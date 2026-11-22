# Advisory events are lossy by design

Lifecycle and operational events are delivered to the user's notifier channel with a
non-blocking send: when the buffer is full, the event is silently dropped. We chose the
recovery loop's throughput (it must never stall on a slow observer) over delivery
guarantees for notifications.

## Consequences

- Events are advisory: no correctness-critical logic may depend on receiving one.
- Applications either drain notifier channels continuously or size their buffers for
  worst-case churn, and accept gaps either way.
