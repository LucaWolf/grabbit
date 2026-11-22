# Manual acknowledgment is the consume default

Consumers start with automatic acknowledgment **off**: processors receive delivery
batches with the acknowledgment gate set, and unacknowledged deliveries are redelivered
if anything dies. Auto-ack exists only as an explicit opt-in, because it silently
trades redelivery safety for throughput.

## Consequences

- Across a recovery transition the gate is forced off for in-flight batches: they are
  processed but left unacknowledged, so the broker redelivers them on the recovered
  channel.
