# Build-tag-gated call tracing

The library instruments the AMQP operations it wraps — declarations, bindings, purges
and deletes, the publish variants, consume, QoS, flow, confirm, transactions — with
calls into an internal tracing harness that compiles to empty no-ops unless the
`test_env` build tag is set. Under that tag the calls emit parameter records onto
context channels, so tests assert exactly what the library passes to the broker
without a live broker. (`Close`, `Cancel`, `Ack`, `Nack`, and `Reject` are not
traced — the harness has no counterparts for them.)

## Consequences

- Production builds carry zero tracing overhead; there is no runtime flag to flip.
- The tag is part of the test contract: CI runs `go test -tags test_env`, and
  trace-sensitive tests fail without it.
