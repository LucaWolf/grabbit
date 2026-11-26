# Examples

Ten runnable demos of the grabbit library. Every demo is a `package main` in this
module, so there is nothing to install or replace:

```sh
go run ./examples/<name>
```

The demos are teaching material, not importable packages. They intentionally keep
their callbacks and logging close to the surface instead of hiding them behind a
shared helper.

## Broker

All demos connect to `amqp://guest:guest@localhost:5672/` with `guest`/`guest`.
Any RabbitMQ-compatible broker works. The repository also ships a local,
Podman-managed three-node LavinMQ cluster with etcd and HAProxy:

```sh
make lavinmq-up      # AMQP through HAProxy on localhost:5672
make lavinmq-down    # stop; broker data is kept
make lavinmq-reset   # stop and delete broker + etcd data
```

See [lavinmq-ha/README.md](./lavinmq-ha/README.md) for topology, management
ports, and troubleshooting. Port `5672` must be free.

## Quick start

```sh
make lavinmq-up
make run EXAMPLE=connection
# or: go run ./examples/connection
```

Most demos run until `Ctrl-C` (`SIGINT`/`SIGTERM`). Each long-running demo
prints a `done:` or `demo running:` line describing the completed happy path
before it waits for the signal. `contexts` and `per_record` terminate by
themselves.

## Catalog

| Example | What it demonstrates | Main API |
| --- | --- | --- |
| `connection` | Managed connection lifecycle, callbacks, clean close | `NewConnection`, `WithConnectionOption*`, `Close` |
| `channel` | Lower-level managed channel; manual queue declaration on channel-up | `NewChannel`, `WithChannelOption*`, `Channel.QueueDeclare` |
| `contexts` | Channel ownership through a context tree; sibling isolation and parent cancellation | `WithChannelOptionContext`, `AwaitStatus(false, ...)` |
| `publisher` | Fire-and-forget publishing with confirmation callbacks | `NewPublisher`, `Publisher.Publish` |
| `publisher_await_delivery` | Matching notify-publish confirmations to request sequence numbers | `GetNextPublishSeqNo`, `WithChannelOptionNotifyPublish` |
| `publisher_routing` | Exchanges, queues, bindings, and direct/topic/default routing | `TopologyOptions`, `Publisher.PublishWithOptions` |
| `pub_deferred` | Per-message deferred confirmations, awaited one-by-one and in order | `PublishDeferredConfirm`, `AwaitDeferredConfirmation` |
| `consumer` | Two competing consumers with the sequential batch processor | `NewConsumer`, `WithChannelOptionProcessor` |
| `consumer_batch` | Batch processing, bulk ACK/NAK, and an exactly-once ACK registry | `CallbackProcessMessages`, `Ack`/`Nack(multiple=true)` |
| `per_record` | Per-record worker pool with bounded concurrency, ack/requeue/drop outcomes, and structured `slog` integration | `WithProcessorConcurrency`, `WithChannelOptionProcessRecord`, `RecordOutcome` |

`lavinmq-ha/` is infrastructure, not a demo; it is not built by the Go tooling.

## Build and verify

```sh
go build ./examples/...   # compile every demo
go test ./examples/...    # no test files; compile check only
make examples             # write every demo binary to bin/
```

## Notes

- The default consumer is the sequential batch model. `consumer`, `consumer_batch`,
  and `publisher_routing` use it deliberately; `per_record` shows the opt-in
  concurrent record model.
- Bulk acknowledgment (`multiple=true`) is a sequential-path pattern. The
  library never issues it on the concurrent record path.
- `per_record` is the structured-logging reference: it uses `log/slog` with the
  JSON handler for both grabbit lifecycle events and per-record processing, plus a
  custom TRACE level for application work. The other demos use the standard `log`
  package to keep their API walkthroughs short. grabbit itself performs no logging;
  it raises events and callbacks.
- `per_record` uses `127.0.0.1` instead of `localhost` to avoid an IPv6 loopback
  mismatch; the bundled HAProxy broker serves both addresses.
- `connection` intentionally stays alive for 15 seconds after closing, to show that
  the reconnect loop has stopped. The other shutdown paths take about 3 seconds.
- `consumer_batch` runs its final ACK-registry check on shutdown. Let the 500
  messages settle before pressing `Ctrl-C`; an early signal can report missing ACKs.
