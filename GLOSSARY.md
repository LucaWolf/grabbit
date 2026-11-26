# grabbit

A managed, auto-reconnecting client library for RabbitMQ. Connections, channels,
publishers, and consumers here are not thin wrappers over the wire protocol: each one
supervises its own state and repairs itself after broker outages. This glossary fixes
the canonical vocabulary for saying so.

## Language

### Clients & lifecycle

**Managed client**:
One of the four client kinds — connection, channel, publisher, consumer — that supervises its own state and repairs itself after broker outages.
_Avoid_: wrapper, object, primary type

**Recovery**:
The umbrella for everything a managed client does to return to an established state after a failure: reconnecting, re-registering broker listeners, re-declaring its declared topology, re-arming state.
_Avoid_: reconnect (too narrow — it is only the dial), rebase

**Recovery loop**:
The background goroutine every managed client owns from construction until cancellation or close; it performs initial establishment and every later recovery.
_Avoid_: reconnect loop, manage loop

**Up / down**:
Narrative words for lifecycle transitions in events and callbacks (went up, went down).
_Avoid_: as probe words — ask "is it established?", not "is it up?"

**Established**:
The state of a managed client whose underlying AMQP resource is connected and usable — the property the status probes wait for.
_Avoid_: open, alive

### Events & notifications

**Event**:
A best-effort notice of a lifecycle or operational occurrence, delivered on a notifier channel. Advisory by nature: a full buffer silently drops it.
_Avoid_: message (reserved for broker payloads), alert

**Notifier channel**:
The user-supplied channel on which a managed client delivers its events.
_Avoid_: events channel, status channel, notification channel (unqualified)

**Broker listeners**:
The raw AMQP notification channels the library registers on the underlying connection or channel, and re-registers during recovery. Library-owned, never user-facing.
_Avoid_: notifiers, notifications

**Lifecycle callback**:
A user hook invoked at a lifecycle transition. The down and recovering kinds may veto further retries by returning false; the up kind cannot veto anything.
_Avoid_: notification, handler

### Consuming

**Batch processor**:
The user hook on the sequential path that receives delivery batches and decides their fate — acknowledge, negatively acknowledge, or reject.
_Avoid_: message handler, consumer callback

**Record processor**:
The user hook on the concurrent path that receives one delivery and returns a record outcome: acknowledge, nack-requeue, or negative without requeue (nack-drop).
_Avoid_: per-message handler, worker callback

**Processor concurrency**:
The per-channel worker-pool size that selects per-record dispatch: zero or negative keeps the sequential model, a positive value runs at most that many record processors at once.
_Avoid_: parallelism, thread count

**Delivery batch**:
The group of deliveries handed to a batch processor at once on the sequential path: filled to the prefetch setting, and flushed partially when deliveries stop arriving for long enough.
_Avoid_: chunk, message group

**Acknowledgment**:
The consumer-side settlement of a delivery — positive (ack), negative with requeue, or negative without requeue. `multiple=true` settles every outstanding delivery up to and including the given tag on that channel; the concurrent path always settles one tag at a time with `multiple=false`.
_Avoid_: confirm (reserved for publisher-side broker confirmation)

**Resolved queue name**:
The queue a channel actually consumes from: the name captured from its destination element when one exists, otherwise the configured consume queue.
_Avoid_: target queue, active queue, auto-generated queue

### Publishing

**Confirm mode**:
The channel state in which the broker acknowledges publishes; armed by a positive confirmation count, disabled entirely by zero.
_Avoid_: confirmations enabled

**Broker confirmation**:
The channel-level acknowledgment of a publish, observed through the publish-confirmed lifecycle hook.
_Avoid_: ack (reserved for consume-side acknowledgment)

**Deferred confirmation**:
The per-message handle returned at publish time and awaited explicitly for its outcome.
_Avoid_: publish receipt, confirm

### Topology

**Topology element**:
A declared exchange or queue — with its optional binding — that a channel establishes at start-up and maintains during recovery.
_Avoid_: topology item, declaration

**Destination element**:
A topology entry whose declared name, possibly server-assigned, becomes the channel's resolved queue name and takes precedence over the configured consume queue.
_Avoid_: target, bound queue
