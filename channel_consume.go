package grabbit

import (
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// consumer sets up the amqp Deliveries feed for the given channel
// (wraps the amqp.Channel.Consume method after setting the QoS).
func (ch *Channel) consumer() (<-chan amqp.Delivery, error) {
	if err := ch.Qos(ch.opt.implParams.PrefetchCount, ch.opt.implParams.PrefetchSize, ch.opt.implParams.QosGlobal); err != nil {
		Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventQos,
			Err:        SomeErrFromError(err, true),
		}.raise(ch.opt.notifier)
	}
	// overwrite the passed queue to consume with the server assigned value
	qName := ch.opt.implParams.ConsumerQueue
	if len(ch.queue) != 0 {
		qName = ch.queue // only when IsDestination
	}

	consumer, err := ch.ConsumeWithContext(ch.opt.ctx, qName,
		ch.opt.implParams.ConsumerName,
		ch.opt.implParams.ConsumerAutoAck,
		ch.opt.implParams.ConsumerExclusive,
		ch.opt.implParams.ConsumerNoLocal,
		ch.opt.implParams.ConsumerNoWait,
		ch.opt.implParams.ConsumerArgs)

	if err != nil {
		Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventConsume,
			Err:        SomeErrFromError(err, true),
		}.raise(ch.opt.notifier)
	}

	return consumer, err
}

// gobble runs the consumer function.
//
// It consumes messages from the given channel and processes them.
// When messages are received, they are stored in a slice called messages and processed when
// the number of messages reaches a certain count or the prefetch timeout is reached.
//
// Parameters:
//   - consumer: a channel of amqp.Delivery for receiving messages.
func (ch *Channel) gobble(consumer <-chan amqp.Delivery) {
	var props DeliveriesProperties
	mustAck := !ch.opt.implParams.ConsumerAutoAck
	messages := make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)
	_, transitions := ch.connected.Details()

	for {
		select {
		case <-ch.opt.ctx.Done(): // main chan and notifiers.Consumers should also be gone
			if len(messages) != 0 {
				// conn/chan are gone, cannot ACK/NAK anyways
				ch.opt.cbProcessMessages(&props, messages, false, ch)
			}
			return
		case msg, ok := <-consumer: // notifiers data
			if !ok {
				if len(messages) != 0 {
					// conn/chan are gone, cannot ACK/NAK anyways
					ch.opt.cbProcessMessages(&props, messages, false, ch)
				}
				return
			}

			// set deliveries property to first in the batch (all the same common header)
			if len(messages) == 0 {
				props = DeliveryPropsFrom(&msg)
			}
			// set data payload
			messages = append(messages, DeliveryDataFrom(&msg))

			// process
			if len(messages) == ch.opt.implParams.PrefetchCount {
				if len(messages) != 0 {
					// if link is still on, this test flies through.
					// If broken then a small delay won't mater anyway since we're recovering and
					// quite possibly client is dumping (want delivered again) this batch
					mustAck = mustAck && ch.AwaitStatus(true, 100*time.Millisecond)
					if _, current := ch.connected.Details(); transitions != current {
						// cannot push ACK if recoverying (fails sending) or recovered (new channel)
						mustAck = false
					}
					ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
				}
				messages = make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)
			}

		case <-time.After(ch.opt.implParams.PrefetchTimeout):
			kind := EventDataExhausted

			if len(messages) != 0 {
				kind = EventDataPartial
				// if link is still on this test flies through.
				// If broken then a small delay won't mater anyway since we're recovering and
				// quite possibly client is dumping (want delivered again) this batch
				mustAck = mustAck && ch.AwaitStatus(true, 100*time.Millisecond)
				if _, current := ch.connected.Details(); transitions != current {
					// cannot push ACK if recoverying (fails sending) or recovered (new channel)
					mustAck = false
				}
				ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
				messages = make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)
			}

			Event{
				SourceType: CliChannel,
				SourceName: ch.opt.name,
				Kind:       kind,
			}.raise(ch.opt.notifier)
		}
	}
}

// gobbleRecords runs the per-record concurrent dispatch loop for one recovery
// generation.
//
// It spawns ProcessorConcurrency workers that pull deliveries directly from the
// AMQP feed (no dispatcher goroutine and no internal work channel), then joins
// the pool before returning. Workers exit on feed close (generation end) or
// context cancellation, between records. Recovery does not cancel the channel
// context, so feed close is the normal generation-end signal; in-flight user
// code is never aborted.
func (ch *Channel) gobbleRecords(consumer <-chan amqp.Delivery) {
	workers := ch.opt.implParams.ProcessorConcurrency
	if workers <= 0 {
		return
	}

	var pool sync.WaitGroup
	pool.Add(workers)
	for range workers {
		go func() {
			defer pool.Done()
			ch.recordWorker(consumer)
		}()
	}

	// "gobble returned implies no outstanding user code" holds, delayed by at
	// most one record per worker.
	pool.Wait()
}

// recordWorker is one worker of the per-record pool. It selects on exactly the
// deliveries feed and context cancellation; there is no timer arm on this path.
func (ch *Channel) recordWorker(consumer <-chan amqp.Delivery) {
	for {
		select {
		case <-ch.opt.ctx.Done():
			return
		case msg, ok := <-consumer:
			if !ok {
				return
			}

			// Snapshot the generation before running user code: a transition that
			// lands while the record is being processed must prevent the settle
			// from hitting the new channel.
			_, transitions := ch.connected.Details()

			props := DeliveryPropsFrom(&msg)
			message := DeliveryDataFrom(&msg)

			process := ch.opt.cbProcessRecord
			if process == nil {
				process = ch.defaultRecordProcessor
			}
			outcome := process(&props, message)

			ch.settleRecord(message.DeliveryTag, outcome, transitions)
		}
	}
}

// settleRecord is the single place that executes a record outcome on the wire.
//
// Ack-worthiness is re-derived per record from ConsumerAutoAck (nothing sticky
// crosses records). The connected latch is then re-checked without blocking: if
// the channel is down, or its transition counter moved past the record's
// generation, the wire call is skipped and the record is left unacknowledged for
// redelivery. The one tolerated step is the establishment Unlock, which can land
// between a delivery being pulled and its settle: snapshot values are even before
// establishment and odd once connected, so exactly one even->odd step is the
// same generation still coming up.
//
// Exactly one of Ack(tag,false) / Nack(tag,false,true) / Nack(tag,false,false)
// is issued. A Reject would be wire-equivalent to the drop Nack and is
// deliberately not a third path. The TOCTOU window between this check and the
// wire call stays narrowed, not closed (see ADR-0008).
func (ch *Channel) settleRecord(tag uint64, outcome RecordOutcome, transitions uint64) {
	if ch.opt.implParams.ConsumerAutoAck {
		return
	}
	if ch.connected.Locked() {
		return
	}
	if _, current := ch.connected.Details(); current != transitions {
		// Allow only the establishment step for a record pulled while the latch
		// was still locked; any other change means a different generation.
		if transitions%2 != 0 || current != transitions+1 {
			return
		}
	}

	switch outcome {
	case RecordAck:
		_ = ch.Ack(tag, false)
	case RecordNackDrop:
		_ = ch.Nack(tag, false, false)
	default: // RecordNackRequeue and any unknown outcome fail safe
		_ = ch.Nack(tag, false, true)
	}
}
