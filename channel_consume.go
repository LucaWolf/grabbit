package grabbit

import (
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
