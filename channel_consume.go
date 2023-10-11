package grabbit

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// consumerSetup sets up the consumer for the given channel.
//
// It takes a pointer to a Channel as a parameter.
// There is no return value.
func consumerSetup(ch *Channel) <-chan amqp.Delivery {
	if err := ch.baseChan.super.Qos(ch.opt.implParams.PrefetchCount, ch.opt.implParams.PrefetchSize, ch.opt.implParams.QosGlobal); err != nil {
		event := Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventQos,
			Err:        SomeErrFromError(err, err != nil),
		}
		raiseEvent(ch.opt.notifier, event)
	}
	// overwrite the passed queue to consume with the server assigned value
	qName := ch.opt.implParams.ConsumerQueue
	if len(ch.queue) != 0 {
		qName = ch.queue // only when IsDestination
	}

	consumer, err := ch.baseChan.super.Consume(qName,
		ch.opt.implParams.ConsumerName,
		ch.opt.implParams.ConsumerAutoAck,
		ch.opt.implParams.ConsumerExclusive,
		ch.opt.implParams.ConsumerNoLocal,
		ch.opt.implParams.ConsumerNoWait,
		ch.opt.implParams.ConsumerArgs)

	if err != nil {
		event := Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventConsume,
			Err:        SomeErrFromError(err, err != nil),
		}
		raiseEvent(ch.opt.notifier, event)
	}

	return consumer
}

// consumerRun runs the consumer function.
//
// It consumes messages from the given channel and processes them.
// When messages are received, they are stored in a slice called messages and processed when
// the number of messages reaches a certain count or the prefetch timeout is reached.
//
// The function takes a pointer to a Channel as a parameter.
// It does not return anything.
func consumerRun(ch *Channel, consumer <-chan amqp.Delivery) {
	var props DeliveriesProperties
	mustAck := !ch.opt.implParams.ConsumerAutoAck
	messages := make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)

	for {
		select {
		case <-ch.opt.ctx.Done(): // main chan and notifiers.Consumers should also be gone
			ch.Cancel(ch.opt.implParams.ConsumerName, true)
			if len(messages) != 0 {
				// conn/chan are gone, cannot ACK/NAK anyways
				mustAck = false
				ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
			}
			return
		case msg, ok := <-consumer: // notifiers data
			if !ok {
				ch.Cancel(ch.opt.implParams.ConsumerName, true)
				if len(messages) != 0 {
					// conn/chan are gone, cannot ACK/NAK anyways
					mustAck = false
					ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
				}
				return
			}

			// set props
			if len(messages) == 0 {
				props = DeliveryPropsFrom(&msg)
			}
			// set data payload
			messages = append(messages, DeliveryDataFrom(&msg))

			// process
			if len(messages) == ch.opt.implParams.PrefetchCount {
				if len(messages) != 0 {
					ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
				}
				messages = make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)
			}

		case <-time.After(ch.opt.implParams.PrefetchTimeout):
			event := Event{
				SourceType: CliConsumer,
				SourceName: ch.opt.name,
				Kind:       EventDataExhausted,
			}
			if len(messages) != 0 {
				event.Kind = EventDataPartial
				ch.opt.cbProcessMessages(&props, messages, mustAck, ch)
				messages = make([]DeliveryData, 0, ch.opt.implParams.PrefetchCount)
			}
			raiseEvent(ch.opt.notifier, event)
		}
	}
}
