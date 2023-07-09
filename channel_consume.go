package grabbit

import (
	"time"
)

func consumerSetup(ch *Channel) {
	ch.notifiers.Consumer = nil

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
	ch.notifiers.Consumer = consumer
}

// consumerRun is the main receiving loop with distributing data processing to the user provided routine
func consumerRun(ch *Channel) {
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
		case msg, ok := <-ch.notifiers.Consumer: // notifiers data
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
