package grabbit

import "time"

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
	props := DeliveriesProperties{}
	tags := DeliveriesRange{}
	messages := make([]DeliveryPayload, 0, ch.opt.implParams.PrefetchCount)

	for {
		select {
		case <-ch.opt.ctx.Done(): // main chan and notifiers.Consumers should also be gone
			ch.Cancel(ch.opt.implParams.ConsumerName, true)
			if len(messages) != 0 {
				ch.opt.cbProcessMessages(&props, tags, messages, ch)
			}
			return
		case msg, ok := <-ch.notifiers.Consumer: // notifiers data
			if !ok {
				ch.Cancel(ch.opt.implParams.ConsumerName, true)
				if len(messages) != 0 {
					ch.opt.cbProcessMessages(&props, tags, messages, ch)
				}
				return
			}
			messages = append(messages, msg.Body)

			// set props
			if len(messages) == 1 {
				tags.First = msg.DeliveryTag
				tags.MustAck = !ch.opt.implParams.ConsumerAutoAck
				props.From(&msg)
			}
			tags.Last = msg.DeliveryTag

			// process
			if len(messages) == ch.opt.implParams.PrefetchCount {
				if len(messages) != 0 {
					ch.opt.cbProcessMessages(&props, tags, messages, ch)
				}
				messages = make([]DeliveryPayload, 0, ch.opt.implParams.PrefetchCount)
			}

		case <-time.After(ch.opt.implParams.PrefetchTimeout): // no more data or session abort
			if len(messages) != 0 {
				ch.opt.cbProcessMessages(&props, tags, messages, ch)
			}
			// let the client decide if they want to cancel/close this consumer
			event := Event{
				SourceType: CliConsumer,
				SourceName: ch.opt.name,
				Kind:       EventDataExhausted,
			}
			raiseEvent(ch.opt.notifier, event)
		}
	}
}
