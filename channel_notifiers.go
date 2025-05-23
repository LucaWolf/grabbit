package grabbit

import amqp "github.com/rabbitmq/amqp091-go"

// PersistentNotifiers are channels that have the lifespan of the channel. Only
// need refreshing when recovering.
type PersistentNotifiers struct {
	Published chan amqp.Confirmation // publishing confirmation
	Returned  chan amqp.Return       // returned messages
	Flow      chan bool              // flow control
	Closed    chan *amqp.Error       // channel closed
	Cancel    chan string            // channel cancelled
	Consumer  <-chan amqp.Delivery   // message intake
}

// notifiers refreshes the notifiers of a channel and returns the notifier channels.
// For publisher channels, it sets up notifiers for various events such as channel closure, cancellation, flow control, publishing confirmation,
// and returned messages. It also calls the Confirm method on baseChan.super to enable publisher confirms.
// For consumer channels, it calls the consumerSetup function to perform setup actions, and then starts a goroutine to run the consumer.
//
// It takes a pointer to a Channel as a parameter and returns PersistentNotifiers.
func (ch *Channel) notifiers() PersistentNotifiers {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	notifiers := PersistentNotifiers{}

	if ch.baseChan.super != nil {
		notifiers.Closed = ch.baseChan.super.NotifyClose(make(chan *amqp.Error))
		notifiers.Cancel = ch.baseChan.super.NotifyCancel(make(chan string))

		// these are publishers specific
		if ch.opt.implParams.IsPublisher {
			notifiers.Flow = ch.baseChan.super.NotifyFlow(make(chan bool))
			notifiers.Published = ch.baseChan.super.NotifyPublish(make(chan amqp.Confirmation, ch.opt.implParams.ConfirmationCount))
			notifiers.Returned = ch.baseChan.super.NotifyReturn(make(chan amqp.Return))

			// TODO extract this outside the notifiers handler as subsequent step
			// also make it optional as not all publishers want confirmation mode
			if err := ch.baseChan.super.Confirm(ch.opt.implParams.ConfirmationNoWait); err != nil {
				Event{
					SourceType: CliChannel,
					SourceName: ch.opt.name,
					Kind:       EventConfirm,
					Err:        SomeErrFromError(err, true),
				}.raise(ch.opt.notifier)
			}
		}
	}

	return notifiers
}
