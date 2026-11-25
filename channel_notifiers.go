package grabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// persistentNotifiers are go channels that have the lifespan of the Channel. Only
// need obtainig once for new amqp channels (like initial setup or recovering).
type persistentNotifiers struct {
	Published chan amqp.Confirmation // publishing confirmation
	Returned  chan amqp.Return       // returned messages
	Flow      chan bool              // flow control
	Closed    chan *amqp.Error       // channel closed
	Cancel    chan string            // channel cancelled
	Consumer  <-chan amqp.Delivery   // message intake
}

// notifiers refreshes the amqp notifiers of a channel.
// For publisher channels, it sets up notifiers for various events such as channel closure, cancellation, flow control, publishing confirmation,
// and returned messages. It also calls the Confirm method on baseChan.super to enable publisher confirms.
// For consumer channels, it calls the consumerSetup function to perform setup actions, and then starts a goroutine to run the consumer.
func (ch *Channel) refreshNotifiers() error {
	ch.baseChan.mu.Lock()
	if ch.baseChan.super != nil {
		// common notifiers
		// Buffered: on an error teardown the amqp channel's shutdown broadcasts the
		// close error before Close returns. On the context-cancellation path
		// manage is inside Close rather than selecting on this channel, so an
		// unbuffered send would deadlock shutdown (and any worker settling a
		// record behind the base channel mutex).
		ch.notifiers.Closed = ch.baseChan.super.NotifyClose(make(chan *amqp.Error, 1))
		ch.notifiers.Cancel = ch.baseChan.super.NotifyCancel(make(chan string))
		// publishers specific
		if ch.opt.implParams.IsPublisher {
			ch.notifiers.Flow = ch.baseChan.super.NotifyFlow(make(chan bool))
			ch.notifiers.Returned = ch.baseChan.super.NotifyReturn(make(chan amqp.Return))

			// zero channel capacity indicates client disables confirmations
			if ch.opt.implParams.ConfirmationCount > 0 {
				ch.notifiers.Published = ch.baseChan.super.NotifyPublish(
					make(chan amqp.Confirmation, ch.opt.implParams.ConfirmationCount),
				)
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
	}
	ch.baseChan.mu.Unlock()
	// consumers specific have own baseChan.super protection
	if ch.opt.implParams.IsConsumer {
		var err error
		if ch.notifiers.Consumer, err = ch.consumer(); err != nil {
			return err
		}
		// The dispatch model is fixed at construction: zero or negative keeps the
		// sequential batch loop unchanged; positive selects per-record workers.
		if ch.opt.implParams.ProcessorConcurrency > 0 {
			go ch.gobbleRecords(ch.notifiers.Consumer)
		} else {
			go ch.gobble(ch.notifiers.Consumer)
		}
	}
	return nil
}
