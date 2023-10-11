package grabbit

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel wraps the base amqp channel by creating a managed channel.
type Channel struct {
	baseChan SafeBaseChan   // supporting amqp channel
	conn     *Connection    // managed connection
	paused   SafeBool       // flow status when of publisher type
	opt      ChannelOptions // user parameters
	queue    string         // currently assigned work queue
}

// NewChannel creates a new managed Channel with the given Connection and optional ChannelOptions.
// There shouldn't be any need to have direct access and is recommended
// using a [Consumer] or [Publisher] instead.
//
// The resulting channel inherits the events notifier, context and delayer
// from the master connection but all can be overridden by passing options.
// Use the 'WithChannelOption<OptionName>' for optionFuncs.
//
// Example Usage:
//
//	  chan := NewChannel(conn,
//	    WithChannelOptionName("myChannel"),
//	    WithChannelOptionDown(Down),
//	    WithChannelOptionUp(Up),
//		WithChannelOptionRecovering(Reattempting),
//		WithChannelOptionNotification(dataStatusChan),
//	    WithChannelOptionContext(ctx),
//	  )
//
// Parameters:
//   - conn: The Connection to associate the Channel with.
//   - optionFuncs: An optional list of functions to modify the ChannelOptions.
//
// Returns: A new Channel object.
func NewChannel(conn *Connection, optionFuncs ...func(*ChannelOptions)) *Channel {
	opt := &ChannelOptions{
		notifier:          conn.opt.notifier,
		name:              "default",
		delayer:           conn.opt.delayer,
		cbNotifyPublish:   defaultNotifyPublish,
		cbNotifyReturn:    defaultNotifyReturn,
		cbProcessMessages: defaultPayloadProcessor,
		ctx:               conn.opt.ctx,
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(opt)
	}

	ch := &Channel{
		baseChan: SafeBaseChan{},
		opt:      *opt,
		conn:     conn,
	}

	ch.opt.ctx, ch.opt.cancelCtx = context.WithCancel(opt.ctx)

	go func() {
		if !ch.reconnectLoop(false) {
			return
		}
		ch.manager()
	}()

	return ch
}

// chanMarkPaused marks a channel as paused or unpaused.
//
// It takes a pointer to a Channel and a boolean value as parameters.
// The function raises an event to indicate whether the channel is blocked or unblocked,
// and updates the paused value of the Channel accordingly.
func chanMarkPaused(ch *Channel, value bool) {
	ch.paused.mu.Lock()
	defer ch.paused.mu.Unlock()

	event := Event{
		SourceType: CliConnection,
		SourceName: ch.opt.name,
		Kind:       EventUnBlocked,
	}
	if value {
		event.Kind = EventBlocked
	}
	raiseEvent(ch.opt.notifier, event)

	ch.paused.value = value
}

// manager manages the channels.
//
// It isolates all notifiers from the 'ch' object and handles various
// cases using a select statement. It listens to the context done channel
// to close the channel and return. It also handles other cases such as
// paused status, published confirmations, returned messages, closed
// errors, and cancellation reasons.
//
// Parameters:
//   - ch: a pointer to the Channel object.
//
// Return type: None.
func (ch *Channel) manager() {
	for {
		notifiers := chanNotifiersRefresh(ch)
		if ch.opt.implParams.IsConsumer {
			go consumerRun(ch, notifiers.Consumer)
		}

		select {
		case <-ch.opt.ctx.Done():
			ch.Close() // cancelCtx() called again but idempotent
			return
		case status := <-notifiers.Flow:
			chanMarkPaused(ch, status)
		case confirm, notifierStatus := <-notifiers.Published:
			if notifierStatus {
				ch.opt.cbNotifyPublish(confirm, ch)
			}
		case msg, notifierStatus := <-notifiers.Returned:
			if notifierStatus {
				ch.opt.cbNotifyReturn(msg, ch)
			}
		case err, notifierStatus := <-notifiers.Closed:
			if !chanRecover(ch, SomeErrFromError(err, err != nil), notifierStatus) {
				return
			}
		case reason, notifierStatus := <-notifiers.Cancel:
			if !chanRecover(ch, SomeErrFromString(reason), notifierStatus) {
				return
			}
		}
	}
}

// chanRecover recovers from a channel error and handles the necessary events and callbacks.
//
// Parameters:
//   - ch: a pointer to the Channel object.
//   - err: an OptionalError value representing the error occurred.
//   - notifierStatus: a boolean indicating the status of the notifier.
//
// Returns:
//   - a boolean value indicating whether the recovery was successful.
func chanRecover(ch *Channel, err OptionalError, notifierStatus bool) bool {
	raiseEvent(ch.opt.notifier, Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       EventDown,
		Err:        err,
	})
	// abort by callback
	if !callbackAllowedDown(ch.opt.cbDown, ch.opt.name, err) {
		return false
	}

	if !notifierStatus {
		ch.baseChan.reset()

		raiseEvent(ch.opt.notifier, Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventClosed,
		})
	}

	// no err means gracefully closed on demand
	return err.IsSet() && ch.reconnectLoop(true)
}

// chanGetNew tries to establish a connection to the channel and returns a boolean indicating success.
// It sends a channel event to the notifier with either EventUp or EventCannotEstablish, depending
// on the new channel status.
//
// It takes a pointer to a Channel struct as a parameter.
// It returns a boolean value.
func chanGetNew(ch *Channel) bool {
	event := Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       EventUp,
	}
	result := true

	if super, err := ch.conn.Channel(); err != nil {
		event.Kind = EventCannotEstablish
		event.Err = SomeErrFromError(err, err != nil)
		result = false
	} else {
		ch.baseChan.set(super)
	}

	raiseEvent(ch.opt.notifier, event)
	callbackDoUp(result, ch.opt.cbUp, ch.opt.name)

	return result
}

// reconnectLoop is a function that performs a reconnection loop for a given channel.
//
// It takes a *Channel pointer as its parameter, which represents the channel to reconnect, and a boolean
// value indicating whether the channel is recovering.
//
// The function returns a boolean value, which indicates whether the reconnection loop was successful or not.
func (ch *Channel) reconnectLoop(recovering bool) bool {
	retry := 0
	for {
		retry = (retry + 1) % 0xFFFF
		// not wanted
		if !callbackAllowedRecovery(ch.opt.cbReconnect, ch.opt.name, retry) {
			return false
		}

		if chanGetNew(ch) {
			// cannot decide (yet) which infra is critical, let the caller decide via the raised events
			chanMakeTopology(ch, recovering)
			return true
		}
		// context cancelled
		if !delayerCompleted(ch.opt.ctx, ch.opt.delayer, retry) {
			return false
		}
	}
}

// chanMakeTopology creates topology for a channel.
//
// The function takes a channel (ch) and a boolean flag (recovering) as parameters.
//
// It creates a local isolated channel (chLocal) and handles any errors that occur during this process.
// It then iterates over the topology of the channel and performs the necessary operations based on the topology configuration.
//   - if the topology element is an exchange, it declares the exchange using the declareExchange function.
//   - if the topology element is a queue, it declares the queue using the declareQueue function.
//   - if the topology element is marked as a destination, it saves a copy of the name for back reference.
//
// Finally, it raises an event for each topology element.
func chanMakeTopology(ch *Channel, recovering bool) {
	// Channels are not concurrent data/usage wise!
	// prefer using a local isolated channel.
	chLocal, err := ch.conn.Channel()
	if err != nil {
		event := Event{
			SourceType: CliChannel,
			SourceName: "topology.auto",
			Kind:       EventCannotEstablish,
			Err:        SomeErrFromError(err, err != nil),
		}
		raiseEvent(ch.opt.notifier, event)
		return
	}
	defer chLocal.Close()

	for _, t := range ch.opt.topology {
		if !t.Declare || (recovering && t.Durable) {
			continue
		}

		event := Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			TargetName: t.Name,
			Kind:       EventDefineTopology,
		}

		var name string

		if t.IsExchange {
			err = declareExchange(chLocal, t)
			event.Err = SomeErrFromError(err, err != nil)
			name = t.Name
		} else {
			queue, err := declareQueue(chLocal, t)
			event.Err = SomeErrFromError(err, err != nil)
			name = queue.Name
		}
		// save a copy for back reference
		if t.IsDestination {
			ch.baseChan.mu.Lock()
			ch.queue = name
			ch.baseChan.mu.Unlock()
		}

		raiseEvent(ch.opt.notifier, event)
	}
}

// declareExchange is a function that declares an exchange in RabbitMQ.
//
// It takes in a *amqp.Channel and a *TopologyOptions as parameters.
// It returns an error.
func declareExchange(ch *amqp.Channel, t *TopologyOptions) error {
	err := ch.ExchangeDeclare(t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
	if err == nil && t.Bind.Enabled {
		source, destination := t.GetRouting()
		err = ch.ExchangeBind(destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
	}

	return err
}

// declareQueue declares a queue and performs additional operations if successful.
//
// Parameters:
//   - ch: Pointer to an amqp.Channel object.
//   - t: Pointer to a TopologyOptions object.
//
// Returns:
//   - amqp.Queue: The declared queue.
//   - error: An error object if there was an issue with the declaration or the additional operations.
func declareQueue(ch *amqp.Channel, t *TopologyOptions) (amqp.Queue, error) {
	queue, err := ch.QueueDeclare(t.Name, t.Durable, t.AutoDelete, t.Exclusive, t.NoWait, t.Args)
	if err == nil {
		// sometimes the assigned name comes back empty. This is an indication of conn errors
		if len(queue.Name) == 0 {
			err = fmt.Errorf("cannot declare durable (%v) queue %s", t.Durable, t.Name)
		} else if t.Bind.Enabled {
			err = ch.QueueBind(queue.Name, t.Bind.Key, t.Bind.Peer, t.Bind.NoWait, t.Bind.Args)
		}
	}

	return queue, err
}
