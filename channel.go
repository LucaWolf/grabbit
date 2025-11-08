package grabbit

import (
	"context"
	"fmt"
	"time"

	trace "traceutils"

	latch "github.com/LucaWolf/go-notifying-latch"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel wraps the base amqp channel by creating a managed channel.
type Channel struct {
	baseChan  SafeBaseChan         // supporting amqp channel
	conn      *Connection          // managed connection
	paused    SafeBool             // flow status when of publisher type
	opt       ChannelOptions       // user parameters
	queue     string               // currently assigned work queue
	connected latch.NotifyingLatch // safe notifiers
	notifiers persistentNotifiers  // amqp channel operational notifiers
	active    latch.NotifyingLatch // managing goroutine status
}

// RecoveryNotifier returns a channel that will emit the current recovery status
// each time the connection recovers.
func (ch *Channel) RecoveryNotifier() *latch.NotifyingLatch {
	return &ch.connected
}

func (ch *Channel) ManagerNotifier() *latch.NotifyingLatch {
	return &ch.active
}

// AwaitAvailable waits till the channel is established or timeout expires.
func (ch *Channel) AwaitAvailable(timeout time.Duration) bool {
	if timeout == 0 {
		timeout = 7500 * time.Millisecond
	}

	return ch.connected.Wait(ch.opt.ctx, false, timeout)
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
		name:              "chan.default",
		delayer:           conn.opt.delayer,
		cbNotifyPublish:   defaultNotifyPublish,
		cbNotifyReturn:    defaultNotifyReturn,
		cbProcessMessages: defaultPayloadProcessor,
		ctx:               conn.opt.ctx,
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(opt)
	}
	opt.ctx, opt.cancelCtx = context.WithCancel(opt.ctx)

	ch := &Channel{
		baseChan:  SafeBaseChan{},
		opt:       *opt,
		conn:      conn,
		connected: latch.NewLatch(true, opt.name),
		active:    latch.NewLatch(true, opt.name),
	}

	go func() {
		ch.active.Unlock()
		defer ch.active.Lock()

		if !ch.reconnectLoop(false) {
			return
		}
		ch.manage()
	}()

	return ch
}

// pause marks the channel as paused or unpaused.
//
// It takes a boolean value as a parameter.
// The method raises an event to indicate whether the channel is blocked or unblocked,
// and updates the paused value of the Channel accordingly.
func (ch *Channel) pause(value bool) {
	ch.paused.mu.Lock()
	defer ch.paused.mu.Unlock()

	kind := EventUnBlocked
	if value {
		kind = EventBlocked
	}

	Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       kind,
	}.raise(ch.opt.notifier)

	ch.paused.value = value
}

// manage keep the channel alive.
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
func (ch *Channel) manage() {
	recovering := true

	for {
		if recovering {
			ch.refreshNotifiers()
			// delayed considered completion: there might be functionality
			// depending on consuming/publishing notifications being setup
			ch.connected.Unlock()
			recovering = false
		}

		// must wait one of these events
		for {
			innerLoop := false
			select {
			case <-ch.opt.ctx.Done():
				ch.Close() // cancelCtx() called again but idempotent
				ch.connected.Lock()
				return
			case status := <-ch.notifiers.Flow:
				ch.pause(status)
			case confirm, notifierStatus := <-ch.notifiers.Published:
				if notifierStatus {
					ch.opt.cbNotifyPublish(confirm, ch)
				}
			case msg, notifierStatus := <-ch.notifiers.Returned:
				if notifierStatus {
					ch.opt.cbNotifyReturn(msg, ch)
				}
			case err, notifierStatus := <-ch.notifiers.Closed:
				if !ch.recover(SomeErrFromError(err, err != nil), notifierStatus) {
					return
				}
				recovering = true
			case reason, notifierStatus := <-ch.notifiers.Cancel:
				if !ch.recover(SomeErrFromString(reason), notifierStatus) {
					return
				}
				recovering = true
			// avoid a hot loop
			case <-time.After(120 * time.Second):
				innerLoop = true
			}
			if !innerLoop {
				break
			}
		}
	}
}

// recover recovers from a channel error and handles the necessary events and callbacks.
//
// Parameters:
//   - ch: a pointer to the Channel object.
//   - err: an OptionalError value representing the error occurred.
//   - notifierStatus: a boolean indicating the status of the notifier.
//
// Returns:
//   - a boolean value indicating whether the recovery was successful.
func (ch *Channel) recover(err OptionalError, notifierStatus bool) bool {
	ch.connected.Lock()
	Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       EventDown,
		Err:        err,
	}.raise(ch.opt.notifier)
	// abort by callback
	if !callbackAllowedDown(ch.opt.cbDown, ch.opt.name, err) {
		return false
	}

	if !notifierStatus {
		ch.baseChan.reset()

		Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventClosed,
		}.raise(ch.opt.notifier)
	}

	// no err means gracefully closed on demand
	return err.IsSet() && ch.reconnectLoop(true)
}

// rebase establishes a new base channel to the AMQP server using the given configuration.
// It sends en event notification with either EventUp or EventCannotEstablish, depending
// on the new channel status.
//
// It returns a boolean value indicating whether the channel was
// successfully established.
func (ch *Channel) rebase(retry int) bool {
	kind := EventUp
	result := true
	optError := OptionalError{}

	connected := ch.conn.AwaitAvailable(ch.opt.delayer.Delay(retry))
	if !connected {
		Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventCannotEstablish,
			Err:        SomeErrFromString("connection recovery timeout"),
		}.raise(ch.opt.notifier)
		return false
	}

	if super, err := ch.conn.Channel(); err != nil {
		kind = EventCannotEstablish
		optError = SomeErrFromError(err, true)
		result = false
	} else {
		ch.baseChan.set(super)
	}

	Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       kind,
		Err:        optError,
	}.raise(ch.opt.notifier)
	callbackDoUp(result, ch.opt.cbUp, ch.opt.name)

	return result
}

// reconnectLoop is a function that performs a reconnection loop for a given channel.
//
// It takes a pointer to a Channel as its parameter, which represents the channel to reconnect, and a boolean
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

		if ch.rebase(retry) {
			// cannot decide (yet) which infra is critical, let the caller decide via the raised events
			ch.makeTopology(recovering)
			return true
		}
		// context cancelled; minimum timer as we already waited the connection up in rebase
		if !delayerCompleted(ch.opt.ctx, ch.opt.delayer, 1) {
			return false
		}
	}
}

// makeTopology creates topology for a channel.
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
func (ch *Channel) makeTopology(recovering bool) {
	// Channels are not concurrent data/usage wise!
	// prefer using a local isolated channel.
	chLocal, err := ch.conn.Channel()
	if err != nil {
		Event{
			SourceType: CliChannel,
			SourceName: "topology.auto",
			Kind:       EventCannotEstablish,
			Err:        SomeErrFromError(err, true),
		}.raise(ch.opt.notifier)
		return
	}
	defer chLocal.Close()

	for _, t := range ch.opt.topology {
		if !t.Declare || (recovering && t.Durable) {
			continue
		}

		var name string
		var optError OptionalError

		if t.IsExchange {
			err := declareExchange(ch.opt.ctx, chLocal, t)
			optError = SomeErrFromError(err, err != nil)
			name = t.Name
		} else {
			queue, err := declareQueue(ch.opt.ctx, chLocal, t)
			optError = SomeErrFromError(err, err != nil)
			name = queue.Name
		}
		// save a copy for back reference
		if t.IsDestination {
			ch.baseChan.mu.Lock()
			ch.queue = name
			ch.baseChan.mu.Unlock()
		}

		Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			TargetName: t.Name,
			Kind:       EventDefineTopology,
			Err:        optError,
		}.raise(ch.opt.notifier)
	}
}

// declareExchange is a function that declares an exchange in RabbitMQ.
//
// It takes in a *amqp.Channel and a *TopologyOptions as parameters.
// It returns an error if the exchange declaration fails or the any binding fails.
func declareExchange(ctx context.Context, ch *amqp.Channel, t *TopologyOptions) error {
	if t.Kind == "" { // default
		t.Kind = "direct"
	}
	err := ch.ExchangeDeclare(t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
	trace.ExchangeDeclare(ctx, t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
	if err == nil && t.Bind.Enabled {
		source, destination := t.GetRouting()
		err = ch.ExchangeBind(destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
		trace.ExchangeBind(ctx, destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
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
//   - error: set if issues with the declaration or any queue binding operations.
func declareQueue(ctx context.Context, ch *amqp.Channel, t *TopologyOptions) (amqp.Queue, error) {
	queue, err := ch.QueueDeclare(t.Name, t.Durable, t.AutoDelete, t.Exclusive, t.NoWait, t.Args)
	trace.QueueDeclare(ctx, t.Name, t.Durable, t.AutoDelete, t.Exclusive, t.NoWait, t.Args)
	if err == nil {
		// sometimes the assigned name comes back empty. This is an indication of conn errors
		if len(queue.Name) == 0 {
			err = fmt.Errorf("cannot declare durable (%v) queue %s", t.Durable, t.Name)
		} else if t.Bind.Enabled {
			err = ch.QueueBind(queue.Name, t.Bind.Key, t.Bind.Peer, t.Bind.NoWait, t.Bind.Args)
			trace.QueueBind(ctx, queue.Name, t.Bind.Key, t.Bind.Peer, t.Bind.NoWait, t.Bind.Args)
		}
	}

	return queue, err
}
