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
	paused    latch.NotifyingLatch // flow status when of publisher type
	opt       ChannelOptions       // user parameters
	queue     string               // currently assigned work queue
	notifiers persistentNotifiers  // amqp channel operational notifiers
	connected latch.NotifyingLatch // establishment internally tracked status
	managed   latch.NotifyingLatch // managing goroutine status
}

// AwaitManager waits till the managing goroutine is in the desired state or timeout expires.
// Provided as a helper tool to confirm the lifespan of the Connection has expired
// (no goroutine leak) when closed and not usually called from the user application layer.
func (ch *Channel) AwaitManager(active bool, timeout time.Duration) bool {
	return awaitOnLatch(&ch.managed, ch.opt.ctx, active, timeout)
}

// AwaitAvailable waits till the channel is established or timeout expires.
//
// Deprecated: replaced by AwaitStatus.
func (ch *Channel) AwaitAvailable(timeout time.Duration) bool {
	return ch.AwaitStatus(true, timeout)
}

// AwaitStatus waits till the channel is in the desired state or timeout expires.
// Pass 'true' for testing open/ready, 'false' for testing if closed.
//
// This is a bit safer than [IsClosed] method since it tracks an internal state that
// flow wraps (after) the base amqp.Channel status change.
func (ch *Channel) AwaitStatus(established bool, timeout time.Duration) bool {
	return awaitOnLatch(&ch.connected, ch.opt.ctx, established, timeout)
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
		managed:   latch.NewLatch(true, opt.name),
		paused:    latch.NewLatch(false, opt.name), // active by default
	}

	go func() {
		ch.managed.Unlock()
		defer ch.managed.Lock()

		if !ch.reconnectLoop(false) {
			return
		}
		ch.manage()
	}()

	return ch
}

// pause marks the channel as paused or unpaused.
//
// It takes a boolean value as a parameter. Server sends 'false' to suspend traffic.
// The method raises an event to indicate whether the channel is blocked or unblocked,
// and updates the paused value of the Channel accordingly.
func (ch *Channel) pause(value bool) {
	kind := EventUnBlocked

	if value {
		ch.paused.Unlock()
	} else {
		ch.paused.Lock()
		kind = EventBlocked
	}

	Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       kind,
	}.raise(ch.opt.notifier)
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
			if err := ch.refreshNotifiers(); err == nil {
				// delayed considered completion: there might be functionality
				// depending on consuming/publishing notifications being setup
				ch.connected.Unlock()
				recovering = false
			} else {
				// cannot do these basics even after a recovery/initial link-up?
				if !ch.recover(SomeErrFromError(err, true), true) {
					return
				}
				<-time.After(ch.opt.delayer.Delay(3))
				continue
			}
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
				ch.baseChan.reset() // ASAP
				ch.connected.Lock() // ASAP
				if !ch.recover(SomeErrFromError(err, err != nil), notifierStatus) {
					return
				}
				recovering = true
			case reason, notifierStatus := <-ch.notifiers.Cancel:
				ch.baseChan.reset() // ASAP
				ch.connected.Lock() // ASAP
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

	connected := ch.conn.AwaitStatus(true, ch.opt.delayer.Delay(retry))
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
