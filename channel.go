package grabbit

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

// Channel wraps the base amqp channel by creating a managed channel.
type Channel struct {
	baseChan  SafeBaseChan        // supporting amqp channel
	conn      *Connection         // managed connection
	paused    SafeBool            // flow status when of publisher type
	opt       ChannelOptions      // user parameters
	queue     string              // currently assigned work queue
	notifiers PersistentNotifiers // static amqp notification channels
	cancelCtx context.CancelFunc  // bolted on opt.ctx; aborts the reconnect loop
}

// NewChannel creates a managed channel.
// There shouldn't be any need to have direct access and is recommended
// using a [Consumer] or [Publisher] instead.
//
// The resulting channel inherits the events notifier, context and delayer
// from the master connection but all can be overridden by passing options/
//
// Use the 'WithChannelOption<OptionName>' for optionFuncs.
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
		baseChan:  SafeBaseChan{},
		notifiers: PersistentNotifiers{},
		opt:       *opt,
		conn:      conn,
	}

	ch.opt.ctx, ch.cancelCtx = context.WithCancel(opt.ctx)

	go func() {
		if !chanReconnectLoop(ch, false) {
			return
		}
		chanManager(ch)
	}()

	return ch
}

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

func chanNotifiersRefresh(ch *Channel) {
	ch.baseChan.mu.RLock()
	defer ch.baseChan.mu.RUnlock()

	if ch.baseChan.super != nil {
		ch.notifiers.Closed = ch.baseChan.super.NotifyClose(make(chan *amqp.Error))
		ch.notifiers.Cancel = ch.baseChan.super.NotifyCancel(make(chan string))

		// these are publishers specific
		if ch.opt.implParams.IsPublisher {
			ch.notifiers.Flow = ch.baseChan.super.NotifyFlow(make(chan bool))
			ch.notifiers.Published = ch.baseChan.super.NotifyPublish(make(chan amqp.Confirmation, ch.opt.implParams.ConfirmationCount))
			ch.notifiers.Returned = ch.baseChan.super.NotifyReturn(make(chan amqp.Return))

			if err := ch.baseChan.super.Confirm(ch.opt.implParams.ConfirmationNoWait); err != nil {
				event := Event{
					SourceType: CliChannel,
					SourceName: ch.opt.name,
					Kind:       EventConfirm,
					Err:        SomeErrFromError(err, err != nil),
				}
				raiseEvent(ch.opt.notifier, event)
			}
		}
		// consumer actions
		if ch.opt.implParams.IsConsumer {
			consumerSetup(ch)
			go consumerRun(ch)
		}
	}
}

func chanManager(ch *Channel) {
	for {
		select {
		case <-ch.opt.ctx.Done():
			ch.Close() // cancelCtx() called again but idempotent
			return
		case status := <-ch.notifiers.Flow:
			chanMarkPaused(ch, status)
		case confirm, notifierStatus := <-ch.notifiers.Published:
			if notifierStatus {
				ch.opt.cbNotifyPublish(confirm, ch)
			}
		case msg, notifierStatus := <-ch.notifiers.Returned:
			if notifierStatus {
				ch.opt.cbNotifyReturn(msg, ch)
			}
		case err, notifierStatus := <-ch.notifiers.Closed:
			if !chanRecover(ch, SomeErrFromError(err, err != nil), notifierStatus) {
				return
			}
		case reason, notifierStatus := <-ch.notifiers.Cancel:
			if !chanRecover(ch, SomeErrFromString(reason), notifierStatus) {
				return
			}
		}
	}
}

// chanRecover attempts recovery. Returns false if wanting to shut-down this channel
// or not possible as indicated by engine via err,notifierStatus
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
	return err.IsSet() && chanReconnectLoop(ch, true)
}

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

// chanReconnectLoop is called once for creating the initial connection then repeatedly as part of the
// chanManager->chanRecover maintenance.
// Returns false when channel was denied by callback or context.
func chanReconnectLoop(ch *Channel, recovering bool) bool {
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
			chanNotifiersRefresh(ch)
			return true
		}
		// context cancelled
		if !delayerCompleted(ch.opt.ctx, ch.opt.delayer, retry) {
			return false
		}
	}
}

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
			SourceName: t.Name,
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
			ch.queue = name
		}

		raiseEvent(ch.opt.notifier, event)
	}
}

// concurrent unsafe but called immediately after recovering
func declareExchange(ch *amqp.Channel, t *TopologyOptions) error {
	err := ch.ExchangeDeclare(t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
	if err == nil && t.Bind.Enabled {
		source, destination := t.GetRouting()
		err = ch.ExchangeBind(destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
	}

	return err
}

// concurrent unsafe but called immediately after recovering
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
