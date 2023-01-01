package grabbit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SafeBaseChan struct {
	Super *amqp.Channel // core channel
	Mu    sync.RWMutex  // makes this concurrent safe, maintenance wise only!
}

func (c *SafeBaseChan) IsSet() bool {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	return c.Super != nil
}

func (c *SafeBaseChan) set(super *amqp.Channel) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.Super = super
}

func (c *SafeBaseChan) reset() {
	c.set(nil)
}

// PersistentNotifiers are channels that have the lifespan of the channel. Only
// need refreshing when recovering.
type PersistentNotifiers struct {
	Published chan amqp.Confirmation // publishing confirmation
	Returned  chan amqp.Return       // returned messages
	Flow      chan bool              // flow control
	Closed    chan *amqp.Error       // channel closed
	Cancel    chan string            // channel cancelled
}

// Channel wraps the base amqp channel y creating a managed channel.
type Channel struct {
	baseChan SafeBaseChan // supporting amqp channel
	conn     *Connection  // managed connection
	paused   SafeBool     // flow status when of publisher type
	// available        SafeBool               // indicates availability. Deprecated.
	opt       ChannelOptions      // user parameters
	queue     string              // currently assigned work queue
	notifiers PersistentNotifiers // static amqp notification channels
	cancelCtx context.CancelFunc  // bolted on opt.ctx; aborts the reconnect loop
}

// // Available returns the overall status of the base connection
//
// Deprecated: use IsClosed instead for testing availability.
// func (ch *Channel) Available() bool {
// 	ch.available.mu.RLock()
// 	defer ch.available.mu.RUnlock()

// 	return ch.baseChan.IsSet() && ch.available.Value
// }

// IsPaused returns a publisher's flow status of the base channel
func (ch *Channel) IsPaused() bool {
	ch.paused.mu.RLock()
	defer ch.paused.mu.RUnlock()

	return ch.paused.Value
}

// IsClosed returns the availability/online status
func (ch *Channel) IsClosed() bool {
	ch.baseChan.Mu.RLock()
	defer ch.baseChan.Mu.RUnlock()

	return ch.baseChan.Super == nil || ch.baseChan.Super.IsClosed()
}

// Close wraps the base channel Close
func (ch *Channel) Close() error {
	ch.baseChan.Mu.RLock()
	defer ch.baseChan.Mu.RUnlock()

	var err error

	if ch.baseChan.Super != nil {
		err = ch.baseChan.Super.Close()
	}
	ch.cancelCtx()

	return err
}

// Channel returns the low level library channel for direct access if so
// desired. WARN: the Super may be nil and needs testing before using. Also make use of its mutex.
// Note: returning address as the inner Super may be changed by the recovering coroutine.
func (ch *Channel) Channel() *SafeBaseChan {
	return &ch.baseChan
}

// Queue returns the active queue. Useful for finding the server assigned name
func (ch *Channel) Queue() string {
	return ch.queue
}

// NewChannel creates a managed channel.
// There shouldn't be any need to have direct access and is recommended
// using a Consumer or Publisher instead.
// The resulting channel inherits the events notifier, context and delayer
// from the master connection but all can be overridden by passing options
func NewChannel(conn *Connection, optionFuncs ...func(*ChannelOptions)) *Channel {
	opt := &ChannelOptions{
		notifier:        conn.opt.notifier,
		name:            "default",
		delayer:         conn.opt.delayer,
		cbNotifyPublish: DefaultNotifyPublish,
		cbNotifyReturn:  DefaultNotifyReturn,
		ctx:             conn.opt.ctx,
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

	ch.paused.Value = value
}

// chanMarkAvailable sets the inner available attribute.
//
// Deprecated: use IsClosed instead for testing availability.
// func chanMarkAvailable(ch *Channel, value bool) {
// 	ch.available.mu.Lock()
// 	defer ch.available.mu.Unlock()

// 	ch.available.Value = value
// }

func chanNotifiersRefresh(ch *Channel) {
	ch.baseChan.Mu.RLock()
	defer ch.baseChan.Mu.RUnlock()

	if ch.baseChan.Super != nil {
		ch.notifiers.Closed = ch.baseChan.Super.NotifyClose(make(chan *amqp.Error))
		ch.notifiers.Cancel = ch.baseChan.Super.NotifyCancel(make(chan string))

		// these are publishers specific
		if ch.opt.implParams.IsPublisher {
			ch.notifiers.Flow = ch.baseChan.Super.NotifyFlow(make(chan bool))
			ch.notifiers.Published = ch.baseChan.Super.NotifyPublish(make(chan amqp.Confirmation, ch.opt.implParams.ConfirmationCount))
			ch.notifiers.Returned = ch.baseChan.Super.NotifyReturn(make(chan amqp.Return))

			ch.baseChan.Super.Confirm(ch.opt.implParams.ConfirmationNoWait)
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
			// chanMarkAvailable(ch, false)
			if !chanRecover(ch, err, notifierStatus) {
				return
			}
		case reason, notifierStatus := <-ch.notifiers.Cancel:
			// chanMarkAvailable(ch, false)
			if !chanRecover(ch, errors.New(reason), notifierStatus) {
				return
			}
		}
	}
}

// chanRecover attempts recovery. Returns false if wanting to shut-down this channel
// or not possible as indicated by engine via err,notifierStatus
func chanRecover(ch *Channel, err error, notifierStatus bool) bool {
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
	return err != nil && chanReconnectLoop(ch, true)
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
		event.Err = err
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
			// chanMarkAvailable(ch, true)
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
			Err:        err,
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
			event.Err = declareExchange(chLocal, t)
			name = t.Name
		} else {
			name, event.Err = declareQueue(chLocal, t)
		}
		// save a copy for back reference
		if t.IsDestination {
			ch.queue = name
		}

		raiseEvent(ch.opt.notifier, event)
	}
}

func declareExchange(ch *amqp.Channel, t *TopologyOptions) error {
	err := ch.ExchangeDeclare(t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
	if err == nil && t.Bind.Enabled {
		source, destination := t.GetRouting()
		err = ch.ExchangeBind(destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
	}

	return err
}

func declareQueue(ch *amqp.Channel, t *TopologyOptions) (string, error) {
	queue, err := ch.QueueDeclare(t.Name, t.Durable, t.AutoDelete, t.Exclusive, t.NoWait, t.Args)
	if err == nil {
		// sometimes the assigned name comes back empty. This is an indication of conn errors
		if len(queue.Name) == 0 {
			err = fmt.Errorf("cannot declare durable (%v) queue %s", t.Durable, t.Name)
		} else if t.Bind.Enabled {
			err = ch.QueueBind(queue.Name, t.Bind.Key, t.Bind.Peer, t.Bind.NoWait, t.Bind.Args)
		}
	}

	return queue.Name, err
}
