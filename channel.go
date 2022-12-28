package grabbit

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel wraps the base amqp channel.
type Channel struct {
	baseChan         *amqp.Channel          // core channel
	conn             *Connection            // managed connection
	paused           SafeBool               // flow status when of publisher type
	opt              ChannelOptions         // user parameters
	queue            amqp.Queue             // currently assigned work queue
	chanNotifyPub    chan amqp.Confirmation // persistent (till baseChan redone)
	chanNotifyReturn chan amqp.Return       // persistent (till baseChan redone)
	cancelCtx        context.CancelFunc     // bolted on opt.ctx; aborts the reconnect loop
}

// IsPaused returns a publisher's flow status of the base channel
func (ch *Channel) IsPaused() bool {
	ch.paused.mu.RLock()
	defer ch.paused.mu.RUnlock()

	return ch.paused.Value
}

// IsClosed returns the availability/online status
func (ch *Channel) IsClosed() bool {
	return ch.baseChan == nil
}

// Close wraps the base channel Close
func (ch *Channel) Close() error {
	var err error

	if ch.baseChan != nil {
		err = ch.baseChan.Close()
	}
	ch.cancelCtx()

	return err
}

// Channel returns the low level library channel for direct access if so
// desired. WARN: the result may be nil and needs testing before using
func (ch *Channel) Channel() *amqp.Channel {
	return ch.baseChan
}

// Queue returns the active queue. Useful for finding the server assigned name
func (ch *Channel) Queue() *amqp.Queue {
	return &ch.queue
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
		opt:  *opt,
		conn: conn,
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

func chanNotifyClose(ch *Channel) chan *amqp.Error {
	if ch.baseChan == nil {
		return nil
	} else {
		return ch.baseChan.NotifyClose(make(chan *amqp.Error))
	}
}

func chanNotifyCancel(ch *Channel) chan string {
	if ch.baseChan == nil {
		return nil
	} else {
		return ch.baseChan.NotifyCancel(make(chan string))
	}
}

func chanNotifyFlow(ch *Channel) chan bool {
	if ch.baseChan == nil {
		return nil
	} else {
		return ch.baseChan.NotifyFlow(make(chan bool))
	}
}

func chanNotifyPublish(ch *Channel) chan amqp.Confirmation {
	if ch.baseChan == nil || !ch.opt.isPublisher {
		return nil
	} else {
		return ch.baseChan.NotifyPublish(make(chan amqp.Confirmation))
	}
}

func chanNotifyReturn(ch *Channel) chan amqp.Return {
	if ch.baseChan == nil || !ch.opt.isPublisher {
		return nil
	} else {
		return ch.baseChan.NotifyReturn(make(chan amqp.Return))
	}
}

func chanManager(ch *Channel) {
	for {
		select {
		case <-ch.opt.ctx.Done():
			return
		case status := <-chanNotifyFlow(ch):
			chanMarkPaused(ch, status)

		case confirm, notifierStatus := <-ch.chanNotifyPub:
			if notifierStatus {
				ch.opt.cbNotifyPublish(confirm, ch)
			}
		case msg, notifierStatus := <-ch.chanNotifyReturn:
			if notifierStatus {
				ch.opt.cbNotifyReturn(msg, ch)
			}
		case err, notifierStatus := <-chanNotifyClose(ch):
			if !chanRecover(ch, err, notifierStatus) {
				return
			}
		case reason, notifierStatus := <-chanNotifyCancel(ch):
			if !chanRecover(ch, errors.New(reason), notifierStatus) {
				return
			}
			// The rest of notifications are publisher or consumer specific.
			// Since they may involve custom routines for each case and channel type
			// is better to capture and treat them in their respective implementation
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
		ch.baseChan = nil
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

	ch.baseChan, event.Err = ch.conn.Channel()
	if event.Err != nil {
		event.Kind = EventCannotEstablish
		result = false
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
		// aborted
		if !(callbackAllowedRecovery(ch.opt.cbReconnect, ch.opt.name, retry) &&
			delayerCompleted(ch.opt.ctx, ch.opt.delayer, retry)) {
			return false
		}

		if chanGetNew(ch) {
			// cannot decide (yet) which infra is critical, let the caller decide via the raised events
			chanMakeTopology(ch, recovering)

			if ch.opt.isPublisher {
				ch.chanNotifyPub = chanNotifyPublish(ch)
				ch.chanNotifyReturn = chanNotifyReturn(ch)
				ch.baseChan.Confirm(ch.opt.implParams.ConfirmationNoWait)
			}
			return true
		}
	}
}

func chanMakeTopology(ch *Channel, recovering bool) {
	for _, t := range ch.opt.topology {
		var err error
		var queue amqp.Queue

		if !t.Declare || (recovering && t.Durable) {
			return // already declared
		}

		if t.IsExchange {
			err = ch.baseChan.ExchangeDeclare(t.Name, t.Kind, t.Durable, t.AutoDelete, t.Internal, t.NoWait, t.Args)
			if err != nil && t.Bind.Enabled {
				source, destination := t.TopologyGetRouting()
				err = ch.baseChan.ExchangeBind(destination, t.Bind.Key, source, t.Bind.NoWait, t.Bind.Args)
			}
		} else {
			queue, err = ch.baseChan.QueueDeclare(t.Name, t.Durable, t.AutoDelete, t.Exclusive, t.NoWait, t.Args)
			// sometimes the assigned name comes back empty. This is an indication of conn errors
			if len(queue.Name) == 0 {
				err = fmt.Errorf("cannot declare durable (%v) queue %s", t.Durable, t.Name)
			}
			if t.IsDestination { // save a copy for back reference
				ch.queue = queue
			}
			if err != nil && t.Bind.Enabled {
				err = ch.baseChan.QueueBind(ch.queue.Name, t.Bind.Key, t.Bind.Peer, t.Bind.NoWait, t.Bind.Args)
			}
		}

		if err != nil {
			event := Event{
				SourceType: CliChannel,
				SourceName: ch.opt.name,
				Kind:       EventDefineTopology,
				Err:        err,
			}
			raiseEvent(ch.opt.notifier, event)
		}
	}
}
