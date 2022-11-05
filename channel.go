package grabbit

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel wraps the base amqp channel.
type Channel struct {
	baseChan  *amqp.Channel      // core channel
	conn      *Connection        // managed connection
	paused    SafeBool           // flow status when of publisher type
	opt       ChannelOptions     // user parameters
	cancelCtx context.CancelFunc // bolted on opt.ctx; aborts the reconnect loop
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

// NewChannel creates a managed channel.
// There shouldn't be any need to have direct access and is recommended
// using a Consumer or Publisher instead.
// The resulting channel inherits the events notifier, context and delayer
// from the master connection but all can be overridden by passing options
func NewChannel(conn *Connection, optionFuncs ...func(*ChannelOptions)) *Channel {
	opt := &ChannelOptions{
		notifier: conn.opt.notifier,
		name:     "default",
		delayer:  conn.opt.delayer,
		ctx:      conn.opt.ctx,
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
		if !chanReconnectLoop(ch) {
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

func chanManager(ch *Channel) {
	for {
		select {
		case <-ch.opt.ctx.Done():
			return
		case status := <-chanNotifyFlow(ch):
			chanMarkPaused(ch, status)
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
	return err != nil && chanReconnectLoop(ch)
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

// chanReconnectLoop returns false when channel was denied by callback or context
func chanReconnectLoop(ch *Channel) bool {
	retry := 0
	for {
		retry = (retry + 1) % 0xFFFF
		// aborted
		if !(callbackAllowedRecovery(ch.opt.cbReconnect, ch.opt.name, retry) &&
			delayerCompleted(ch.opt.ctx, ch.opt.delayer, retry)) {
			return false
		}

		if chanGetNew(ch) {
			return true
		}
	}
}
