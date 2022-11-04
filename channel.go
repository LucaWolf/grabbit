package grabbit

import (
	"context"
	"errors"
	"time"

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

// Close wraps the base connection Close
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
// There should be need to have direct access and you'd be better off
// using a consumer or publisher instead
func NewChannel(conn *Connection, optionFuncs ...func(*ChannelOptions)) *Channel {
	opt := &ChannelOptions{
		notifier: make(chan Event, 5),
		name:     "default",
		delayer:  DefaultDelayer{Value: 7500 * time.Millisecond},
		ctx:      context.Background(),
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(opt)
	}

	ch := &Channel{
		opt: *opt,
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

func chanMarkBlocked(ch *Channel, value bool) {
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
	RaiseEvent(ch.opt.notifier, event)

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
		case blk := <-chanNotifyFlow(ch):
			chanMarkBlocked(ch, blk)
		case err, notifierStatus := <-chanNotifyClose(ch):
			if !chanRecover(ch, err, notifierStatus) {
				return
			}
		case reason, notifierStatus := <-chanNotifyCancel(ch):
			if !chanRecover(ch, errors.New(reason), notifierStatus) {
				return
			}
			// Design: the rest of notifications are publisher or consumer specific.
			// Since they may involve custom routines for each case and channel type
			// is better to capture and treat them in their respective implementation
		}
	}
}

// chanRecover attempts recovery. Returns true if wanting to shut-down this channel
// or not possible as indicated by engine via err,notifierStatus
func chanRecover(ch *Channel, err error, notifierStatus bool) bool {
	// async
	RaiseEvent(ch.opt.notifier, Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       EventDown,
		Err:        err,
	})
	// sync
	if ch.opt.cbDown != nil && !ch.opt.cbDown(ch.opt.name, err) {
		return true
	}

	if !notifierStatus {
		ch.baseChan = nil
		RaiseEvent(ch.opt.notifier, Event{
			SourceType: CliChannel,
			SourceName: ch.opt.name,
			Kind:       EventClosed,
		})
	}

	// no err means gracefully closed on demand
	TODO not happy with this bool logic. Review required !!!
	if !(err == nil || chanReconnectLoop(ch)) {
		return true
	}

	return false
}

func chanGetNew(ch *Channel) bool {
	event := Event{
		SourceType: CliChannel,
		SourceName: ch.opt.name,
		Kind:       EventUp,
	}
	available := true

	ch.baseChan, event.Err = ch.conn.Channel()
	if event.Err != nil {
		event.Kind = EventCannotEstablish
		available = false
	}

	// async
	RaiseEvent(ch.opt.notifier, event)
	// sync
	if available && ch.opt.cbUp != nil {
		ch.opt.cbUp(ch.opt.name)
	}

	return available
}

func chanReconnectLoop(ch *Channel) bool {
	retry := 0

	for {
		retry = (retry + 1) % 0xFFFF

		// sync
		if ch.opt.cbReconnect != nil && !ch.opt.cbReconnect(ch.opt.name, retry) {
			return false
		}

		select {
		case <-ch.opt.ctx.Done():
			return false
		case <-time.After(ch.opt.delayer.Delay(retry)):
		}

		if chanGetNew(ch) {
			return true
		}
	}
}
