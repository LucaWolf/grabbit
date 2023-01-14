package grabbit

import (
	"context"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection wraps a [SafeBaseConn] with additional attributes
// (impl. details: rabbit URL, [ConnectionOptions] and a cancelling context).
// Applications should obtain a connection using [NewConnection].
type Connection struct {
	baseConn  SafeBaseConn       // supporting amqp connection
	address   string             // where to connect
	blocked   SafeBool           // TCP stream status
	opt       ConnectionOptions  // user parameters
	cancelCtx context.CancelFunc // bolted on opt.ctx; aborts the reconnect loop
}

// NewConnection creates a managed connection.

// Internally it derives a new WithCancel context from the passed (if any) context via
// [WithConnectionOptionContext]. Use the 'WithConnectionOption<OptionName>' for optionFuncs.
func NewConnection(address string, config amqp.Config, optionFuncs ...func(*ConnectionOptions)) *Connection {
	opt := &ConnectionOptions{
		notifier: make(chan Event),
		name:     "default",
		delayer:  DefaultDelayer{Value: 7500 * time.Millisecond},
		ctx:      context.Background(),
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(opt)
	}

	conn := &Connection{
		baseConn: SafeBaseConn{},
		address:  address,
		opt:      *opt,
	}

	conn.opt.ctx, conn.cancelCtx = context.WithCancel(opt.ctx)

	go func() {
		if !connReconnectLoop(conn, config) {
			return
		}
		connManager(conn, config)
	}()

	return conn
}

func connMarkBlocked(conn *Connection, value bool) {
	conn.blocked.mu.Lock()
	defer conn.blocked.mu.Unlock()

	event := Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventUnBlocked,
	}
	if value {
		event.Kind = EventBlocked
	}
	raiseEvent(conn.opt.notifier, event)

	conn.blocked.value = value
}

func (conn *Connection) connRefreshCredentials() {
	if conn.opt.credentials != nil {
		if secret, err := conn.opt.credentials.Password(); err == nil {
			if u, err := url.Parse(conn.address); err == nil {
				u.User = url.UserPassword(u.User.Username(), secret)
				conn.address = u.String()
			}
		}
	}
}

func connErrorNotifiers(conn *Connection) (evtClosed chan *amqp.Error, evtBlocked chan amqp.Blocking) {
	conn.baseConn.mu.RLock()
	defer conn.baseConn.mu.RUnlock()

	if conn.baseConn.super != nil {
		evtClosed = conn.baseConn.super.NotifyClose(make(chan *amqp.Error))
		evtBlocked = conn.baseConn.super.NotifyBlocked(make(chan amqp.Blocking)) // TODO: is this persistent (similar to chan.Flow?)
	}

	return
}

func connManager(conn *Connection, config amqp.Config) {
	for {
		evtClosed, evtBlocked := connErrorNotifiers(conn)

		select {
		case <-conn.opt.ctx.Done():
			conn.Close() // cancelCtx() called again but idempotent
			return
		case status := <-evtBlocked:
			connMarkBlocked(conn, status.Active)
		case err, notifierStatus := <-evtClosed:
			if !connRecover(conn, config, SomeErrFromError(err, err != nil), notifierStatus) {
				return
			}
		}
	}
}

// connRecover attempts recovery. Returns false if wanting to shut-down this connection
// or not possible as indicated by engine via err,notifierStatus
func connRecover(conn *Connection, config amqp.Config, err OptionalError, notifierStatus bool) bool {
	raiseEvent(conn.opt.notifier, Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventDown,
		Err:        err,
	})
	// abort by callback
	if !callbackAllowedDown(conn.opt.cbDown, conn.opt.name, err) {
		return false
	}

	if !notifierStatus {
		conn.baseConn.reset()

		raiseEvent(conn.opt.notifier, Event{
			SourceType: CliConnection,
			SourceName: conn.opt.name,
			Kind:       EventClosed,
		})
	}
	// no err means gracefully closed on demand
	return err.IsSet() && connReconnectLoop(conn, config)
}

func connDial(conn *Connection, config amqp.Config) bool {
	event := Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventUp,
	}
	result := true

	conn.connRefreshCredentials()

	if super, err := amqp.DialConfig(conn.address, config); err != nil {
		event.Err = SomeErrFromError(err, err != nil)
		event.Kind = EventCannotEstablish
		result = false
	} else {
		conn.baseConn.set(super)
	}

	raiseEvent(conn.opt.notifier, event)
	callbackDoUp(result, conn.opt.cbUp, conn.opt.name)

	return result
}

// It is called once for creating the initial connection then repeatedly as part of the
// connManager->connRecover maintenance.
// Returns false when connection was denied by callback or context.
func connReconnectLoop(conn *Connection, config amqp.Config) bool {
	retry := 0
	for {
		retry = (retry + 1) % 0xFFFF
		// not wanted
		if !callbackAllowedRecovery(conn.opt.cbReconnect, conn.opt.name, retry) {
			return false
		}

		if connDial(conn, config) {
			return true
		}
		// context cancelled
		if !delayerCompleted(conn.opt.ctx, conn.opt.delayer, retry) {
			return false
		}
	}
}
