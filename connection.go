package grabbit

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SafeBaseConn wraps in a concurrency safe way the low level amqp.Connection.
// The fields are made public in order to allow full amqp operations range over the connection
// (like for example creating an application layer, non-managed amqp channel).
//
// Warnings: to be used externally in ready only mode! Reserve short lived operations via the mutex;
// this prevents the maintenance routine from refreshing a lost connection!
type SafeBaseConn struct {
	Super *amqp.Connection // core connection
	Mu    sync.RWMutex     // makes this concurrent safe, maintenance wise only!
}

// IsSet tests if the low level amp connection is set.
func (c *SafeBaseConn) IsSet() bool {
	c.Mu.RLock()
	defer c.Mu.RUnlock()

	return c.Super != nil
}

func (c *SafeBaseConn) set(super *amqp.Connection) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.Super = super
}

func (c *SafeBaseConn) reset() {
	c.set(nil)
}

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

// IsBlocked returns the TCP flow status of the base connection.
func (conn *Connection) IsBlocked() bool {
	conn.blocked.mu.RLock()
	defer conn.blocked.mu.RUnlock()

	return conn.blocked.value
}

// IsClosed safely wraps the amqp connection IsClosed
func (conn *Connection) IsClosed() bool {
	conn.baseConn.Mu.RLock()
	defer conn.baseConn.Mu.RUnlock()

	return conn.baseConn.Super == nil || conn.baseConn.Super.IsClosed()
}

// Close safely wraps the amqp connection Close and terminates the maintenance loop.
func (conn *Connection) Close() error {
	conn.baseConn.Mu.RLock()
	defer conn.baseConn.Mu.RUnlock()

	var err error

	if conn.baseConn.Super != nil {
		err = conn.baseConn.Super.Close()
	}
	conn.cancelCtx()

	return err
}

// Connection returns the safe base connection and thus indirectly the low level library connection.
func (conn *Connection) Connection() *SafeBaseConn {
	return &conn.baseConn
}

// Channel safely wraps the amqp connection Channel() function.
// The lock is used internally so there is no need for the application layer
// to access the lock directly.
func (conn *Connection) Channel() (*amqp.Channel, error) {
	conn.baseConn.Mu.RLock()
	defer conn.baseConn.Mu.RUnlock()

	if conn.baseConn.Super != nil {
		return conn.baseConn.Super.Channel()
	}

	return nil, errors.New("connection not available")
}

// NewConnection creates a managed connection.
// Internally it derives a new WithCancel context from the passed (if any) context via
// [WithConnectionOptionContext]
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
	conn.baseConn.Mu.RLock()
	defer conn.baseConn.Mu.RUnlock()

	if conn.baseConn.Super != nil {
		evtClosed = conn.baseConn.Super.NotifyClose(make(chan *amqp.Error))
		evtBlocked = conn.baseConn.Super.NotifyBlocked(make(chan amqp.Blocking)) // TODO: is this persistent (similar to chan.Flow?)
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
			// connMarkAvailable(conn, false)
			if !connRecover(conn, config, err, notifierStatus) {
				return
			}
		}
	}
}

// connRecover attempts recovery. Returns false if wanting to shut-down this connection
// or not possible as indicated by engine via err,notifierStatus
func connRecover(conn *Connection, config amqp.Config, err *amqp.Error, notifierStatus bool) bool {
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
	return err != nil && connReconnectLoop(conn, config)
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
		event.Err = err
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
			// connMarkAvailable(conn, true)
			return true
		}
		// context cancelled
		if !delayerCompleted(conn.opt.ctx, conn.opt.delayer, retry) {
			return false
		}
	}
}
