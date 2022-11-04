package grabbit

import (
	"context"
	"errors"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection wraps the base amqp connection
type Connection struct {
	baseConn  *amqp.Connection   // core connection
	address   string             // where to connect
	blocked   SafeBool           // TCP stream status
	opt       ConnectionOptions  // user parameters
	cancelCtx context.CancelFunc // bolted on opt.ctx; aborts the reconnect loop
}

// IsBlocked returns the TCP flow status of the base connection
func (conn *Connection) IsBlocked() bool {
	conn.blocked.mu.RLock()
	defer conn.blocked.mu.RUnlock()

	return conn.blocked.Value
}

// IsClosed wraps the base connection IsClosed
func (conn *Connection) IsClosed() bool {
	return conn.baseConn == nil || conn.baseConn.IsClosed()
}

// Close wraps the base connection Close
func (conn *Connection) Close() error {
	var err error

	if conn.baseConn != nil {
		err = conn.baseConn.Close()
	}
	conn.cancelCtx()

	return err
}

// Connection returns the low level library connection for direct access if so
// desired. WARN: the result may be nil and needs testing before using
func (conn *Connection) Connection() *amqp.Connection {
	return conn.baseConn
}

// Channel wraps the base connection Channel
func (conn *Connection) Channel() (*amqp.Channel, error) {
	if conn.baseConn != nil {
		return conn.baseConn.Channel()
	}

	return nil, errors.New("connection not available")
}

// NewConnection creates a managed connection
func NewConnection(address string, config amqp.Config, optionFuncs ...func(*ConnectionOptions)) *Connection {
	opt := &ConnectionOptions{
		notifier: make(chan Event, 5),
		name:     "default",
		delayer:  DefaultDelayer{Value: 7500 * time.Millisecond},
		ctx:      context.Background(),
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(opt)
	}

	conn := &Connection{
		address: address,
		opt:     *opt,
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
	RaiseEvent(conn.opt.notifier, event)

	conn.blocked.Value = value
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

func connDial(conn *Connection, config amqp.Config) bool {
	event := Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventUp,
	}
	connected := true

	conn.connRefreshCredentials()

	conn.baseConn, event.Err = amqp.DialConfig(conn.address, config)
	if event.Err != nil {
		event.Kind = EventCannotEstablish
		connected = false
	}

	// async
	RaiseEvent(conn.opt.notifier, event)
	// sync
	if connected && conn.opt.cbUp != nil {
		conn.opt.cbUp(conn.opt.name)
	}

	return connected
}

func connNotifyClose(conn *Connection) chan *amqp.Error {
	if conn.baseConn == nil {
		return nil
	} else {
		return conn.baseConn.NotifyClose(make(chan *amqp.Error))
	}
}

func connNotifyBlocked(conn *Connection) chan amqp.Blocking {
	if conn.baseConn == nil {
		return nil
	} else {
		return conn.baseConn.NotifyBlocked(make(chan amqp.Blocking))
	}
}

func connManager(conn *Connection, config amqp.Config) {
	for {
		select {
		case <-conn.opt.ctx.Done():
			return
		case blk := <-connNotifyBlocked(conn):
			connMarkBlocked(conn, blk.Active)
		case err, notifierStatus := <-connNotifyClose(conn):
			// async
			RaiseEvent(conn.opt.notifier, Event{
				SourceType: CliConnection,
				SourceName: conn.opt.name,
				Kind:       EventDown,
				Err:        err,
			})
			// sync
			if conn.opt.cbDown != nil && !conn.opt.cbDown(conn.opt.name, err) {
				return
			}

			if !notifierStatus {
				conn.baseConn = nil
				RaiseEvent(conn.opt.notifier, Event{
					SourceType: CliConnection,
					SourceName: conn.opt.name,
					Kind:       EventClosed,
				})
			}

			// no err means gracefully closed on demand
			TODO not happy with this bool logic. Review required !!!
			if !(err == nil || connReconnectLoop(conn, config)) {
				return
			}
		}
	}
}

func connReconnectLoop(conn *Connection, config amqp.Config) bool {
	retry := 0

	for {
		retry = (retry + 1) % 0xFFFF

		// sync
		if conn.opt.cbReconnect != nil && !conn.opt.cbReconnect(conn.opt.name, retry) {
			return false
		}

		select {
		case <-conn.opt.ctx.Done():
			return false
		case <-time.After(conn.opt.delayer.Delay(retry)):
		}

		if connDial(conn, config) {
			return true
		}
	}
}
