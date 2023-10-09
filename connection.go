package grabbit

import (
	"context"
	"errors"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection wraps a [SafeBaseConn] with additional attributes
// (impl. details: rabbit URL, [ConnectionOptions] and a cancelling context).
// Applications should obtain a connection using [NewConnection].
type Connection struct {
	baseConn SafeBaseConn      // supporting amqp connection
	address  string            // where to connect
	blocked  SafeBool          // TCP stream status
	opt      ConnectionOptions // user parameters
}

// NewConnection creates a new managed Connection object with the given address, configuration, and option functions.
//
// Example Usage:
//
//	  conn := NewConnection("amqp://guest:guest@localhost:5672/", amqp.Config{},
//		  WithConnectionOptionContext(context.Background(),
//		  WithConnectionOptionName("default"),
//		  WithConnectionOptionDown(Down),
//		  WithConnectionOptionUp(Up),
//		  WithConnectionOptionRecovering(Reattempting),
//		  WithConnectionOptionNotification(connStatusChan),
//	  )
//
// Parameters:
//   - address: the address of the connection.
//   - config: the AMQP configuration.
//   - optionFuncs: variadic option functions to customize the connection options.
//
// Returns: a new Connection object.
func NewConnection(address string, config amqp.Config, optionFuncs ...func(*ConnectionOptions)) *Connection {
	opt := ConnectionOptions{
		notifier: make(chan Event),
		name:     "default",
		delayer:  DefaultDelayer{Value: 7500 * time.Millisecond},
		ctx:      context.Background(),
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(&opt)
	}

	conn := &Connection{
		baseConn: SafeBaseConn{},
		address:  address,
		opt:      opt,
	}

	conn.opt.ctx, conn.opt.cancelCtx = context.WithCancel(opt.ctx)

	go func() {
		if !conn.reconnectLoop(config) {
			return
		}
		conn.manager(config)
	}()

	return conn
}

// setFlow updates the flow control status of the Connection.
//
// It takes a value of type amqp.Blocking as a parameter and updates the
// blocked status of the Connection accordingly. If the value is active, the
// Connection is considered blocked and an EventBlocked event is raised. If
// the value is inactive, the Connection is considered unblocked and an
// EventUnBlocked event is raised.
//
// The function also sets the SourceType to CliConnection, the SourceName to
// the name of the Connection, the Kind to the appropriate EventType based on
// the value, and the Err to a SomeErrFromString value created from the reason
// provided in the value parameter.
//
// The function does not return anything.
func (conn *Connection) setFlow(value amqp.Blocking) {
	conn.blocked.mu.Lock()
	conn.blocked.value = value.Active
	conn.blocked.mu.Unlock()

	var kind EventType
	if value.Active {
		kind = EventBlocked
	} else {
		kind = EventUnBlocked
	}

	event := Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       kind,
		Err:        SomeErrFromString(value.Reason),
	}
	raiseEvent(conn.opt.notifier, event)
}

// refreshCredentials refreshes the credentials of the Connection.
// If the credentials field of the Connection object is not nil it retrieves the password from the credentials object and
// updates the address field of the Connection object with the username included in the URL and the password.
//
// No parameters.
// No return type.
func (conn *Connection) refreshCredentials() {
	if conn.opt.credentials != nil {
		if secret, err := conn.opt.credentials.Password(); err == nil {
			if u, err := url.Parse(conn.address); err == nil {
				u.User = url.UserPassword(u.User.Username(), secret)
				conn.address = u.String()
			}
		}
	}
}

// notificationChannels returns the notification channels for the Connection.
//
// It acquires a lock on the baseConn mutex and releases it when done. If the baseConn.super is not nil,
// it creates and returns two channels: evtClosed for notifying on connection close, and evtBlocked for notifying on connection blockage.
// If the baseConn.super is nil, it returns nil for both channels and an error indicating that the connection is not yet available.
//
// Returns:
//   - chan *amqp.Error: A channel for notifying on connection close.
//   - chan amqp.Blocking: A channel for notifying on connection blockage.
//   - error: An error indicating that the connection is not yet available.
func (conn *Connection) notificationChannels() (chan *amqp.Error, chan amqp.Blocking, error) {
	conn.baseConn.mu.Lock()
	defer conn.baseConn.mu.Unlock()

	if conn.baseConn.super != nil {
		evtClosed := conn.baseConn.super.NotifyClose(make(chan *amqp.Error))
		evtBlocked := conn.baseConn.super.NotifyBlocked(make(chan amqp.Blocking)) // TODO: is this persistent (similar to chan.Flow?)
		return evtClosed, evtBlocked, nil
	}

	return nil, nil, errors.New("connection not yet available")
}

// manager is a function that manages the connection state.
//
// It takes a config parameter of type amqp.Config.
// This function does not return anything.
func (conn *Connection) manager(config amqp.Config) {
	for {
		evtClosed, evtBlocked, err := conn.notificationChannels()
		if err != nil {
			// FIXME adopt a circuit breaker policy
			time.Sleep(conn.opt.delayer.Delay(3))
			continue
		}

		select {
		case <-conn.opt.ctx.Done():
			conn.Close() // cancelCtx() called again but idempotent
			return
		case status := <-evtBlocked:
			conn.setFlow(status)
		case err, notifierStatus := <-evtClosed:
			if !conn.recover(config, SomeErrFromError(err, err != nil), notifierStatus) {
				return
			}
		}
	}
}

// recover recovers the connection with the specified configuration after an error occurs.
//
// It raises an event to notify the notifier about the connection going down and checks if the
// callback is allowed to handle the connection going down. If the notifier status is false, it
// resets the base connection and raises an event to notify the notifier about the connection
// being closed. Finally, it checks if the error is set and starts the reconnection loop.
//
// Parameters:
//   - config: the AMQP configuration to reconnect with.
//   - err: an optional error that occurred during the connection.
//   - notifierStatus: a boolean indicating whether the notifier is active.
//
// Returns:
//   - a boolean indicating if the recovery was successful.
func (conn *Connection) recover(config amqp.Config, err OptionalError, notifierStatus bool) bool {
	raiseEvent(conn.opt.notifier, Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventDown,
		Err:        err,
	})
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
	return err.IsSet() && conn.reconnectLoop(config)
}

// dial connects to the AMQP server using the given configuration.
//
// The function takes a `config` parameter of type `amqp.Config` which
// specifies the configuration options for the connection.
//
// It returns a boolean value indicating whether the connection was
// successfully established.
func (conn *Connection) dial(config amqp.Config) bool {
	event := Event{
		SourceType: CliConnection,
		SourceName: conn.opt.name,
		Kind:       EventUp,
	}
	result := true

	conn.refreshCredentials()

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

// reconnectLoop is a function that handles the reconnection process for the Connection struct.
//
// It takes in a config parameter of type amqp.Config.
// It returns a boolean value indicating whether the reconnection was successful or not.
// If a callback function callbackAllowedRecovery returns false or a delayer function delayerCompleted returns false, it returns false.
func (conn *Connection) reconnectLoop(config amqp.Config) bool {
	retry := 0
	for {
		retry = (retry + 1) % 0xFFFF
		if !callbackAllowedRecovery(conn.opt.cbReconnect, conn.opt.name, retry) {
			return false
		}

		if conn.dial(config) {
			return true
		}
		if !delayerCompleted(conn.opt.ctx, conn.opt.delayer, retry) {
			return false
		}
	}
}
