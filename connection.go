package grabbit

import (
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SecretProvider interface {
	Password() (string, error)
}

type DelayProvider interface {
	Delay(retry int) time.Duration
}

type ConnectionOptions struct {
	notifier    chan Event     // feedback channel
	name        string         // tag for this connection
	credentials SecretProvider // value for UpdateSecret()
	delayer     DelayProvider  // how much to wait between re-attempts
}

type Connection struct {
	conn    *amqp.Connection  // core connection
	address string            // where to connect
	ready   SafeBool          // is connection usable/available
	options ConnectionOptions // user parameters
}

type DefaultDelayer struct {
	Value time.Duration
}

func (delayer DefaultDelayer) Delay(retry int) time.Duration {
	return delayer.Value
}

func WithConnectionOptionPassword(credentials SecretProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.credentials = credentials
	}
}

func WithConnectionOptionDelay(delayer DelayProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.delayer = delayer
	}
}

func WithConnectionOptionName(name string) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.name = name
	}
}

func WithConnectionOptionNotification(ch chan Event) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.notifier = ch
	}
}

func (c *Connection) RefreshCredentials() {
	if c.options.credentials != nil {
		if secret, err := c.options.credentials.Password(); err == nil {
			if u, err := url.Parse(c.address); err == nil {
				u.User = url.UserPassword(u.User.Username(), secret)
				c.address = u.String()
			}
		}
	}
}

func (c *Connection) SetReady(value bool) {
	c.ready.mu.Lock()
	defer c.ready.mu.Unlock()

	c.ready.Value = value
}

func (c *Connection) Ready() bool {
	c.ready.mu.RLock()
	defer c.ready.mu.RUnlock()

	return c.ready.Value
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func NewConnection(address string, config amqp.Config, optionFuncs ...func(*ConnectionOptions)) (*Connection, error) {

	options := &ConnectionOptions{
		notifier: make(chan Event, 5),
		name:     "default",
		delayer:  DefaultDelayer{Value: 7500 * time.Second},
	}

	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	conn := &Connection{
		address: address,
		ready:   SafeBool{},
		options: *options,
	}

	conn.RefreshCredentials()

	link, err := amqp.DialConfig(conn.address, config)
	if err != nil {
		return nil, err
	}

	conn.conn = link
	RaiseEvent(conn.options.notifier, Event{
		SourceType: CliConnection,
		SourceName: conn.options.name,
		Kind:       EventUp,
	})

	// start the close notifications monitoring and recovery loop
	go func() {
		for {
			retry := 0
			// TODO also monitor the block notifications
			err, ok := <-conn.conn.NotifyClose(make(chan *amqp.Error))

			conn.SetReady(false)
			RaiseEvent(conn.options.notifier, Event{
				SourceType: CliConnection,
				SourceName: conn.options.name,
				Kind:       EventDown,
				Err:        err,
			})

			if !ok {
				RaiseEvent(conn.options.notifier, Event{
					SourceType: CliConnection,
					SourceName: conn.options.name,
					Kind:       EventClosed,
				})
			}

			for {
				retry = (retry + 1) % 0xFF
				<-time.After(conn.options.delayer.Delay(retry))

				conn.RefreshCredentials()

				if link, err := amqp.DialConfig(conn.address, config); err != nil {
					RaiseEvent(conn.options.notifier, Event{
						SourceType: CliConnection,
						SourceName: conn.options.name,
						Kind:       EventCannotRecoverYet,
						Err:        err,
					})
				} else {
					conn.conn = link
					conn.SetReady(true)
					RaiseEvent(conn.options.notifier, Event{
						SourceType: CliConnection,
						SourceName: conn.options.name,
						Kind:       EventUp,
					})
					break
				}
			}
		}
	}()

	return conn, nil
}
