package grabbit

import (
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	conn    *amqp.Connection  // core connection
	address string            // where to connect
	ready   SafeBool          // is connection usable/available
	options ConnectionOptions // user parameters
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
		delayer:  DefaultDelayer{Value: 7500 * time.Millisecond},
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

	// start the close notifications monitoring and recovery manager
	go func() {
		for {
			retry := 0
			// TODO also monitor the block notifications
			err, chOpen := <-conn.conn.NotifyClose(make(chan *amqp.Error))

			conn.SetReady(false)
			RaiseEvent(conn.options.notifier, Event{
				SourceType: CliConnection,
				SourceName: conn.options.name,
				Kind:       EventDown,
				Err:        err,
			})

			if !chOpen {
				RaiseEvent(conn.options.notifier, Event{
					SourceType: CliConnection,
					SourceName: conn.options.name,
					Kind:       EventClosed,
				})

				// graceful close by upper layer, no need to reconnect
				if err == nil {
					break
				}
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
