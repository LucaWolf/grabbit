package grabbit

import (
	"time"
)

// defaultPayloadProcessor processes the payload using default logic.
//
// It takes the following parameters:
//   - props: a pointer to DeliveriesProperties struct
//   - messages: a slice of DeliveryData structs
//   - mustAck: a boolean indicating whether the messages must be acknowledged
//   - ch: a pointer to Channel struct
//
// It does not return any value.
func defaultPayloadProcessor(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
	Event{
		SourceType: CliChannel,
		SourceName: props.ConsumerTag,
		Kind:       EventMessageReceived,
		Err:        SomeErrFromString("default processor discards messages"),
	}.raise(ch.opt.notifier)

	if mustAck && len(messages) != 0 {
		ch.Ack(messages[len(messages)-1].DeliveryTag, true)
	}
}

// Consumer implements an object allowing calling applications
// to receive messages on already established connections.
// Create a consumer instance by calling [NewConsumer].
type Consumer struct {
	channel *Channel        // assigned channel
	opt     ConsumerOptions // specific options
}

// Channel returns the managed [Channel] which can be further used to extract [SafeBaseChan]
func (p *Consumer) Channel() *Channel {
	return p.channel
}

// Cancel wraps safely the base consumer channel cancellation. It enforces
// a false value for `noWait` parameter to the amqp cancellation.
func (p *Consumer) Cancel() error {
	// false indicates future intention (i.e. process already retrieved)
	return p.channel.Cancel(p.opt.ConsumerName, false)
}

// NewConsumer creates a consumer with the desired options and then starts consuming.
// It creates and opens a new dedicated [Channel] using the passed shared connection.
// NOTE: It's advisable to use separate connections for Channel.Publish and Channel.Consume
func NewConsumer(conn *Connection, opt ConsumerOptions, optionFuncs ...func(*ChannelOptions)) *Consumer {
	useParams := ChanUsageParameters{
		ConsumerUsageOptions: opt.ConsumerUsageOptions,
	}
	chanOpt := append(optionFuncs, WithChannelOptionUsageParams(useParams))

	return &Consumer{
		channel: NewChannel(conn, chanOpt...),
		opt:     opt,
	}
}

// Available returns the status of both the underlying connection and channel.
// (prefer using AwaitStatus method)
func (c *Consumer) Available() (bool, bool) {
	return !c.channel.conn.IsClosed(), !c.channel.IsClosed()
}

// AwaitAvailable waits till the consumers's infrastructure is ready or timeout expires.
// It delegates operation to the  supporting [Channel].
// (pollFreq is now obsolete)
//
// Deprecated: replaced by AwaitStatus.
func (c *Consumer) AwaitAvailable(timeout time.Duration, pollFreq time.Duration) bool {
	return c.channel.AwaitStatus(true, timeout)
}

// AwaitStatus waits till the channel is in the desired state or timeout expires.
// It delegates operation to the supporting [Channel].
// Pass 'true' for testing open/active, 'false' for testing if closed.
func (c *Consumer) AwaitStatus(established bool, timeout time.Duration) bool {
	return c.channel.AwaitStatus(established, timeout)
}

// AwaitManager waits till the managing goroutine is in the desired state or timeout expires.
// Provided as a helper tool to confirm the lifespan of the Connection has expired
// (no goroutine leak) when closed and not usually called from the user application layer.
// It delegates operation to the supporting [Channel].
func (c *Consumer) AwaitManager(active bool, timeout time.Duration) bool {
	return c.channel.AwaitManager(active, timeout)
}

// Close shuts down cleanly the consumer channel. If there are other consumers of the same queue,
// it is advisable to call the `Cancel` method of this consumer beforehand,
// to let the server know it needs redistributing the messages.
func (c *Consumer) Close() error {
	return c.channel.Close()
}
