package grabbit

import (
	"context"
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

// Cancel wraps safely the base consumer channel cancellation.
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
func (c *Consumer) Available() (bool, bool) {
	return !c.channel.conn.IsClosed(), !c.channel.IsClosed()
}

// AwaitAvailable waits till the consumer infrastructure is ready or timeout expires.
// Useful when the connections and channels are about being created or recovering.
// When passing zero value parameter the defaults used are 7500ms for timeout and
// 330 ms for polling frequency.
func (c *Consumer) AwaitAvailable(timeout, pollFreq time.Duration) bool {
	if timeout == 0 {
		timeout = 7500 * time.Millisecond
	}
	if pollFreq == 0 {
		pollFreq = 330 * time.Millisecond
	}

	d := time.Now().Add(timeout)
	ctxLocal, cancel := context.WithDeadline(c.channel.opt.ctx, d)
	defer cancel()

	// status polling
	ticker := time.NewTicker(pollFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ctxLocal.Done():
			return false
		case <-ticker.C:
			if connUp, chanUp := c.Available(); connUp && chanUp {
				return true
			}
		}
	}
}

// Close shuts down cleanly the publisher channel.
func (c *Consumer) Close() error {
	return c.channel.Close()
}
