package grabbit

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher implements an object allowing calling applications
// to publish messages on already established connections.
// Create a publisher instance by calling [NewPublisher].
type Publisher struct {
	channel *Channel         // assigned channel
	opt     PublisherOptions // specific options
}

// defaultNotifyPublish provides a base implementation of [CallbackNotifyPublish] which can be
// overwritten with [WithChannelOptionNotifyPublish]. If confirm.Ack is false
// it sends an [EventMessagePublished] kind of event over the notification channel
// (see [WithChannelOptionNotification]) with a literal error containing the delivery tag.
func defaultNotifyPublish(confirm amqp.Confirmation, ch *Channel) {
	if !confirm.Ack {
		event := Event{
			SourceType: CliPublisher,
			SourceName: ch.opt.name,
			Kind:       EventMessagePublished,
			Err: SomeErrFromString(
				fmt.Sprintf("delivery tag %d unconfirmed", confirm.DeliveryTag),
			),
		}
		raiseEvent(ch.opt.notifier, event)
	}
}

// defaultNotifyReturn provides a base implementation of [CallbackNotifyReturn] which can be
// overwritten with [WithChannelOptionNotifyReturn].
// It sends an [EventMessageReturned] kind of event over the notification channel
// (see [WithChannelOptionNotification]) with a literal error containing the return message ID.
func defaultNotifyReturn(msg amqp.Return, ch *Channel) {
	event := Event{
		SourceType: CliPublisher,
		SourceName: ch.opt.name,
		Kind:       EventMessageReturned,
		Err: SomeErrFromString(
			fmt.Sprintf("message %s returned", msg.MessageId),
		),
	}
	raiseEvent(ch.opt.notifier, event)
}

// Channel returns the managed [Channel] which can be further used to extract [SafeBaseChan]
func (p *Publisher) Channel() *Channel {
	return p.channel
}

// NewPublisher creates a publisher with the desired options.
// It creates and opens a new dedicated [Channel] using the passed shared connection.
func NewPublisher(conn *Connection, opt PublisherOptions, optionFuncs ...func(*ChannelOptions)) *Publisher {
	useParams := ChanUsageParameters{
		PublisherUsageOptions: opt.PublisherUsageOptions,
	}
	chanOpt := append(optionFuncs, WithChannelOptionUsageParams(useParams))

	return &Publisher{
		channel: NewChannel(conn, chanOpt...),
		opt:     opt,
	}
}

// Publish wraps the amqp.PublishWithContext using the internal [PublisherOptions]
// cached when the publisher was created.
func (p *Publisher) Publish(msg amqp.Publishing) error {

	if p.channel.IsClosed() {
		return amqp.ErrClosed
	}

	return p.channel.PublishWithContext(
		p.opt.Context, p.opt.Exchange, p.opt.Key, p.opt.Mandatory, p.opt.Immediate,
		msg)
}

// PublishWithOptions wraps the amqp.PublishWithContext using the passed options.
func (p *Publisher) PublishWithOptions(opt PublisherOptions, msg amqp.Publishing) error {

	if p.channel.IsClosed() {
		return amqp.ErrClosed
	}

	return p.channel.PublishWithContext(
		opt.Context, opt.Exchange, opt.Key, opt.Mandatory, opt.Immediate,
		msg)
}

// Available returns the status of both the underlying connection and channel.
func (p *Publisher) Available() (bool, bool) {
	return !p.channel.conn.IsClosed(), !p.channel.IsClosed()
}

// AwaitAvailable waits till the publisher infrastructure is ready or timeout expires.
// Useful when the connections and channels are about being created or recovering.
// When passing zero value parameter the defaults used are 7500ms for timeout and
// 330 ms for polling frequency.
func (p *Publisher) AwaitAvailable(timeout, pollFreq time.Duration) bool {
	if timeout == 0 {
		timeout = 7500 * time.Millisecond
	}
	if pollFreq == 0 {
		pollFreq = 330 * time.Millisecond
	}

	// status polling
	ticker := time.NewTicker(pollFreq)
	defer ticker.Stop()
	done := make(chan bool)

	// session timeout
	go func() {
		time.Sleep(timeout)
		done <- true
	}()

	for {
		select {
		case <-done:
			return false
		case <-ticker.C:
			if connUp, chanUp := p.Available(); connUp && chanUp {
				return true
			}
		}
	}
}

// Close shuts down cleanly the publisher channel.
func (p *Publisher) Close() error {
	return p.channel.Close()
}
