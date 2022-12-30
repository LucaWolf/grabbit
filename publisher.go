package grabbit

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	channel *Channel         // assigned channel
	opt     PublisherOptions // specific options
}

func DefaultNotifyPublish(confirm amqp.Confirmation, ch *Channel) {
	if !confirm.Ack {
		event := Event{
			SourceType: CliPublisher,
			SourceName: ch.opt.name,
			Kind:       EventMessagePublished,
			Err:        fmt.Errorf("delivery tag %d failed and lost", confirm.DeliveryTag),
		}
		raiseEvent(ch.opt.notifier, event)
	}
}

func DefaultNotifyReturn(msg amqp.Return, ch *Channel) {
	event := Event{
		SourceType: CliPublisher,
		SourceName: ch.opt.name,
		Kind:       EventMessageReturned,
		Err:        fmt.Errorf("message %s returned and lost", msg.MessageId),
	}
	raiseEvent(ch.opt.notifier, event)
}

// NewPublisher creates a publisher with the desired options.
// It opens a new dedicated channel using the passed shared connection.
func NewPublisher(conn *Connection, opt PublisherOptions, optionFuncs ...func(*ChannelOptions)) *Publisher {
	implParams := ChanUsageParameters{
		IsPublisher:        true,
		ConfirmationNoWait: opt.ConfirmationNoWait,
		ConfirmationCount:  opt.ConfirmationCount,
	}
	chanOpt := append(optionFuncs, WithChannelOptionUsageParams(implParams))

	return &Publisher{
		channel: NewChannel(conn, chanOpt...),
		opt:     opt,
	}
}

// PublishWithOptions wraps the PublishWithContext using internal PublisherOptions
func (p *Publisher) Publish(msg amqp.Publishing) error {

	if p.channel.IsClosed() {
		return errors.New("publisher channel is not yet available. Try again later")
	}

	// TODO this feels somehow unsafe even though we tested for IsClosed.
	// Super channel may be down and getting refreshed in this very moment !!!
	return p.channel.baseChan.Super.PublishWithContext(
		p.opt.Context, p.opt.Exchange, p.opt.Key, p.opt.Mandatory, p.opt.Immediate,
		msg)
}

// PublishWithOptions wraps the PublishWithContext using external PublisherOptions
func (p *Publisher) PublishWithOptions(opt PublisherOptions, msg amqp.Publishing) error {

	if !p.channel.IsClosed() {
		return errors.New("publisher channel is not yet available. Try again later")
	}

	// TODO this feels somehow unsafe even though we tested for IsClosed.
	// Super channel may be down and getting refreshed in this very moment !!!
	return p.channel.baseChan.Super.PublishWithContext(
		opt.Context, opt.Exchange, opt.Key, opt.Mandatory, opt.Immediate,
		msg)
}

// Available returns the status of both the underlying connection and channel
func (p *Publisher) Available() (bool, bool) {
	return !p.channel.conn.IsClosed(), !p.channel.IsClosed()
}

// AwaitAvailable waits till the publisher infrastructure is ready. Useful when the connections and channels
// are about being created or recovering.
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

// Close shuts down cleanly the publisher channel
func (p *Publisher) Close() error {
	return p.channel.Close()
}
