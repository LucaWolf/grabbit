package grabbit

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	channel *Channel // assigned channel
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
	{
		event := Event{
			SourceType: CliPublisher,
			SourceName: ch.opt.name,
			Kind:       EventMessageReturned,
			Err:        fmt.Errorf("message %s returned and lost", msg.MessageId),
		}
		raiseEvent(ch.opt.notifier, event)
	}
}

// NewPublisher creates a publisher with the desired options.
// It opens a new dedicated channel using the passed shared connection.
func NewPublisher(conn *Connection, params ImplementationParameters, optionFuncs ...func(*ChannelOptions)) *Publisher {

	// forcing this for Channel.Confirm
	opt := append(optionFuncs, WithChannelOptionPublisherParams(params))

	return &Publisher{
		channel: NewChannel(conn, opt...),
	}
}

func (p *Publisher) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if p.channel.IsClosed() {
		return errors.New("publisher channel is not yet available. Try again later")
	}
	return p.channel.baseChan.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (p *Publisher) Close() error {
	return p.channel.Close()
}
