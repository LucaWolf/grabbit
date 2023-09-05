package grabbit

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConfirmationOutcome int

//go:generate stringer -type=ConfirmationOutcome  -linecomment
const (
	ConfirmationTimeOut  ConfirmationOutcome = iota // no timely response
	ConfirmationClosed                              // data confirmation channel is closed
	ConfirmationDisabled                            // base channel has not been put into confirm mode
	ConfirmationPrevious                            // lower sequence number than expected
	ConfirmationACK                                 // ACK (publish confirmed)
	ConfirmationNAK                                 // NAK (publish negative acknowledgement)
)

// DeferredConfirmation wraps [amqp.DeferredConfirmation] with additional data.
// It inherits (by embedding) all original fields and functonality from the amqp object.
type DeferredConfirmation struct {
	*amqp.DeferredConfirmation                     // wrapped low level confirmation
	Outcome                    ConfirmationOutcome // acknowledgment received stats
	RequestSequence            uint64              // sequence of the original request (GetNextPublishSeqNo)
	ChannelName                string              // channel name of the publisher
	Queue                      string              // queue name of the publisher
}

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

func (p *Publisher) AwaitDeferredConfirmation(d *DeferredConfirmation, tmr time.Duration) *DeferredConfirmation {
	if d.DeferredConfirmation == nil {
		d.Outcome = ConfirmationDisabled
		return d
	}

	select {
	case <-time.After(tmr):
		d.Outcome = ConfirmationTimeOut
	case <-p.opt.Context.Done():
		d.Outcome = ConfirmationClosed
	// FIXME: could this be triggered by recovery before the channel's context?
	case <-d.Done():
		if d.RequestSequence > d.DeliveryTag {
			d.Outcome = ConfirmationPrevious
		} else if d.Acked() {
			d.Outcome = ConfirmationACK
		} else {
			d.Outcome = ConfirmationNAK
		}
	}

	return d
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

// PublishDeferredConfirm wraps the amqp.PublishWithDeferredConfirmWithContext using the internal [PublisherOptions]
// cached when the publisher was created.
func (p *Publisher) PublishDeferredConfirm(msg amqp.Publishing) (*DeferredConfirmation, error) {
	if p.channel.IsClosed() {
		return nil, amqp.ErrClosed
	}

	var err error
	confirmation := &DeferredConfirmation{
		Outcome:         ConfirmationClosed,
		ChannelName:     p.channel.Name(),
		Queue:           p.channel.Queue(),
		RequestSequence: p.channel.GetNextPublishSeqNo(),
	}
	confirmation.DeferredConfirmation, err = p.channel.PublishWithDeferredConfirmWithContext(
		p.opt.Context, p.opt.Exchange, p.opt.Key, p.opt.Mandatory, p.opt.Immediate, msg)

	return confirmation, err
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

// PublishDeferredConfirmWithOptions wraps the amqp.PublishWithDeferredConfirmWithContext using the passed options.
func (p *Publisher) PublishDeferredConfirmWithOptions(opt PublisherOptions, msg amqp.Publishing) (*DeferredConfirmation, error) {
	if p.channel.IsClosed() {
		return nil, amqp.ErrClosed
	}

	var err error
	confirmation := &DeferredConfirmation{
		Outcome:         ConfirmationClosed,
		ChannelName:     p.channel.Name(),
		Queue:           p.channel.Queue(),
		RequestSequence: p.channel.GetNextPublishSeqNo(),
	}
	confirmation.DeferredConfirmation, err = p.channel.PublishWithDeferredConfirmWithContext(
		opt.Context, opt.Exchange, opt.Key, opt.Mandatory, opt.Immediate, msg)

	return confirmation, err
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
