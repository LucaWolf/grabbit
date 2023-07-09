package grabbit

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// IsPaused returns a publisher's flow status of the base channel.
func (ch *Channel) IsPaused() bool {
	ch.paused.mu.RLock()
	defer ch.paused.mu.RUnlock()

	return ch.paused.value
}

// IsClosed safely wraps the base channel IsClosed.
func (ch *Channel) IsClosed() bool {
	ch.baseChan.mu.RLock()
	defer ch.baseChan.mu.RUnlock()

	return ch.baseChan.super == nil || ch.baseChan.super.IsClosed()
}

// Close wraps the base channel Close.
func (ch *Channel) Close() error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	var err error

	if ch.baseChan.super != nil {
		// TODO It's advisable to wait for all Confirmations to arrive before
		// calling Channel.Close() or Connection.Close().
		err = ch.baseChan.super.Close()
	}
	ch.cancelCtx()

	return err
}

// Cancel wraps safely the base channel cancellation.
func (ch *Channel) Cancel(consumer string, noWait bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.Cancel(consumer, noWait)
	}
	return amqp.ErrClosed
}

// Reject safely wraps the base channel Ack.
func (ch *Channel) Reject(tag uint64, requeue bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.Reject(tag, requeue)
	}
	return amqp.ErrClosed
}

// Ack safely wraps the base channel Ack.
func (ch *Channel) Ack(tag uint64, multiple bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.Ack(tag, multiple)
	}
	return amqp.ErrClosed
}

// Ack safely wraps the base channel Nak.
func (ch *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.Nack(tag, multiple, requeue)
	}
	return amqp.ErrClosed
}

// QueueInspect safely wraps the base channel QueueInspect.
//
// Deprecated: use QueueDeclarePassive
func (ch *Channel) QueueInspect(name string) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.QueueInspect(name)
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// QueueDeclarePassive safely wraps the base channel QueueInspect.
func (ch *Channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// PublishWithContext safely wraps the base channel PublishWithContext.
func (ch *Channel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	}
	return amqp.ErrClosed
}

// PublishWithDeferredConfirmWithContext safely wraps the base channel PublishWithDeferredConfirmWithContext.
func (ch *Channel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	}
	return nil, amqp.ErrClosed
}

// QueuePurge safely wraps the base channel QueuePurge.
func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.QueuePurge(name, noWait)
	}
	return 0, amqp.ErrClosed
}

// GetNextPublishSeqNo safely wraps the base channel GetNextPublishSeqNo
func (ch *Channel) GetNextPublishSeqNo() uint64 {
	ch.paused.mu.RLock()
	defer ch.paused.mu.RUnlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.GetNextPublishSeqNo()
	}
	return 0
}

// QueueDelete safely wraps the base channel QueueDelete.
func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.QueueDelete(name, ifUnused, ifEmpty, noWait)
	}
	return 0, amqp.ErrClosed
}

// QueueDeclare safely wraps the base channel QueueDeclare.
// Prefer using the [QueueDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// ExchangeDelete safely wraps the base channel ExchangeDelete.
func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.ExchangeDelete(name, ifUnused, noWait)
	}
	return amqp.ErrClosed
}

// ExchangeDeclare safely wraps the base channel ExchangeDeclare
// Prefer using the [ExchangeDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return ch.baseChan.super.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
	}
	return amqp.ErrClosed
}

// QueueDeclareWithTopology safely declares a desired queue as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) QueueDeclareWithTopology(t *TopologyOptions) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return declareQueue(ch.baseChan.super, t)
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// ExchangeDeclareWithTopology safely declares a desired exchange as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) ExchangeDeclareWithTopology(t *TopologyOptions) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return declareExchange(ch.baseChan.super, t)
	}
	return amqp.ErrClosed
}

// Queue returns the active (as indicated by [IsDestination] option in topology options) queue name.
// Useful for finding the server assigned name.
func (ch *Channel) Queue() string {
	return ch.queue
}

// Name returns the tag defined originally when creating this channel
func (ch *Channel) Name() string {
	return ch.opt.name
}

// Channel returns the low level library channel for further direct access to its Super() low level channel.
// Use sparingly and prefer using the predefined [Channel] wrapping methods instead.
// Pair usage with the provided full [Lock][UnLock] or read [RLock][RUnlock]
// locking/unlocking mechanisms for safety!
func (ch *Channel) Channel() *SafeBaseChan {
	return &ch.baseChan
}
