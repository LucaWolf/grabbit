package grabbit

import (
	"context"

	trace "traceutils"

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

// Close safely wraps the amqp channel Close and terminates the maintenance loop.
// The inner base channel is reset and the context is cancelled. Operation is idempotent
// to mirror the base amqp library contract.
func (ch *Channel) Close() error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	var err error

	if ch.baseChan.super != nil {
		// TODO It's advisable to wait for all Confirmations to arrive before
		// calling Channel.Close() or Connection.Close().
		err = ch.baseChan.super.Close()
		ch.baseChan.super = nil
	}
	ch.opt.cancelCtx()

	return err
}

// Cancel wraps safely the base channel cancellation.
// Unlike Close, Cancel is not idempotent.
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
		result, err := ch.baseChan.super.QueueInspect(name)
		trace.QueueInspect(ch.opt.ctx, name)
		return result, err
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// QueueDeclarePassive safely wraps the base channel QueueInspect.
func (ch *Channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		resuult, err := ch.baseChan.super.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
		trace.QueueDeclarePassive(ch.opt.ctx, name, durable, autoDelete, exclusive, noWait, args)
		return resuult, err
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// PublishWithContext safely wraps the base channel PublishWithContext.
func (ch *Channel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result := ch.baseChan.super.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
		trace.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
		return result
	}
	return amqp.ErrClosed
}

// PublishWithDeferredConfirmWithContext safely wraps the base channel PublishWithDeferredConfirmWithContext.
func (ch *Channel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result, err := ch.baseChan.super.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
		trace.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
		return result, err
	}
	return nil, amqp.ErrClosed
}

// QueuePurge safely wraps the base channel QueuePurge.
func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result, err := ch.baseChan.super.QueuePurge(name, noWait)
		trace.QueuePurge(ch.opt.ctx, name, noWait)
		return result, err
	}
	return 0, amqp.ErrClosed
}

// GetNextPublishSeqNo safely wraps the base channel GetNextPublishSeqNo
func (ch *Channel) GetNextPublishSeqNo() uint64 {
	ch.baseChan.mu.RLock()
	defer ch.baseChan.mu.RUnlock()

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
		result, err := ch.baseChan.super.QueueDelete(name, ifUnused, ifEmpty, noWait)
		trace.QueueDelete(ch.opt.ctx, name, ifUnused, ifEmpty, noWait)
		return result, err
	}
	return 0, amqp.ErrClosed
}

// QueueDeclare safely wraps the base channel QueueDeclare.
// Prefer using the [QueueDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result, err := ch.baseChan.super.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		trace.QueueDeclare(ch.opt.ctx, name, durable, autoDelete, exclusive, noWait, args)
		return result, err
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// ExchangeDelete safely wraps the base channel ExchangeDelete.
func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result := ch.baseChan.super.ExchangeDelete(name, ifUnused, noWait)
		trace.ExchangeDelete(ch.opt.ctx, name, ifUnused, noWait)
		return result
	}
	return amqp.ErrClosed
}

// ExchangeDeclare safely wraps the base channel ExchangeDeclare
// Prefer using the [ExchangeDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result := ch.baseChan.super.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
		trace.ExchangeDeclare(ch.opt.ctx, name, kind, durable, autoDelete, internal, noWait, args)
		return result
	}
	return amqp.ErrClosed
}

// Qos safely wraps the base channel Qos method, setting quality of service parameters.
func (ch *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result := ch.baseChan.super.Qos(prefetchCount, prefetchSize, global)
		trace.Qos(ch.opt.ctx, prefetchCount, prefetchSize, global)
		return result
	}
	return amqp.ErrClosed
}

// Consume safely wraps the base channel Consume.
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		result, err := ch.baseChan.super.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		trace.Consume(ch.opt.ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		return result, err
	}
	return nil, amqp.ErrClosed
}

// QueueDeclareWithTopology safely declares a desired queue as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) QueueDeclareWithTopology(t *TopologyOptions) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return declareQueue(ch.opt.ctx, ch.baseChan.super, t)
	}
	return amqp.Queue{}, amqp.ErrClosed
}

// ExchangeDeclareWithTopology safely declares a desired exchange as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) ExchangeDeclareWithTopology(t *TopologyOptions) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	if ch.baseChan.super != nil {
		return declareExchange(ch.opt.ctx, ch.baseChan.super, t)
	}
	return amqp.ErrClosed
}

// Queue returns the active (as indicated by [IsDestination] option in topology options) queue name.
// Useful for finding the server assigned name.
func (ch *Channel) Queue() string {
	ch.baseChan.mu.RLock()
	defer ch.baseChan.mu.RUnlock()

	return ch.queue
}

// Name returns the tag defined originally when creating this channel
func (ch *Channel) Name() string {
	return ch.opt.name
}

// Channel returns the low level library channel for further direct access to its Super() low level channel.
// Use sparingly and prefer using the predefined [Channel] wrapping methods instead.
// Pair usage with the provided full [Lock][Unlock] or read [RLock][RUnlock]
// locking/unlocking mechanisms for safety!
func (ch *Channel) Channel() *SafeBaseChan {
	return &ch.baseChan
}
