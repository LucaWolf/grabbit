package grabbit

import (
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

	return ch.baseChan.super.Cancel(consumer, noWait)
}

// Ack safely wraps the base channel Ack.
func (ch *Channel) Ack(tag uint64, multiple bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.Ack(tag, multiple)
}

// Ack safely wraps the base channel Nak.
func (ch *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.Nack(tag, multiple, requeue)
}

// QueueInspect safely wraps the base channel QueueInspect.
func (ch *Channel) QueueInspect(name string) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.QueueInspect(name)
}

// QueuePurge safely wraps teh base channel QueuePurge.
func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.QueuePurge(name, noWait)
}

// QueueDelete safely wraps the base channel QueueDelete.
func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// QueueDeclare safely wraps the base channel QueueDeclare.
// Prefer using the [QueueDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// ExchangeDeclare safely wraps the base channel ExchangeDeclare
// Prefer using the [ExchangeDeclareWithTopology] instead; that also supports bindings, see [TopologyOptions]
func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return ch.baseChan.super.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// QueueDeclareWithTopology safely declares a desired queue as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) QueueDeclareWithTopology(t *TopologyOptions) (amqp.Queue, error) {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return declareQueue(ch.baseChan.super, t)
}

// ExchangeDeclareWithTopology safely declares a desired exchange as described in the parameter;
// see [TopologyOptions]
func (ch *Channel) ExchangeDeclareWithTopology(t *TopologyOptions) error {
	ch.baseChan.mu.Lock()
	defer ch.baseChan.mu.Unlock()

	return declareExchange(ch.baseChan.super, t)
}

// Queue returns the active (as indicated by [IsDestination] option in topology options) queue name.
// Useful for finding the server assigned name.
func (ch *Channel) Queue() string {
	return ch.queue
}

// Channel returns the low level library channel for further direct access to its Super() low level channel.
// Use sparingly and prefer using the predefined [Channel] wrapping methods instead.
// Pair usage with the locking/unlocking routines for safety!
func (ch *Channel) Channel() *SafeBaseChan {
	return &ch.baseChan
}
