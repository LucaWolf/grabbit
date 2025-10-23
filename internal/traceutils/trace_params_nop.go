//go:build !test_env

package traceutils

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Confirm is a no-op for live code. For testing purposes it builds a ParamsConfirm
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Confirm(ctx context.Context, noWait bool) {
}

// Consume is a no-op for live code. For testing purposes it builds a ParamsConsume
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Consume(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
}

// ConsumeWithContext is a no-op for live code. For testing purposes it builds a ParamsConsumeWithContext
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
}

// ExchangeBind is a no-op for live code. For testing purposes it builds a ParamsExchangeBind
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func ExchangeBind(ctx context.Context, destination, key, source string, noWait bool, args amqp.Table) {
}

// ExchangeDeclare is a no-op for live code. For testing purposes it builds a ParamsExchangeDeclare
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func ExchangeDeclare(ctx context.Context, name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) {
}

// ExchangeDelete is a no-op for live code. For testing purposes it builds a ParamsExchangeDelete
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func ExchangeDelete(ctx context.Context, name string, ifUnused, noWait bool) {
}

// Flow is a no-op for live code. For testing purposes it builds a ParamsFlow
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Flow(ctx context.Context, active bool) {
}

// Get is a no-op for live code. For testing purposes it builds a ParamsGet
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Get(ctx context.Context, queue string, autoAck bool) {
}

// Publish is a no-op for live code. For testing purposes it builds a ParamsPublishWithContext
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

// PublishWithContext is a no-op for live code. For testing purposes it builds a ParamsPublishWithContext
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

// PublishWithDeferredConfirm is a no-op for live code. For testing purposes it builds a ParamsPublishWithContext
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func PublishWithDeferredConfirm(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

// PublishWithDeferredConfirmWithContext is a no-op for live code. For testing purposes it builds a ParamsPublishWithContext
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

// Qos is a no-op for live code. For testing purposes it builds a ParamsQoS
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Qos(ctx context.Context, prefetchCount, prefetchSize int, global bool) {
}

// QueueBind is a no-op for live code. For testing purposes it builds a ParamsQueueBind
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueBind(ctx context.Context, queue, key, exchange string, noWait bool, args amqp.Table) {
}

// QueueDeclare is a no-op for live code. For testing purposes it builds a ParamsQueueDeclare
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueDeclare(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
}

// QueueDeclarePassive is a no-op for live code. For testing purposes it builds a ParamsQueueDeclare
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueDeclarePassive(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
}

// QueueDelete is a no-op for live code. For testing purposes it builds a ParamsQueueDelete
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueDelete(ctx context.Context, name string, ifUnused, ifEmpty, noWait bool) {
}

// QueueInspect is a no-op for live code. For testing purposes it builds a ParamsQueueInspect
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueInspect(ctx context.Context, name string) {
}

// QueuePurge is a no-op for live code. For testing purposes it builds a ParamsQueuePurge
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueuePurge(ctx context.Context, name string, noWait bool) {
}

// QueueUnbind is a no-op for live code. For testing purposes it builds a ParamsQueueUnbind
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func QueueUnbind(ctx context.Context, name, key, exchange string, args amqp.Table) {
}

// Recover is a no-op for live code. For testing purposes it builds a ParamsRecover
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Recover(ctx context.Context, requeue bool) {
}

// Tx is a no-op for live code. For testing purposes it builds a ParamsTx
func Tx(ctx context.Context) {
}

// TxCommit is a no-op for live code. For testing purposes it builds a ParamsTxCommit
func TxCommit(ctx context.Context) {
}

// TxRollback is a no-op for live code. For testing purposes it builds a ParamsTxRollback
func TxRollback(ctx context.Context) {
}
