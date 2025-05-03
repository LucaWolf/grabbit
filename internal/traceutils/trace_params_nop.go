//go:build !test_env

package traceutils

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consume is a no-op for live code. For testing purposes it builds a ParamsConsume
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Consume(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
}

// Qos is a no-op for live code. For testing purposes it builds a ParamsQoS
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Qos(ctx context.Context, prefetchCount, prefetchSize int, global bool) {
}

func ExchangeDeclare(ctx context.Context, name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) {
}

func QueueDeclare(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
}
func QueueDeclarePassive(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
}

func QueueBind(ctx context.Context, queue, key, exchange string, noWait bool, args amqp.Table) {
}

func ExchangeBind(ctx context.Context, destination, key, source string, noWait bool, args amqp.Table) {
}

func QueueInspect(ctx context.Context, name string) {
}

func PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

func PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
}

func QueuePurge(ctx context.Context, name string, noWait bool) {
}

func QueueDelete(ctx context.Context, name string, ifUnused, ifEmpty, noWait bool) {
}

func ExchangeDelete(ctx context.Context, name string, ifUnused, noWait bool) {
}
