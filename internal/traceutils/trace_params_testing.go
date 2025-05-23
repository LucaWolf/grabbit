//go:build test_env

package traceutils

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Consume(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "Consume",
		Tag:  consumer,
		Value: ParamsConsume{
			queue, consumer, autoAck, exclusive, noLocal, noWait, args,
		},
	}

	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func Qos(ctx context.Context, prefetchCount, prefetchSize int, global bool) {
	p := ParamsTrace{
		Name: "Qos",
		Tag:  "",
		Value: ParamsQoS{
			prefetchCount, prefetchSize, global,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func ExchangeDeclare(ctx context.Context, name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "ExchangeDeclare",
		Tag:  name,
		Value: ParamsExchangeDeclare{
			name, kind, durable, autoDelete, internal, noWait, args,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueueDeclare(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "QueueDeclare",
		Tag:  name,
		Value: ParamsQueueDeclare{
			name, durable, autoDelete, exclusive, noWait, args,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueueDeclarePassive(ctx context.Context, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "QueueDeclarePassive",
		Tag:  name,
		Value: ParamsQueueDeclare{
			name, durable, autoDelete, exclusive, noWait, args,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueueBind(ctx context.Context, queue, key, exchange string, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "QueueBind",
		Tag:  queue,
		Value: ParamsQueueBind{
			queue, key, exchange, noWait, args,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func ExchangeBind(ctx context.Context, destination, key, source string, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "ExchangeBind",
		Tag:  destination,
		Value: ParamsExchangeBind{
			destination, key, source, noWait, args,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueueInspect(ctx context.Context, name string) {
	p := ParamsTrace{
		Name: "QueueInspect",
		Tag:  name,
		Value: ParamsQueueInspect{
			name,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	p := ParamsTrace{
		Name: "PublishWithContext",
		Tag:  key,
		Value: ParamsPublishWithContext{
			exchange, key, mandatory, immediate, msg,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	p := ParamsTrace{
		Name: "PublishWithDeferredConfirmWithContext",
		Tag:  key,
		Value: ParamsPublishWithContext{
			exchange, key, mandatory, immediate, msg,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueuePurge(ctx context.Context, name string, noWait bool) {
	p := ParamsTrace{
		Name: "QueuePurge",
		Tag:  name,
		Value: ParamsQueuePurge{
			name, noWait,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func QueueDelete(ctx context.Context, name string, ifUnused, ifEmpty, noWait bool) {
	p := ParamsTrace{
		Name: "QueueDelete",
		Tag:  name,
		Value: ParamsQueueDelete{
			name, ifUnused, ifEmpty, noWait,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}

func ExchangeDelete(ctx context.Context, name string, ifUnused, noWait bool) {
	p := ParamsTrace{
		Name: "ExchangeDelete",
		Tag:  name,
		Value: ParamsExchangeDelete{
			name, ifUnused, noWait,
		},
	}
	// not all tests want to use the channel
	if paramsCh := ctx.Value(TraceChannelParamsName); paramsCh != nil {
		paramsCh.(chan ParamsTrace) <- p
	}
}
