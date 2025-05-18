//go:build test_env

package traceutils

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {
	p := ParamsTrace{
		Name: "Consume",
		Tag:  consumer,
		Value: ParamsConsume{
			queue, consumer, autoAck, exclusive, noLocal, noWait, args,
		},
	}

	// not all tests want to use the channel
	if ParamsCh != nil {
		ParamsCh <- p
	}

}

func Qos(prefetchCount, prefetchSize int, global bool) {
	p := ParamsTrace{
		Name: "Qos",
		Tag:  "",
		Value: ParamsQoS{
			prefetchCount, prefetchSize, global,
		},
	}
	// not all tests want to use the channel
	if ParamsCh != nil {
		ParamsCh <- p
	}
}
