//go:build !test_env

package traceutils

import amqp "github.com/rabbitmq/amqp091-go"

// Consume is a no-op for live code. For testing purposes it builds a ParamsConsume
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) {

}

// Qos is a no-op for live code. For testing purposes it builds a ParamsQoS
// and sends it to the 'ParamsCh' channel (see TraceHandler).
func Qos(prefetchCount, prefetchSize int, global bool) {

}
