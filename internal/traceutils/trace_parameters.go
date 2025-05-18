package traceutils

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type ParamsConsume struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type ParamsQoS struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}
