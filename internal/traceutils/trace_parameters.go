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

type ParamsExchangeDeclare struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type ParamsQueueDeclare struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type ParamsQueueBind struct {
	Queue    string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

type ParamsExchangeBind struct {
	Destination string
	Key         string
	Source      string
	NoWait      bool
	Args        amqp.Table
}

type ParamsQueueInspect struct {
	Name string
}

type ParamsPublishWithContext struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing
}

type ParamsQueuePurge struct {
	Queue  string
	NoWait bool
}

type ParamsQueueDelete struct {
	Queue    string
	IfUnused bool
	IfEmpty  bool
	NoWait   bool
}

type ParamsExchangeDelete struct {
	Exchange string
	IfUnused bool
	NoWait   bool
}
