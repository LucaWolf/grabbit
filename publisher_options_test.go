package grabbit

import (
	"context"
	"reflect"
	"testing"
	"time"

	trace "traceutils"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDefaultPublisherOptions(t *testing.T) {
	ctx := context.TODO()
	opt := PublisherOptions{
		PublisherUsageOptions: PublisherUsageOptions{
			ConfirmationCount:  10,
			ConfirmationNoWait: false,
			IsPublisher:        true,
		},
		Immediate: false,
		Mandatory: false,
	}

	tests := []struct {
		name string
		want PublisherOptions
	}{
		{
			"default",
			*opt.WithContext(ctx),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultPublisherOptions(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultPublisherOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPublisherOptionsSetters(t *testing.T) {
	opt := DefaultPublisherOptions()

	// ConfirmationNoWait
	if opt.ConfirmationNoWait != false {
		t.Errorf("DefaultPublisherOptions().ConfirmationNoWait -- should default to false")
	}

	result := opt.WithConfirmationNoWait(true)
	if result.ConfirmationNoWait != true {
		t.Errorf("opt.WithConfirmationNoWait(true).ConfirmationNoWait -- should be true")
	}

	result = opt.WithConfirmationNoWait(false)
	if result.ConfirmationNoWait != false {
		t.Errorf("opt.WithConfirmationNoWait(false).ConfirmationNoWait -- should be false")
	}

	// WithContext
	ctx := context.TODO()
	result = opt.WithContext(ctx)
	if result.Context != ctx {
		t.Errorf("opt.WithContext(ctx).Context -- should be ctx")
	}

	// WithImmediate
	result = opt.WithImmediate(true)
	if result.Immediate != true {
		t.Errorf("opt.WithImmediate(true).Immediate -- should be true")
	}
	result = opt.WithImmediate(false)
	if result.Immediate != false {
		t.Errorf("opt.WithImmediate(false).Immediate -- should be false")
	}

	// WithMandatory
	result = opt.WithMandatory(true)
	if result.Mandatory != true {
		t.Errorf("opt.WithMandatory(true).Mandatory -- should be true")
	}
	result = opt.WithMandatory(false)
	if result.Mandatory != false {
		t.Errorf("opt.WithMandatory(false).Mandatory -- should be false")
	}

	// WithExchange
	result = opt.WithExchange("test")
	if result.Exchange != "test" {
		t.Errorf("opt.WithExchange(\"test\").Exchange -- should be \"test\"")
	}

	// WithKey
	result = opt.WithKey("test")
	if result.Key != "test" {
		t.Errorf("opt.WithKey(\"test\").Key -- should be \"test\"")
	}

	// WithConfirmationCount
	result = opt.WithConfirmationsCount(15)
	if result.ConfirmationCount != 15 {
		t.Errorf("opt.WithConfirmationCount(15).ConfirmationCount -- should be 15")
	}

	if !reflect.DeepEqual(result, &opt) {
		t.Errorf("DefaultPublisherOptions().result.pointer = %v, want %v", result, opt)
	}
}

func TestPublisherOptions(t *testing.T) {
	QueueName := "workload_publisher_options"
	chCapacity := 32
	MANDATORY := true
	IMMEDIATE := true
	CONFIRM_NOWAIT := false // true blocks the amqp Confirm operation :-/
	IF_UNUSED := true
	IF_EMPTY := true
	DELETE_NOWAIT := true
	EXCHANGE := "exchange_opt"
	KEY := "key"
	statusCh := make(chan Event, chCapacity)

	validators := []trace.TraceValidator{
		{
			Name: "PublishWithDeferredConfirmWithContext",
			Tag:  QueueName,
			Expectations: []trace.FieldExpectation{
				{
					Field: "Key",
					Value: reflect.ValueOf(KEY),
				},
				{
					Field: "Exchange",
					Value: reflect.ValueOf(EXCHANGE),
				},
				{
					Field: "Mandatory",
					Value: reflect.ValueOf(MANDATORY),
				},
				{
					Field: "Immediate",
					Value: reflect.ValueOf(IMMEDIATE),
				},
			},
		},
		{
			Name: "QueueDelete",
			Tag:  QueueName,
			Expectations: []trace.FieldExpectation{
				{
					Field: "Queue",
					Value: reflect.ValueOf(QueueName),
				},
				{
					Field: "IfUnused",
					Value: reflect.ValueOf(IF_UNUSED),
				},
				{
					Field: "IfEmpty",
					Value: reflect.ValueOf(IF_EMPTY),
				},
				{
					Field: "NoWait",
					Value: reflect.ValueOf(DELETE_NOWAIT),
				},
			},
		},
		{
			Name: "ExchangeDelete",
			Tag:  EXCHANGE,
			Expectations: []trace.FieldExpectation{
				{
					Field: "Exchange",
					Value: reflect.ValueOf(EXCHANGE),
				},
				{
					Field: "IfUnused",
					Value: reflect.ValueOf(IF_UNUSED),
				},
				{
					Field: "NoWait",
					Value: reflect.ValueOf(DELETE_NOWAIT),
				},
			},
		},
	}
	ctxMaster, ctxCancel := trace.ConsumeTracesContext(validators)
	defer ctxCancel()

	chCounters := &EventCounters{
		Up:           &SafeCounter{},
		MsgPublished: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)

	// we want testing consumers/publishers reliability as soon as
	// w/out any artificial delay induced by testing the connection up status (tested elsewhere)
	// if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
	// 	t.Fatal("timeout waiting for connection to be ready")
	// }

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos,
		&TopologyOptions{
			Name:       EXCHANGE,
			Declare:    true,
			IsExchange: true,
			Durable:    true,
		},
		&TopologyOptions{
			Name:    QueueName,
			Declare: true,
			Bind: TopologyBind{
				Enabled: true,
				Key:     KEY,
				Peer:    EXCHANGE,
			},
		},
	)

	// make a publisher + queues infra
	returnCounter := &SafeCounter{}
	optPub := DefaultPublisherOptions()
	optPub.
		WithKey(KEY).
		WithExchange(EXCHANGE).
		WithContext(ctxMaster).
		WithConfirmationsCount(chCapacity).
		WithConfirmationNoWait(CONFIRM_NOWAIT).
		WithImmediate(IMMEDIATE).
		WithMandatory(MANDATORY)

	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("pub.options.test"),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotifyReturn(OnNotifyReturn(returnCounter)),
	)
	defer publisher.Channel().ExchangeDelete(EXCHANGE, IF_UNUSED, DELETE_NOWAIT)
	defer publisher.Channel().QueueDelete(QueueName, IF_UNUSED, IF_EMPTY, DELETE_NOWAIT)
	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}

	// all supported publising methods
	if err := publisher.Publish(amqp.Publishing{Body: []byte("test")}); err != nil {
		t.Error("Publish failed", err)
	}
	if _, err := publisher.PublishDeferredConfirm(amqp.Publishing{Body: []byte("test")}); err != nil {
		t.Error("PublishDeferredConfirmconfirm failed", err)
	}
	if err := publisher.PublishWithOptions(optPub, amqp.Publishing{Body: []byte("test")}); err != nil {
		t.Error("PublishWithOptions failed", err)
	}
	if _, err := publisher.PublishDeferredConfirmWithOptions(optPub, amqp.Publishing{Body: []byte("test")}); err != nil {
		t.Error("PublishDeferredConfirmWithOptions failed", err)
	}

	// parameters via API match the ones passed to the wrapped channel
	resultsCh := ctxMaster.Value(trace.TraceChannelResultsName).(chan trace.ErrorTrace)
	if len(resultsCh) > 0 {
		trace := <-resultsCh
		t.Errorf("Failed -> %v", trace.Err)
	}

	// general testing the publisher close/cancel/etc.
	if returnCounter.Value() != 0 {
		t.Errorf("no publish returns expected, got %d", returnCounter.Value())
	}

	// Closing the channel fails when IMMEDIATE is true
	if err := publisher.Close(); err != nil {
		// server raises: exception not_implemented: "immediate=true" :-/
		// t.Error("Close conn failed", err)
	}

	// still have access to the Channel
	c := publisher.Channel()
	if !c.IsClosed() {
		t.Error("channel should be closed")
	}
	//
	if publisher.AwaitAvailable(0, 0) {
		t.Error("publisher context should be done")
	}
	// all publishing should fail now with amqp.ErrClosed
	err := publisher.Publish(amqp.Publishing{Body: []byte("test")})
	if err != amqp.ErrClosed {
		t.Error("closed: Publish failed", err)
	}
	_, err = publisher.PublishDeferredConfirm(amqp.Publishing{Body: []byte("test")})
	if err != amqp.ErrClosed {
		t.Error("closed: PublishDeferredConfirmconfirm failed", err)
	}
	err = publisher.PublishWithOptions(optPub, amqp.Publishing{Body: []byte("test")})
	if err != amqp.ErrClosed {
		t.Error("closed: PublishWithOptions failed", err)
	}
	_, err = publisher.PublishDeferredConfirmWithOptions(optPub, amqp.Publishing{Body: []byte("test")})
	if err != amqp.ErrClosed {
		t.Error("closed: PublishDeferredConfirmWithOptions failed", err)
	}

}
