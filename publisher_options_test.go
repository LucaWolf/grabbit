package grabbit

import (
	"context"
	"fmt"
	"reflect"
	"testing"

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
		WithConnectionOptionName("conn.pub.opt"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

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
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready yet")
	}

	// all supported publising methods
	pubMethods := []PublishRoutine{
		PublishSimple,
		PublishWithOptions,
		PublishDeferredConfirm,
		PublishDeferredConfirmWithOptions,
	}
	pubCount := 0
	for i, pubMethods := range pubMethods {
		if _, err := PublishMsgBulkWith(pubMethods, publisher, optPub, i, pubMethods.String()); err != nil {
			t.Errorf("%s failed %s\n", pubMethods.String(), err)
		}
		pubCount += i
	}

	// parameters via API match the ones passed to the wrapped channel
	resultsCh := ctxMaster.Value(trace.TraceChannelResultsName).(chan trace.ErrorTrace)
	if len(resultsCh) > 0 {
		trace := <-resultsCh
		t.Errorf("Failed -> %v", trace.Err)
	}

	// we sent (pubCount) immediate with no consumers, expect returns notifications from server
	if !ConditionWait(ctxMaster, returnCounter.ValueEquals(pubCount), ShortPoll) {
		t.Errorf("publish returns expected %d, got %d", pubCount, returnCounter.Value())
	}

	// rabbitMQ only: Closing the channel fails when IMMEDIATE is true
	// exception not_implemented: "immediate=true"
	if err := publisher.Close(); err != nil {
		t.Error("Close conn failed", err)
	}

	// publisher's channel should be done
	if !publisher.AwaitStatus(false, LongPoll.Timeout) {
		t.Error("publisher should be closed")
	}
	// and so its managing routine
	if !publisher.AwaitManager(false, LongPoll.Timeout) {
		t.Error("publisher's manager should have terminated")
	}

	// still have access to the Channel
	c := publisher.Channel()
	if !c.IsClosed() {
		t.Error("channel should be closed")
	}

	// all publishing should fail now with amqp.ErrClosed
	for _, pubMethods := range pubMethods {
		_, err := PublishMsgBulkWith(pubMethods, publisher, optPub, 1, pubMethods.String())
		if err != amqp.ErrClosed {
			t.Errorf("%s should return amqp.ErrClosed %s\n", pubMethods.String(), err)
		}
	}
}

func TestPublishNoConfirm(t *testing.T) {
	QueueName := "workload_publish_no_confirm"
	const MSG_COUNT = 100
	statusCh := make(chan Event, MSG_COUNT)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel()

	chCounters := &EventCounters{
		MsgPublished: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.pub.no.confirm"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	// connection is up
	if !conn.AwaitStatus(true, DefaultPoll.Timeout) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	// massive publishing causes a lot of events.
	opt := DefaultPublisherOptions()
	opt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(0)

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos, &TopologyOptions{
		Name:          QueueName,
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	})

	publisher := NewPublisher(conn, opt,
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(statusCh),
	)
	defer publisher.Channel().QueueDelete(QueueName, false, false, true)

	if !publisher.AwaitStatus(true, DefaultPoll.Timeout) {
		t.Fatal("publisher not ready yet")
	}

	tag := "no-confirmations"
	n, err := PublishMsgBulkWith(PublishSimple, publisher, opt, MSG_COUNT, tag)
	if err != nil {
		t.Error(err)
	}
	if n != MSG_COUNT {
		t.Error("Incomplete publishing return", n)
	}

	// NOTE: disabling confirmation channel will also shunt the cbNotifyPublish callback!
	// No MsgPublished change is expected.
	if ConditionWait(ctxMaster, chCounters.MsgPublished.Greater(0), ShortPoll) {
		t.Error("timeout pushing all the messages", chCounters.MsgPublished.Value())
	}

	batchSelector := NewSafeRand(MSG_COUNT)
	registry := NewSafeRegisterMap()

	optConsumer := DefaultConsumerOptions()
	optConsumer.WithQueue(QueueName).WithPrefetchTimeout(DefaultPoll.Timeout)

	consumer := NewConsumer(conn, *optConsumer.WithName("consumer.one"),
		WithChannelOptionName("chan.consumer.one"),
		WithChannelOptionProcessor(MsgHandlerBulk(batchSelector, registry)),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
	)

	if !consumer.AwaitStatus(true, DefaultPoll.Timeout) {
		t.Fatal("consumer not ready yet")
	}

	// this also test for messages consistency as the registry map keeps unique entries
	if !ConditionWait(ctxMaster, registry.LenEquals(MSG_COUNT), LongVeryFrequentPoll) {
		t.Errorf("incomplete consumption: expected %d vs. read %d", MSG_COUNT, registry.Length())
	}
	// probe for some random value by the publishing schema
	msg := fmt.Sprintf(PUBLISH_DATA_SCHEMA, tag, batchSelector.ClampInt(MSG_COUNT))
	if _, has := registry.Has(msg); !has {
		t.Error("missing expected value:", msg)
	}
}
