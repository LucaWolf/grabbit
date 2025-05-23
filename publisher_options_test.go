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
	QueueName := "workload"
	chCapacity := 32
	MANDATORY := true
	IMMEDIATE := true
	EXCHANGE := "exchange"
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
	}
	ctxMaster, ctxCancel := trace.ConsumeTracesContext(validators)
	defer ctxCancel()

	eventCounters := &EventCounters{
		Up:           &SafeCounter{},
		MsgPublished: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, eventCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)
	// await connections which should have raised a series of events
	if !ConditionWait(ctxMaster, eventCounters.Up.NotZero, 40*time.Second, 1*time.Second) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos,
		&TopologyOptions{
			Name:       EXCHANGE,
			Durable:    true,
			Declare:    true,
			IsExchange: true,
		},
		&TopologyOptions{
			Name:    QueueName,
			Durable: true,
			Declare: true,
			Bind: TopologyBind{
				Enabled: true,
				Key:     KEY,
				Peer:    EXCHANGE,
			},
		},
	)

	// make a publisher + queues infra
	optPub := DefaultPublisherOptions()
	optPub.
		WithKey(KEY).
		WithExchange(EXCHANGE).
		WithContext(ctxMaster).
		WithConfirmationsCount(chCapacity).
		// TODO both these options fail... create dedicated publshing tests and fix
		// WithConfirmationNoWait(true)
		WithImmediate(IMMEDIATE).
		WithMandatory(MANDATORY)

	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
	)
	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}

	if _, err := PublishMsgBulk(publisher, 1, "test"); err != nil {
		t.Error(err)
	}

	// parameters via API match the ones passed to the wrapped channel
	resultsCh := ctxMaster.Value(trace.TraceChannelResultsName).(chan trace.ErrorTrace)
	if len(resultsCh) > 0 {
		trace := <-resultsCh
		t.Errorf("Failed -> %v", trace.Err)
	}
}
