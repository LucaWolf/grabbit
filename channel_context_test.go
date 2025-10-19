package grabbit

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestContextCancellation tests that context are handler properly.
//
// 1. sibling contexts are independent. Closing one channel (which cancels own ctx) should not affect the other.
// 2. the context cancellation is propagated from parent to child
func TestContextCancellation(t *testing.T) {
	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionName("test.ctx"),
	)
	defer conn.Close() // 'goleak' would complain w/out final clean-up

	alphaStatusChan := make(chan Event, 5)
	betaStatusChan := make(chan Event, 5)
	// parent context
	ctxAlpha, ctxAlphaCancel := context.WithCancel(context.Background())
	// child context
	ctxBeta, _ := context.WithCancel(ctxAlpha)
	defer ctxAlphaCancel()

	// events accounting
	alphaEventCounters := &EventCounters{
		Up:     &SafeCounter{},
		Down:   &SafeCounter{},
		Closed: &SafeCounter{},
	}
	go procStatusEvents(ctxAlpha, alphaStatusChan, alphaEventCounters, nil)

	betaEventCounters := &EventCounters{
		Up:     &SafeCounter{},
		Down:   &SafeCounter{},
		Closed: &SafeCounter{},
	}
	go procStatusEvents(ctxBeta, betaStatusChan, betaEventCounters, nil)
	// create two independent channels; expect their inner contexts to become decoupled
	alphaCh := NewChannel(conn,
		WithChannelOptionContext(ctxAlpha),
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionNotification(alphaStatusChan),
	)
	betaCh := NewChannel(conn,
		WithChannelOptionContext(ctxBeta),
		WithChannelOptionName("chan.beta"),
		WithChannelOptionNotification(betaStatusChan),
	)

	if !ConditionWait(ctxAlpha, alphaEventCounters.Up.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for Alpha connection to be ready")
	}
	if !ConditionWait(ctxBeta, betaEventCounters.Up.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for Beta connection to be ready")
	}
	<-time.After(1 * time.Second)
	if alphaEventCounters.Down.NotZero() || alphaEventCounters.Closed.NotZero() {
		t.Error("Alpha connection went down/closed unexpectedly")
	}
	if betaEventCounters.Down.NotZero() || betaEventCounters.Closed.NotZero() {
		t.Error("Beta connection went down/closed unexpectedly")
	}

	// closing alphaCh should not close betaCh even though beta's context has been initiated from alpha's.
	// this is because closing alphaCh should only trip-over the hidden ctxInnerAlpha.
	// ctxAlpha-->ctxInnerAlpha
	//         \->ctxBeta->ctxInnerBeta
	//
	if err := alphaCh.Close(); err != nil {
		t.Errorf("Channel alpha closing.0 failed with = %v", err)
	}
	if !ConditionWait(ctxAlpha, alphaEventCounters.Down.NotZero, DefaultPoll) {
		t.Error("alphaCh should have been closed by now:", alphaEventCounters.Down.Value())
	}
	if ConditionWait(ctxBeta, betaEventCounters.Down.NotZero, ShortPoll) {
		t.Error("betaCh should still be open")
	}

	// but closing the parent ctxAlpha context should induce beta to shut-down
	ctxAlphaCancel()
	// Reminder: sudden death via ctx cancellation _might_ not provide any Down/Closed feedback
	if !ConditionWait(context.TODO(), betaCh.IsClosed, DefaultPoll) {
		t.Error("betaCh should have been closed by now: ", betaEventCounters.Down.Value())
	}

	// can close several times w/out any repercussions
	if err := alphaCh.Close(); err != nil {
		t.Errorf("Channel alpha closing.1 failed with = %v", err)
	}
	if err := betaCh.Close(); err != nil {
		t.Errorf("Channel beta closing.1 failed with = %v", err)
	}
}
