package grabbit

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var downCallbackCounter SafeCounter
var upCallbackCounter SafeCounter
var recoveringCallbackCounter SafeCounter

func connDownCB(name string, err OptionalError) bool {
	downCallbackCounter.Add(1)
	return true // want continuing
}

func connUpCB(name string) {
	upCallbackCounter.Add(1)
}

func connReconnectCB(name string, retry int) bool {
	recoveringCallbackCounter.Add(1)
	return true // want continuing
}

func TestNewConnection(t *testing.T) {
	connStatusChan := make(chan Event, 32)

	downCallbackCounter.Reset()
	upCallbackCounter.Reset()
	recoveringCallbackCounter.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventCounters := &EventCounters{
		Up:       &SafeCounter{},
		Down:     &SafeCounter{},
		Closed:   &SafeCounter{},
		Recovery: &SafeCounter{},
	}
	go procStatusEvents(ctx, connStatusChan, eventCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionName("test.conn"),
		WithConnectionOptionDown(connDownCB),
		WithConnectionOptionUp(connUpCB),
		WithConnectionOptionRecovering(connReconnectCB),
		WithConnectionOptionNotification(connStatusChan),
		WithConnectionOptionContext(ctx),
	)
	// await connection which should have raised a series of events
	if !ConditionWait(ctx, eventCounters.Up.NotZero, 30*time.Second, time.Second) {
		t.Fatal("timeout waiting for connection to be ready")
	}
	<-time.After(3 * time.Second)

	// TODO send to the RMQ engine a client disconnect command
	// this should trigger a recovery, hence the callbacks will have its kind
	// expect down & up again

	conn.Close()
	if !ConditionWait(ctx, eventCounters.Down.NotZero, 5*time.Second, time.Second) {
		t.Error("timeout waiting for connection to be down")
	}
	if !ConditionWait(ctx, eventCounters.Closed.NotZero, 5*time.Second, time.Second) {
		t.Error("timeout waiting for connection to be closed")
	}

	// finally test we got all desired callback.
	if upCallbackCounter.Value() != 1 {
		t.Errorf("upCallback expected %v, got %v", 1, upCallbackCounter.Value())
	}
	if downCallbackCounter.Value() != 1 {
		t.Errorf("downCallback expected %v, got %v", 1, downCallbackCounter.Value())
	}
	// this is called at least once during the initial connection,
	// and then after each 'EventCannotEstablish' (e.g. rabbitMQ service was not available)
	// ... but we skipped those.
	if recoveringCallbackCounter.Value() != 1 {
		t.Errorf("recoveringCallback expected %v, got %v", 1, recoveringCallbackCounter.Value())
	}
}

func ReattemptingDenied(name string, retry int) bool {
	recoveringCallbackCounter.Add(1)
	return false // break out
}

func TestConnectionDenyRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	downCallbackCounter.Reset()
	upCallbackCounter.Reset()
	recoveringCallbackCounter.Reset()

	conn := NewConnection(
		"amqp://guest:quest@localhost:5672/", // bad pwd to make it fail forever
		amqp.Config{},
		WithConnectionOptionName("grabbit-test"),
		WithConnectionOptionDown(connDownCB),
		WithConnectionOptionUp(connUpCB),
		WithConnectionOptionRecovering(ReattemptingDenied),
		WithConnectionOptionContext(ctx),
	)
	// give it a bit to actually call the callbacks
	<-time.After(5 * time.Second)

	if !conn.IsClosed() {
		t.Error("connection should be initially closed")
	}
	if recoveringCallbackCounter.Value() == 0 {
		t.Errorf("recoveringCallback expected some")
	}
	// it never went up
	if upCallbackCounter.Value() != 0 {
		t.Errorf("upCallback expected %v, got %v", 0, upCallbackCounter.Value())
	}
	// it never transitioned down (from up)
	if downCallbackCounter.Value() != 0 {
		t.Errorf("downCallback expected %v, got %v", 0, downCallbackCounter.Value())
	}
	if !conn.IsClosed() {
		t.Error("connection should be finally closed")
	}
	// base connection does not exists
	baseConn := conn.Connection()
	if baseConn.IsSet() {
		t.Error("connection should not be set")
	}
	// can close several times w/out any repercussions
	conn.Close()
	conn.Close()
	conn.Close()
}

func TestConnectionCloseContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	downCallbackCounter.Reset()
	upCallbackCounter.Reset()
	recoveringCallbackCounter.Reset()

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionName("grabbit-test"),
		WithConnectionOptionDown(connDownCB),
		WithConnectionOptionUp(connUpCB),
		WithConnectionOptionRecovering(connReconnectCB),
		WithConnectionOptionContext(ctx),
	)

	// another way of waiting for the connection to be ready
	for i := 0; i < 10; i++ {
		if i == 10 {
			t.Fatal("timeout waiting for connection to be ready")
		}
		if !conn.IsClosed() {
			break
		}
		<-time.After(3 * time.Second)
	}
	if conn.IsClosed() {
		t.Fatal("connection should now be open")
	}
	// kill connection handler's context
	cancel()
	<-time.After(3 * time.Second)

	if !conn.IsClosed() {
		t.Error("connection should now be closed")
	}
	// can close several times w/out any repercussions
	if err := conn.Close(); err != nil {
		t.Errorf("Connection closing.1 failed with = %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Errorf("Connection closing.2 failed with = %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Errorf("Connection closing.3 failed with = %v", err)
	}

	baseConn := conn.Connection()
	if baseConn.IsSet() {
		t.Error("connection should not be set")
	}

	if upCallbackCounter.Value() != 1 {
		t.Errorf("upCallback expected %v, got %v", 1, upCallbackCounter.Value())
	}
	// ctx cancel provides abrupt shutdown: no down/close events (or callback)
	if downCallbackCounter.Value() != 0 {
		t.Errorf("downCallback expected %v, got %v", 0, downCallbackCounter.Value())
	}
	// there must be at least one initial attempt
	if recoveringCallbackCounter.Value() == 0 {
		t.Errorf("recoveringCallback expected some")
	}

}
