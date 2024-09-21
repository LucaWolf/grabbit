package grabbit

import (
	"context"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SafeCounter struct {
	counter int
	mu      sync.RWMutex // makes this concurrent safe, maintenance wise only!
}

func (c *SafeCounter) Add(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter += delta
}

func (c *SafeCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter = 0
}

func (c *SafeCounter) Counter() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.counter
}

var downCallbackCounter SafeCounter
var upCallbackCounter SafeCounter
var recoveringCallbackCounter SafeCounter

func Down(name string, err OptionalError) bool {
	downCallbackCounter.Add(1)
	return true // want continuing
}

func Up(name string) {
	upCallbackCounter.Add(1)
}

func Reattempting(name string, retry int) bool {
	recoveringCallbackCounter.Add(1)
	return true // want continuing
}

func TestNewConnection(t *testing.T) {
	connStatusChan := make(chan Event, 32)
	wgReady := &sync.WaitGroup{}
	wgDown := &sync.WaitGroup{}
	wgClosed := &sync.WaitGroup{}
	wgReady.Add(1)

	downCallbackCounter.Reset()
	upCallbackCounter.Reset()
	recoveringCallbackCounter.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done(): // 1st level of coroutine protection
				return
			case event, ok := <-connStatusChan:
				if !ok { // 2nd level of coroutine protection
					t.Log("connStatusChan closed")
					return
				}
				switch event.Kind {
				case EventUp:
					wgDown.Add(1) // activate next triggers
					wgClosed.Add(1)
					wgReady.Done() // signal ready
				case EventDown:
					wgDown.Done()
				case EventClosed:
					wgClosed.Done()
				case EventCannotEstablish:
					// let's tally up with the callbacks so to capture just the real hiccups
					recoveringCallbackCounter.Add(-1)
				default:
					t.Log("conn EVENT: ", event)
				}
			}
		}
	}()

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL,
		amqp.Config{},
		WithConnectionOptionName("grabbit-test"),
		WithConnectionOptionDown(Down),
		WithConnectionOptionUp(Up),
		WithConnectionOptionRecovering(Reattempting),
		WithConnectionOptionNotification(connStatusChan),
		WithConnectionOptionContext(ctx),
	)
	// await connection which should have raised a series of events
	if !wgDoneOrTimeout(wgReady, 30*time.Second) {
		t.Fatal("timeout waiting for connection to be ready")
	}
	<-time.After(5 * time.Second)

	// TODO send to the RMQ engine a client disconnect command
	// this should trigger a recovery, hence the callbacks will have its kind
	// expect down & up again

	conn.Close()
	if !wgDoneOrTimeout(wgDown, 5*time.Second) {
		t.Fatal("timeout waiting for connection to be down")
	}
	if !wgDoneOrTimeout(wgClosed, 5*time.Second) {
		t.Fatal("timeout waiting for connection to be closed")
	}

	// finally test we got all desired callback.
	if upCallbackCounter.Counter() != 1 {
		t.Fatalf("upCallback expected %v, got %v", 1, upCallbackCounter.Counter())
	}
	if downCallbackCounter.Counter() != 1 {
		t.Fatalf("downCallback expected %v, got %v", 1, downCallbackCounter.Counter())
	}
	// this is called at least once during the initial connection,
	// and then after each 'EventCannotEstablish' (e.g. rabbitMQ service was not available)
	// ... but we skipped those.
	if recoveringCallbackCounter.Counter() != 1 {
		t.Fatalf("recoveringCallback expected %v, got %v", 1, recoveringCallbackCounter.Counter())
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
		WithConnectionOptionDown(Down),
		WithConnectionOptionUp(Up),
		WithConnectionOptionRecovering(ReattemptingDenied),
		WithConnectionOptionContext(ctx),
	)
	// give it a bit to actually call the callbacks
	<-time.After(5 * time.Second)

	if !conn.IsClosed() {
		t.Fatal("connection should be initially closed")
	}
	if recoveringCallbackCounter.Counter() == 0 {
		t.Fatalf("recoveringCallback expected some")
	}
	// it never went up
	if upCallbackCounter.Counter() != 0 {
		t.Fatalf("upCallback expected %v, got %v", 0, upCallbackCounter.Counter())
	}
	// it never transitioned down (from up)
	if downCallbackCounter.Counter() != 0 {
		t.Fatalf("downCallback expected %v, got %v", 0, downCallbackCounter.Counter())
	}
	if !conn.IsClosed() {
		t.Fatal("connection should be finally closed")
	}
	// base connection does not exists
	baseConn := conn.Connection()
	if baseConn.IsSet() {
		t.Fatal("connection should not be set")
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
		CONN_ADDR_RMQ_LOCAL,
		amqp.Config{},
		WithConnectionOptionName("grabbit-test"),
		WithConnectionOptionDown(Down),
		WithConnectionOptionUp(Up),
		WithConnectionOptionRecovering(Reattempting),
		WithConnectionOptionContext(ctx),
	)

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
	cancel() // Warning: no EventDown/EventClosed pushed
	<-time.After(3 * time.Second)

	if !conn.IsClosed() {
		t.Fatal("connection should now be closed")
	}
	// can close several times w/out any repercussions
	conn.Close()
	conn.Close()
	conn.Close()

	baseConn := conn.Connection()
	if baseConn.IsSet() {
		t.Fatal("connection should not be set")
	}

	if upCallbackCounter.Counter() != 1 {
		t.Fatalf("upCallback expected %v, got %v", 1, upCallbackCounter.Counter())
	}
	// ctx cancel provides abrupt shutdown: no down/close events (or callback)
	if downCallbackCounter.Counter() != 0 {
		t.Fatalf("downCallback expected %v, got %v", 0, downCallbackCounter.Counter())
	}
	// there must be at least one initial attempt
	if recoveringCallbackCounter.Counter() == 0 {
		t.Fatalf("recoveringCallback expected some")
	}

}
