package grabbit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func expectSingleQueue(cli *rabbithole.Client, name string) error {
	qs, err := cli.ListQueues()
	if err != nil {
		return err
	}
	if len(qs) != 1 {
		return errors.New("expecting a single queue")
	}
	q := qs[0]
	if q.Name != name {
		return fmt.Errorf("expecting %s got %s", name, q.Name)
	}
	return nil
}

// TestChannelTopology tests that topologies are re-created after the current channel is recovered
func TestChannelTopology(t *testing.T) {
	qName := "test_queue"
	qDurable := false // we want to test if recreated after recovery

	downCallbackCounter.Reset()
	upCallbackCounter.Reset()
	recoveringCallbackCounter.Reset()
	delayerCallbackCounter.Reset()

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionName("test.ctx"),
	)
	defer conn.Close()

	statusCh := make(chan Event, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 'goleak' would complain w/out final clean-up

	topos := make([]*TopologyOptions, 0, 2)
	topos = append(topos, &TopologyOptions{
		Name:          qName,
		IsDestination: true,
		Durable:       qDurable,
		Declare:       true,
	})

	// events accounting
	chCounters := &EventCounters{
		Up:       &SafeCounter{},
		Down:     &SafeCounter{},
		Closed:   &SafeCounter{},
		Recovery: &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, chCounters, nil)
	// create the test channel
	testCh := NewChannel(conn,
		WithChannelOptionContext(ctx),
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
		// also test these callbacks are actually called at some point
		WithChannelOptionDown(connDownCB),
		WithChannelOptionUp(connUpCB),
		WithChannelOptionRecovering(connReconnectCB),
		WithChannelOptionDelay(tracingDelayer{Value: time.Second}),
	)

	// channel setup takes a while (we don't wait here for connection completion)
	// so it will trigger quite a few "recovery" and "delayer" callbacks
	if !ConditionWait(ctx, recoveringCallbackCounter.NotZero, 3*time.Second, 0) {
		t.Fatal("timeout waiting for channel to setup/recovery event")
	}

	// link-up
	if !ConditionWait(ctx, chCounters.Up.NotZero, 30*time.Second, 0) {
		t.Fatal("timeout waiting for channel to be ready")
	}
	if !ConditionWait(ctx, upCallbackCounter.NotZero, 3*time.Second, 0) {
		t.Fatal("timeout waiting for channel to be ready (cb)")
	}

	// there should be no unrequested hiccups on the channel
	if ConditionWait(
		ctx,
		func() bool { return chCounters.Down.NotZero() || chCounters.Closed.NotZero() },
		3*time.Second,
		0) {
		t.Error("channel went down/closed unexpectedly")
	}

	// Power grab: directly via the inner base and super channels.
	// WANNING: murky waters, make sure you protect the inner workings
	baseCh := testCh.Channel()
	amqpCh := baseCh.Super()
	baseCh.Lock()
	_, err := amqpCh.QueueDeclarePassive(qName, qDurable, false, false, false, nil)
	baseCh.UnLock()
	if err != nil {
		t.Error("failed to fetched queue for channel topology", err)
	}
	// no errors mean the queue parameters match our topology.
	// on error QueueDeclarePassive() throws and kills your channel

	// List queues by alternative means
	rhClient, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
	if err != nil {
		t.Error("rabbithole controller unavailable")
	}

	if err := expectSingleQueue(rhClient, qName); err != nil {
		t.Error("rabbithole failed to list queue", err)
	}

	// Forcefully close test specific connection
	upCounterBefore := chCounters.Up.Value()
	upCallbackCounterBefore := upCallbackCounter.Value()
	recoveryCountBefore := chCounters.Recovery.Value()
	delayerCallbackCounterBefore := delayerCallbackCounter.Value()

	xs, _ := rhClient.ListConnections()
	for _, x := range xs {
		if _, err := rhClient.CloseConnection(x.Name); err != nil {
			t.Error("rabbithole failed to close connection", err, " for: ", x.Name)
		}
	}

	// test the grabbit connection and queue have recovered after a while
	if !ConditionWait(ctx, chCounters.Down.NotZero, 30*time.Second, 0) {
		t.Error("timeout waiting for channel to go down")
	}
	if !ConditionWait(ctx, downCallbackCounter.NotZero, 3*time.Second, 0) {
		t.Error("timeout waiting for channel to go down (cb)")
	}

	// Note: EventClosed is only expected when we cleanly close the channel.
	// We would have got one for the connection though... but have not used a procStatusEvents for that.

	if !ConditionWait(ctx, func() bool { return chCounters.Up.Value() > upCounterBefore }, 30*time.Second, 0) {
		t.Error("expecting Up count to increase")
	}
	if !ConditionWait(ctx, func() bool { return upCallbackCounter.Value() > upCallbackCounterBefore }, 3*time.Second, 0) {
		t.Error("expecting Up count to increase (cb)")
	}

	// since we killed the connection and re-establishing usually takes a while,
	// we expect the channel recovery to fail initially... so an increasing counter
	if !ConditionWait(ctx, func() bool { return chCounters.Recovery.Value() > recoveryCountBefore }, 30*time.Second, 0) {
		t.Error("expecting Recovery count to increase")
	}
	if !ConditionWait(ctx, func() bool { return delayerCallbackCounter.Value() > delayerCallbackCounterBefore }, 30*time.Second, 0) {
		t.Error("expecting Recovery count to increase (cb)")
	}

	// test the queue name again
	if err := expectSingleQueue(rhClient, qName); err != nil {
		t.Error("rabbithole failed to list queue:", err)
	}
}
