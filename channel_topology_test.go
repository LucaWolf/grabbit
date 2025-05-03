package grabbit

import (
	"context"
	"fmt"
	"testing"
	"time"

	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func expectQueue(cli *rabbithole.Client, name string) error {
	qs, err := cli.ListQueues()
	if err != nil {
		return err
	}
	for _, q := range qs {
		if q.Name == name {
			return nil
		}
	}
	return fmt.Errorf("queue %s not found", name)
}

func expectExchange(cli *rabbithole.Client, name string) error {
	es, err := cli.ListExchanges()
	if err != nil {
		return err
	}
	for _, e := range es {
		if e.Name == name {
			return nil
		}
	}
	return fmt.Errorf("exchange %s not found", name)
}

// TestChannelTopology tests that topologies are re-created after the current channel is recovered
func TestChannelTopology(t *testing.T) {
	const KEY_ALERTS = "key.pagers"                         // routing key into pagers queue
	const KEY_EMAILS = "key.emails"                         // routing key into emails queue
	const EXCHANGE_NOTIFICATIONS = "exchange.notifications" // direct key dispatch exchange
	const QUEUE_PAGERS = "queue.pagers"                     // alerts deposit for alert routed messages
	const QUEUE_EMAILS = "queue.emails"                     // emails deposit for info routed messages
	const DURABLE = false                                   // we want to test if recreated after recovery

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

	topos := make([]*TopologyOptions, 0, 4)
	// create an ephemeral 'logs' exchange
	topos = append(topos, &TopologyOptions{
		Name:          EXCHANGE_NOTIFICATIONS,
		Declare:       true,
		IsExchange:    true,
		IsDestination: false,
		Durable:       DURABLE,
		Kind:          "direct",
	})
	// create an ephemeral 'pagers' queue, bound to 'logs' exchange and route key 'alert'
	topos = append(topos, &TopologyOptions{
		Name:          QUEUE_PAGERS,
		Declare:       true,
		Durable:       DURABLE,
		IsDestination: true,
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_NOTIFICATIONS,
			Key:     KEY_ALERTS,
		},
	})
	topos = append(topos, &TopologyOptions{
		Name:          QUEUE_EMAILS,
		IsDestination: true,
		Durable:       DURABLE,
		Declare:       true,
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_NOTIFICATIONS,
			Key:     KEY_EMAILS,
		},
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
	defer testCh.ExchangeDelete(EXCHANGE_NOTIFICATIONS, false, true)
	defer testCh.QueueDelete(QUEUE_EMAILS, false, false, true)
	defer testCh.QueueDelete(QUEUE_PAGERS, false, false, true)

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

	// general API calls for coverage
	baseCh := testCh.Channel()
	if !baseCh.IsSet() {
		t.Error("base channel's super is not set")
	}

	baseCh.RLock()
	if baseCh.mu.TryLock() {
		t.Error("base channel should be read locked")
	}
	baseCh.RUnlock()
	baseCh.Lock()
	if baseCh.mu.TryLock() {
		t.Error("base channel should be locked")
	}
	baseCh.Unlock()

	if testCh.IsClosed() {
		t.Error("channel should not be closed")
	}
	if testCh.IsPaused() {
		t.Error("channel should not be paused")
	}

	// test super access; in real life you'd also lock when using amqp channel/connection. AVOID!
	super := baseCh.Super()
	_, err := super.QueueDeclarePassive(QUEUE_EMAILS, DURABLE, false, false, false, nil)
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

	// initial setup - ephemeral exchanges and queues
	if err := expectQueue(rhClient, QUEUE_EMAILS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_EMAILS)
	}
	if err := expectQueue(rhClient, QUEUE_PAGERS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_PAGERS)
	}
	if err := expectExchange(rhClient, EXCHANGE_NOTIFICATIONS); err != nil {
		t.Error("rabbithole failed to list exchange:", err, EXCHANGE_NOTIFICATIONS)
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

	// fianal setup
	if err := expectQueue(rhClient, QUEUE_EMAILS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_EMAILS)
	}
	if err := expectQueue(rhClient, QUEUE_PAGERS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_PAGERS)
	}
	if err := expectExchange(rhClient, EXCHANGE_NOTIFICATIONS); err != nil {
		t.Error("rabbithole failed to list exchange:", err, EXCHANGE_NOTIFICATIONS)
	}
}
