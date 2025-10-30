package grabbit

import (
	"context"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

	// has no notifications ch consumer.
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
		Up:          &SafeCounter{},
		Down:        &SafeCounter{},
		Closed:      &SafeCounter{},
		BadRecovery: &SafeCounter{},
		Topology:    &SafeCounter{},
	}
	// do not count connection statistics further down this test in validations
	// all counters are channels related only
	go procStatusEvents(ctx, statusCh, chCounters, nil)

	// we get the EventUp only after con  *and* topologies are done,
	// so it may take a bit longer than minimal delay
	chanDelayer := tracingDelayer{Value: DefaultPoll.Timeout}
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
		WithChannelOptionDelay(chanDelayer),
	)
	defer testCh.ExchangeDelete(EXCHANGE_NOTIFICATIONS, false, true)
	defer testCh.QueueDelete(QUEUE_EMAILS, false, false, true)
	defer testCh.QueueDelete(QUEUE_PAGERS, false, false, true)

	// current version of channel recovery awaits connection's signaling completion before
	// redoing the RMQ channel, subject to its delayer
	// So no longer expecting EventCannotEstablish
	// nonetheless, various callbacks still execute.
	if !ConditionWait(ctx, recoveringCallbackCounter.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for channel to setup/recovery event")
	}

	// channel is up
	if !ConditionWait(ctx, chCounters.Up.ValueEquals(1), DefaultPoll) {
		t.Fatal("timeout waiting for channel to be ready", chCounters.Up.Value())
	}
	if !ConditionWait(ctx, upCallbackCounter.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for channel to be ready (cb)")
	}

	// there should be no unrequested hiccups on the channel
	if ConditionWait(ctx, evtAny(chCounters.Down.NotZero, chCounters.Closed.NotZero), ShortPoll) {
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

	// initial setup - ephemeral exchanges and queues
	if err := rmqc.expectQueue(QUEUE_EMAILS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_EMAILS)
	}
	if err := rmqc.expectQueue(QUEUE_PAGERS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_PAGERS)
	}
	if err := rmqc.expectExchange(EXCHANGE_NOTIFICATIONS); err != nil {
		t.Error("rabbithole failed to list exchange:", err, EXCHANGE_NOTIFICATIONS)
	}

	// Forcefully close test specific connection
	upCounterBefore := chCounters.Up.Value()
	upCallbackCounterBefore := upCallbackCounter.Value()
	badRecoveryCountBefore := chCounters.BadRecovery.Value()
	delayerCallbackCounterBefore := delayerCallbackCounter.Value()
	topologyCountBefore := chCounters.Topology.Value()

	if err := rmqc.killConnections(); err != nil {
		t.Error(err)
	}
	tConnectionKill := time.Now()

	// test the grabbit connection and queue have recovered after a while
	if !ConditionWait(ctx, chCounters.Down.ValueEquals(1), ShortPoll) {
		t.Error("timeout waiting for channel to go down")
	}

	log.Println("INFO: channel reported DOWN after", time.Since(tConnectionKill))

	if !ConditionWait(ctx, downCallbackCounter.NotZero, ShortPoll) {
		t.Error("timeout waiting for channel to go down (cb)")
	}

	// Note: EventClosed is only expected when we cleanly close the channel.
	// We would have got one for the connection though... but have not used a procStatusEvents for that.

	if !ConditionWait(ctx, chCounters.Up.Greater(upCounterBefore), DefaultPoll) {
		t.Error("expecting Up count to increase")
	}
	recoveryDelay := time.Since(tConnectionKill)
	log.Println("INFO: channel reported UP after", recoveryDelay)
	if !ConditionWait(ctx, upCallbackCounter.Greater(upCallbackCounterBefore), ShortPoll) {
		t.Error("expecting Up count to increase (cb)")
	}

	// channel recovery awaits connection's signaling completion before redoing the RMQ channel
	// Nonetheless EventCannotEstablish can happen if delayer is exceed by the whole recovery
	// which is now: conn UP + chann UP + toplogies redeclared
	if recoveryDelay > chanDelayer.Value {
		log.Println("Channel took too long to recover on this run!")
		if !ConditionWait(ctx, chCounters.BadRecovery.Greater(badRecoveryCountBefore), ShortPoll) {
			t.Error("expecting failed recovery count to increase")
		}
	} else {
		if ConditionWait(ctx, chCounters.BadRecovery.Greater(badRecoveryCountBefore), ShortPoll) {
			t.Error("expecting failed recovery count to maintain")
		}
	}

	if !ConditionWait(ctx, delayerCallbackCounter.Greater(delayerCallbackCounterBefore), DefaultPoll) {
		t.Error("expecting Recovery count to increase (cb)")
	}
	if !ConditionWait(ctx, chCounters.Topology.Greater(topologyCountBefore), DefaultPoll) {
		t.Error("expecting topolgy declaration count to increase")
	}

	// fianal setup
	if err := rmqc.expectQueue(QUEUE_EMAILS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_EMAILS)
	}
	if err := rmqc.expectQueue(QUEUE_PAGERS); err != nil {
		t.Error("rabbithole failed to list queue:", err, QUEUE_PAGERS)
	}
	if err := rmqc.expectExchange(EXCHANGE_NOTIFICATIONS); err != nil {
		t.Error("rabbithole failed to list exchange:", err, EXCHANGE_NOTIFICATIONS)
	}
}
