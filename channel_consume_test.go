package grabbit

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/goleak"
)

// closePublisherNow retires the publisher immediately after preloading. Leaving
// an idle publisher channel alive across a shutdown or forced recovery would add
// an unrelated channel teardown to the scenario under test.
func closePublisherNow(t *testing.T, publisher *Publisher) {
	t.Helper()

	if err := publisher.Close(); err != nil {
		t.Errorf("publisher close: %v", err)
	}
	if !publisher.AwaitManager(false, LongPoll.Timeout) {
		t.Error("publisher manager did not exit after Close")
	}
}

// TestConcurrentConsumerRecoveryCompletion forces a connection loss while
// records are being processed. The worker pool must survive the generation
// change, every record must be observed at least once (duplicates tolerated),
// and the surviving generation must settle the queue rather than leave records
// unacknowledged.
func TestConcurrentConsumerRecoveryCompletion(t *testing.T) {
	const (
		QueueName      = "workload_consumer_concurrent_recovery"
		MSG_COUNT      = 120
		PREFETCH_COUNT = 4
		WORKERS        = 4
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 256)
	counters := &EventCounters{
		Up:            &SafeCounter{},
		Down:          &SafeCounter{},
		DataExhausted: &SafeCounter{},
		DataPartial:   &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.recovery"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}

	tracker := &concurrencyTracker{}
	registry := NewSafeRegisterMap()
	redelivered := &SafeCounter{}
	processed := &SafeCounter{}
	batchCalls := &SafeCounter{}
	slow := MsgHandlerSlow(processed, 10*time.Millisecond)

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithProcessorConcurrency(WORKERS)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.recovery"),
		WithChannelOptionTopology(topos),
		// Registered but must be ignored on the concurrent path.
		WithChannelOptionProcessor(func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
			batchCalls.Add(1)
		}),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			tracker.Enter()
			defer tracker.Exit()

			if message.Redelivered {
				redelivered.Add(1)
			}
			outcome := slow(props, message)
			registry.Set(string(message.Body), "record")
			return outcome
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}

	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.recovery.publisher"),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.concurrent.recovery"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	// Cut the link while records are demonstrably in flight.
	if !ConditionWait(ctx, registry.LenGreaterEquals(MSG_COUNT/3), LongVeryFrequentPoll) {
		t.Fatalf("timeout waiting for processing to start: %d/%d", registry.Length(), MSG_COUNT)
	}
	if err := rmqc.killConnections(conn.opt.name); err != nil {
		t.Fatalf("kill connections: %v", err)
	}

	if !conn.AwaitStatus(false, LongPoll.Timeout) {
		t.Fatal("connection should have gone DOWN")
	}
	if !consumer.AwaitStatus(false, LongPoll.Timeout) {
		t.Fatal("consumer should have gone DOWN")
	}
	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection should have gone back UP")
	}
	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("consumer should have gone back UP")
	}

	if !ConditionWait(ctx, registry.LenEquals(MSG_COUNT), LongPoll) {
		t.Fatalf("incomplete consumption after recovery: %d/%d", registry.Length(), MSG_COUNT)
	}
	if redelivered.Value() == 0 {
		t.Error("expected at least one redelivered record after the forced recovery")
	}
	if got := processed.Value(); got < MSG_COUNT {
		t.Errorf("record callbacks = %d, want at least %d", got, MSG_COUNT)
	}

	// The concurrent path has no batch fill and no flush timer. The direct
	// callback counter is the evidence that survives a full notifier buffer;
	// the event counters additionally pin the retired event kinds.
	if got := batchCalls.Value(); got != 0 {
		t.Errorf("batch callback ran %d times on the concurrent path, want 0", got)
	}
	if got := counters.DataExhausted.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataExhausted events, want 0", got)
	}
	if got := counters.DataPartial.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataPartial events, want 0", got)
	}

	// The surviving generation must have settled every record: no ready and
	// no unacknowledged messages remain.
	rmqc.requireQueueDrained(t, QueueName, LongPoll)

	if !awaitInFlightZero(ctx, tracker, ShortPoll) {
		t.Errorf("record callbacks still in flight after completion: %d", tracker.Current())
	}
	if max := tracker.Max(); max > WORKERS {
		t.Errorf("peak concurrency = %d, want <= %d", max, WORKERS)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}

// TestConcurrentConsumerShutdown covers both shutdown surfaces with workers in
// flight: an explicit Close and cancellation of the owning context. Neither may
// abandon a callback, leave the manager running, or start records afterwards.
func TestConcurrentConsumerShutdown(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		testConcurrentConsumerShutdown(t, "close", false)
	})
	t.Run("ContextCancel", func(t *testing.T) {
		testConcurrentConsumerShutdown(t, "ctx", true)
	})
}

func testConcurrentConsumerShutdown(t *testing.T, suffix string, viaContext bool) {
	const (
		MSG_COUNT      = 64
		PREFETCH_COUNT = 4
		WORKERS        = 4
	)
	QueueName := "workload_consumer_concurrent_shutdown_" + suffix

	// Attribute leaks to this scenario rather than to the suite verdict.
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chanCtx, chanCancel := context.WithCancel(ctx)
	defer chanCancel()

	statusCh := make(chan Event, 256)
	counters := &EventCounters{
		Up:   &SafeCounter{},
		Down: &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.shutdown."+suffix),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	// Publish before the consumer starts so deliveries are waiting as soon as
	// the workers come up.
	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.shutdown.publisher."+suffix),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.concurrent.shutdown."+suffix); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	tracker := &concurrencyTracker{}
	processed := &SafeCounter{}
	slow := MsgHandlerSlow(processed, 75*time.Millisecond)
	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithProcessorConcurrency(WORKERS)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.shutdown."+suffix),
		WithChannelOptionContext(chanCtx),
		WithChannelOptionTopology(topos),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			tracker.Enter()
			defer tracker.Exit()

			return slow(props, message)
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}
	if !ConditionWait(ctx, func() bool { return tracker.Current() >= 2 }, ShortPoll) {
		t.Fatalf("expected in-flight callbacks before shutdown, current=%d", tracker.Current())
	}

	if viaContext {
		chanCancel()
	} else if err := consumer.Close(); err != nil {
		t.Fatalf("consumer close: %v", err)
	}

	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after shutdown")
	}

	// The pool joins on callback return: in-flight work drains without the
	// shutdown call blocking on user code. The channel context is already
	// cancelled in the ContextCancel variant, so drain on a fresh deadline.
	if !awaitInFlightZero(context.Background(), tracker, LongPoll) {
		t.Fatalf("callbacks still in flight after shutdown: %d", tracker.Current())
	}

	// No worker may start a new record after the drain.
	settled := processed.Value()
	<-time.After(250 * time.Millisecond)
	if got := processed.Value(); got != settled {
		t.Errorf("callbacks continued after shutdown: %d -> %d", settled, got)
	}
	if max := tracker.Max(); max > WORKERS {
		t.Errorf("peak concurrency = %d, want <= %d", max, WORKERS)
	}
}

// TestConcurrentConsumerConcurrencyBound checks the advertised bound: manual
// acknowledgments are limited by min(workers, prefetch); auto-ack is limited by
// the worker pool alone, because the broker ignores prefetch for no-ack
// consumers.
func TestConcurrentConsumerConcurrencyBound(t *testing.T) {
	t.Run("ManualAckPrefetchBound", func(t *testing.T) {
		testConcurrentConsumerBound(t, "manual_prefetch", false, 8, 4, 4)
	})
	t.Run("ManualAckWorkerBound", func(t *testing.T) {
		testConcurrentConsumerBound(t, "manual_worker", false, 2, 8, 2)
	})
	t.Run("AutoAckWorkerBound", func(t *testing.T) {
		testConcurrentConsumerBound(t, "auto", true, 4, 1, 4)
	})
}

func testConcurrentConsumerBound(t *testing.T, suffix string, autoAck bool, workers, prefetch, expected int) {
	const MSG_COUNT = 64
	QueueName := "workload_consumer_concurrent_bound_" + suffix

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 128)
	go procStatusEvents(ctx, statusCh, &EventCounters{Up: &SafeCounter{}}, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.bound."+suffix),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.bound.publisher."+suffix),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.concurrent.bound."+suffix); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	tracker := &concurrencyTracker{}
	registry := NewSafeRegisterMap()
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(prefetch).
		WithProcessorConcurrency(workers).
		WithAutoAck(autoAck)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.bound."+suffix),
		WithChannelOptionTopology(topos),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			tracker.Enter()
			<-release
			tracker.Exit()

			registry.Set(string(message.Body), suffix)
			return RecordAck
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}

	reached := ConditionWait(ctx, func() bool { return tracker.Current() >= expected }, ShortPoll)
	unblock()
	if !reached {
		t.Fatalf("peak concurrency never reached %d (current=%d)", expected, tracker.Current())
	}

	if !ConditionWait(ctx, registry.LenEquals(MSG_COUNT), LongPoll) {
		t.Fatalf("incomplete consumption: %d/%d", registry.Length(), MSG_COUNT)
	}

	// The spec only promises an upper bound; the lower bound guards against a
	// sequential regression passing the ≤ assertion trivially.
	if max := tracker.Max(); max > expected {
		t.Errorf("peak concurrency = %d, want <= %d", max, expected)
	}
	if max := tracker.Max(); max < 2 {
		t.Errorf("peak concurrency = %d, want concurrent execution (>= 2)", max)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}

// TestConcurrentConsumerNoFlushEvents pins the retirement set on the concurrent
// path: with a queue that stays idle past several PrefetchTimeout windows, the
// channel raises neither EventDataExhausted nor EventDataPartial, and the batch
// callback is never invoked. The absence of a flush after an idle window is the
// observable proof that no timer is armed.
func TestConcurrentConsumerNoFlushEvents(t *testing.T) {
	const (
		QueueName        = "workload_consumer_concurrent_noflush"
		MSG_COUNT        = 5
		PREFETCH_COUNT   = 4
		WORKERS          = 2
		PREFETCH_TIMEOUT = 100 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 128)
	counters := &EventCounters{
		Up:            &SafeCounter{},
		DataExhausted: &SafeCounter{},
		DataPartial:   &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.noflush"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.noflush.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.concurrent.noflush"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	registry := NewSafeRegisterMap()
	batchCalls := &SafeCounter{}
	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithPrefetchTimeout(PREFETCH_TIMEOUT).
		WithProcessorConcurrency(WORKERS)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.noflush"),
		WithChannelOptionTopology(topos),
		// Registered but must be ignored on the concurrent path.
		WithChannelOptionProcessor(func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
			batchCalls.Add(1)
		}),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			registry.Set(string(message.Body), "record")
			return RecordAck
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}
	if !ConditionWait(ctx, registry.LenEquals(MSG_COUNT), LongPoll) {
		t.Fatalf("incomplete consumption: %d/%d", registry.Length(), MSG_COUNT)
	}

	// Stay idle well past several flush windows.
	<-time.After(3 * PREFETCH_TIMEOUT)
	if got := counters.DataExhausted.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataExhausted events, want 0", got)
	}
	if got := counters.DataPartial.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataPartial events, want 0", got)
	}
	if got := batchCalls.Value(); got != 0 {
		t.Errorf("batch callback ran %d times on the concurrent path, want 0", got)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}

// TestSequentialConsumerPartialFlush is the control for the concurrent
// retirement test: the sequential path must still raise EventDataPartial when
// fewer than PrefetchCount records arrive within the prefetch timeout.
func TestSequentialConsumerPartialFlush(t *testing.T) {
	const (
		QueueName        = "workload_consumer_sequential_partial"
		MSG_COUNT        = 2
		PREFETCH_COUNT   = 4
		PREFETCH_TIMEOUT = 100 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 128)
	counters := &EventCounters{
		Up:          &SafeCounter{},
		DataPartial: &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.sequential.partial"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.sequential.partial.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "tag.sequential.partial"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	batchCalls := &SafeCounter{}
	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithPrefetchTimeout(PREFETCH_TIMEOUT)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.sequential.partial"),
		WithChannelOptionTopology(topos),
		WithChannelOptionProcessor(MsgHandlerCount(batchCalls)),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("sequential consumer not ready")
	}
	if !ConditionWait(ctx, counters.DataPartial.GreaterEquals(1), LongPoll) {
		t.Fatalf("sequential path did not raise EventDataPartial (batch calls=%d)", batchCalls.Value())
	}
	if !ConditionWait(ctx, batchCalls.GreaterEquals(1), ShortPoll) {
		t.Fatal("sequential partial batch was not processed")
	}
	rmqc.requireQueueDrained(t, QueueName, LongPoll)

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}

// TestConcurrentConsumerPoisonRequeue verifies that a record which keeps
// requeueing loops finitely once the callback stops asking for redelivery. The
// fixed worker pool must absorb every attempt without spawning per-attempt
// goroutines.
func TestConcurrentConsumerPoisonRequeue(t *testing.T) {
	const (
		QueueName      = "workload_consumer_concurrent_poison"
		PREFETCH_COUNT = 1
		WORKERS        = 1
		MAX_TRIES      = 10
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 128)
	counters := &EventCounters{Up: &SafeCounter{}}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.poison"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(1)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.poison.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, 1, "exch.concurrent.poison"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	tracker := &concurrencyTracker{}
	attempts := &SafeCounter{}
	registry := NewSafeRegisterMap()
	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithProcessorConcurrency(WORKERS)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.poison"),
		WithChannelOptionTopology(topos),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			tracker.Enter()
			defer tracker.Exit()

			attempts.Add(1)
			if attempts.Value() < MAX_TRIES {
				<-time.After(10 * time.Millisecond)
				return RecordNackRequeue
			}
			registry.Set(string(message.Body), "record")
			return RecordAck
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}

	baselineGoroutines := runtime.NumGoroutine()

	if !ConditionWait(ctx, registry.LenEquals(1), LongPoll) {
		t.Fatalf("poison record was not settled: attempts=%d", attempts.Value())
	}
	if got := attempts.Value(); got != MAX_TRIES {
		t.Errorf("poison record attempts = %d, want %d", got, MAX_TRIES)
	}
	rmqc.requireQueueDrained(t, QueueName, LongPoll)

	// Every attempt ran on the one worker; no goroutine was created per retry.
	if max := tracker.Max(); max != WORKERS {
		t.Errorf("peak concurrency = %d, want %d", max, WORKERS)
	}
	if !awaitInFlightZero(ctx, tracker, ShortPoll) {
		t.Errorf("callbacks still in flight: %d", tracker.Current())
	}
	<-time.After(250 * time.Millisecond)
	// A per-retry leak would add MAX_TRIES goroutines; the small tolerance only
	// absorbs runtime/broker jitter.
	if after := runtime.NumGoroutine(); after > baselineGoroutines+3 {
		t.Errorf("goroutine count grew across %d poison retries: %d -> %d", MAX_TRIES, baselineGoroutines, after)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}

// TestConcurrentConsumerGenerationOverlap deliberately holds the first
// generation's callbacks open across a forced recovery. The new generation must
// be allowed to start while the old workers are still finishing, producing the
// documented transient of up to 2N callbacks; both generations must then join
// cleanly.
func TestConcurrentConsumerGenerationOverlap(t *testing.T) {
	const (
		QueueName      = "workload_consumer_concurrent_overlap"
		MSG_COUNT      = 64
		PREFETCH_COUNT = 4
		WORKERS        = 4
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statusCh := make(chan Event, 256)
	counters := &EventCounters{
		Up:            &SafeCounter{},
		Down:          &SafeCounter{},
		DataExhausted: &SafeCounter{},
		DataPartial:   &SafeCounter{},
	}
	go procStatusEvents(ctx, statusCh, counters, nil)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctx),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.concurrent.overlap"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection not ready")
	}

	topos := []*TopologyOptions{{
		Name:          QueueName,
		Durable:       true,
		Declare:       true,
		IsDestination: true,
	}}
	optPub := DefaultPublisherOptions()
	optPub.WithKey(QueueName).WithContext(ctx).WithConfirmationsCount(MSG_COUNT)
	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.concurrent.overlap.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(nil),
	)
	defer publisher.Close()
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.concurrent.overlap"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	closePublisherNow(t, publisher)

	tracker := &concurrencyTracker{}
	registry := NewSafeRegisterMap()
	batchCalls := &SafeCounter{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchCount(PREFETCH_COUNT).
		WithProcessorConcurrency(WORKERS)

	consumer := NewConsumer(conn, optConsumer,
		WithChannelOptionName("chan.concurrent.overlap"),
		WithChannelOptionTopology(topos),
		// Registered but must be ignored on the concurrent path.
		WithChannelOptionProcessor(func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
			batchCalls.Add(1)
		}),
		WithChannelOptionProcessRecord(func(props *DeliveriesProperties, message DeliveryData) RecordOutcome {
			tracker.Enter()
			defer tracker.Exit()

			registry.Set(string(message.Body), "record")
			<-release
			return RecordAck
		}),
	)
	defer func() {
		_, _ = consumer.Channel().QueueDelete(QueueName, false, false, true)
		_ = consumer.Close()
	}()

	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("concurrent consumer not ready")
	}

	// Hold the first generation's full worker pool open.
	if !ConditionWait(ctx, func() bool { return tracker.Current() >= WORKERS }, ShortPoll) {
		t.Fatalf("first generation did not fill the pool: current=%d", tracker.Current())
	}
	if err := rmqc.killConnections(conn.opt.name); err != nil {
		t.Fatalf("kill connections: %v", err)
	}

	if !conn.AwaitStatus(false, LongPoll.Timeout) {
		t.Fatal("connection should have gone DOWN")
	}
	if !consumer.AwaitStatus(false, LongPoll.Timeout) {
		t.Fatal("consumer should have gone DOWN")
	}
	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("connection should have gone back UP")
	}
	if !consumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("consumer should have gone back UP")
	}

	// The old workers are still blocked in user code; the recovered generation
	// must now pull the redelivered records concurrently with them.
	if !ConditionWait(ctx, func() bool { return tracker.Current() > WORKERS }, LongPoll) {
		t.Fatalf("no generation overlap observed: current=%d, want > %d", tracker.Current(), WORKERS)
	}

	unblock()
	if !ConditionWait(ctx, registry.LenEquals(MSG_COUNT), LongPoll) {
		t.Fatalf("incomplete consumption after overlap: %d/%d", registry.Length(), MSG_COUNT)
	}
	rmqc.requireQueueDrained(t, QueueName, LongPoll)
	if !awaitInFlightZero(ctx, tracker, LongPoll) {
		t.Errorf("callbacks still in flight after overlap: %d", tracker.Current())
	}

	if max := tracker.Max(); max <= WORKERS || max > 2*WORKERS {
		t.Errorf("peak concurrency = %d, want in (%d, %d]", max, WORKERS, 2*WORKERS)
	}
	if got := batchCalls.Value(); got != 0 {
		t.Errorf("batch callback ran %d times on the concurrent path, want 0", got)
	}
	if got := counters.DataExhausted.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataExhausted events, want 0", got)
	}
	if got := counters.DataPartial.Value(); got != 0 {
		t.Errorf("concurrent path raised %d EventDataPartial events, want 0", got)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("consumer close: %v", err)
	}
	if !consumer.AwaitManager(false, LongPoll.Timeout) {
		t.Error("consumer manager did not exit after Close")
	}
}
