// Command per_record demonstrates grabbit's per-record concurrent consumer
// dispatch. It publishes a small workload, consumes it with a bounded record
// worker pool, and prints the outcome of every record plus a final summary.
//
// JSON logging uses log/slog. grabbit itself performs no logging: it raises events
// and callbacks, and this demo bridges them into the application's logger.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	grabbit "github.com/LucaWolf/grabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	brokerURL    = "amqp://guest:guest@127.0.0.1:5672/"
	queueName    = "workload.per_record"
	messageCount = 24
	prefetch     = 8
	workers      = 8
	recordDelay  = 200 * time.Millisecond
	runTimeout   = 60 * time.Second
)

// slog has DEBUG, INFO, WARN, and ERROR built in. TRACE is a custom level one
// step below DEBUG, labelled "TRACE" by replaceLevel below.
const levelTrace slog.Level = slog.LevelDebug - 4

func replaceLevel(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.LevelKey {
		if level, ok := a.Value.Any().(slog.Level); ok && level <= levelTrace {
			a.Value = slog.StringValue("TRACE")
		}
	}
	return a
}

func logTrace(msg string, args ...any) {
	slog.Log(context.Background(), levelTrace, msg, args...)
}

// eventLevel maps a grabbit lifecycle event to a slog level. The library never
// logs; it raises events, and the application decides how to record them.
func eventLevel(event grabbit.Event) slog.Level {
	switch event.Kind {
	case grabbit.EventConfirm, grabbit.EventQos, grabbit.EventConsume:
		return slog.LevelError
	case grabbit.EventDefineTopology:
		if event.Err.IsSet() {
			return slog.LevelError
		}
		return slog.LevelDebug
	case grabbit.EventCannotEstablish, grabbit.EventBlocked,
		grabbit.EventMessageReturned:
		return slog.LevelWarn
	case grabbit.EventDown, grabbit.EventClosed,
		grabbit.EventMessagePublished, grabbit.EventMessageReceived:
		if event.Err.IsSet() {
			return slog.LevelWarn
		}
		return slog.LevelDebug
	default:
		return slog.LevelDebug
	}
}

func eventAttrs(event grabbit.Event) []any {
	attrs := []any{
		"source_type", event.SourceType.String(),
		"source", event.SourceName,
		"kind", event.Kind.String(),
	}
	if event.TargetName != "" {
		attrs = append(attrs, "target", event.TargetName)
	}
	if event.Err.IsSet() {
		attrs = append(attrs, "error", event.Err.Error())
	}
	return attrs
}

// outcomeStats tracks the callback attempts and terminal outcomes. It is the
// demo's proof that the worker pool overlaps records and that every message
// reaches a terminal outcome (ack or drop).
type outcomeStats struct {
	mu               sync.Mutex
	attempts         int
	acks             int
	requeues         int
	drops            int
	terminalOutcomes int
	current          int
	peak             int
	terminalByIndex  map[int]bool
}

type outcomeSnapshot struct {
	attempts         int
	acks             int
	requeues         int
	drops            int
	terminalOutcomes int
	peak             int
}

func newOutcomeStats() *outcomeStats {
	return &outcomeStats{terminalByIndex: make(map[int]bool)}
}

// begin records a callback starting and returns the current overlap.
func (s *outcomeStats) begin() (inFlight int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attempts++
	s.current++
	if s.current > s.peak {
		s.peak = s.current
	}
	return s.current
}

func (s *outcomeStats) exit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.current--
}

// trackOutcome accounts for one outcome and reports whether every message has
// now reached a terminal (ack or drop) outcome.
func (s *outcomeStats) trackOutcome(idx int, outcome grabbit.RecordOutcome) (terminalOutcomes int, complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch outcome {
	case grabbit.RecordAck:
		s.acks++
	case grabbit.RecordNackRequeue:
		s.requeues++
	default:
		s.drops++
	}

	if outcome != grabbit.RecordNackRequeue && !s.terminalByIndex[idx] {
		s.terminalByIndex[idx] = true
		s.terminalOutcomes++
	}
	return s.terminalOutcomes, s.terminalOutcomes == messageCount
}

func (s *outcomeStats) snapshot() outcomeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return outcomeSnapshot{
		attempts:         s.attempts,
		acks:             s.acks,
		requeues:         s.requeues,
		drops:            s.drops,
		terminalOutcomes: s.terminalOutcomes,
		peak:             s.peak,
	}
}

// recordProcessor exercises all three outcomes:
//
//	idx % 3 == 0 -> ack
//	idx % 3 == 1 -> requeue on the first delivery, drop on the redelivery
//	idx % 3 == 2 -> drop (a poison record)
//
// Every branch sleeps, so the in-flight count shows the worker overlap.
func recordProcessor(stats *outcomeStats, done chan<- struct{}) grabbit.CallbackProcessRecord {
	return func(props *grabbit.DeliveriesProperties, message grabbit.DeliveryData) grabbit.RecordOutcome {
		idx, err := strconv.Atoi(message.MessageId)
		if err != nil {
			slog.Warn("record has a non-numeric MessageId; treating it as poison",
				"record", message.MessageId)
			idx = -1
		}

		inFlight := stats.begin()
		defer stats.exit()

		<-time.After(recordDelay)

		var (
			outcome grabbit.RecordOutcome
			reason  string
		)
		switch idx % 3 {
		case 0:
			outcome = grabbit.RecordAck
			reason = "normal completion"
		case 1:
			if message.Redelivered {
				outcome = grabbit.RecordNackDrop
				reason = "requeued once; dropping to stop the loop"
			} else {
				outcome = grabbit.RecordNackRequeue
				reason = "transient failure; requeue"
			}
		default:
			outcome = grabbit.RecordNackDrop
			reason = "poison record; drop"
		}

		terminal, complete := stats.trackOutcome(idx, outcome)
		logTrace("record processed",
			"consumer", props.ConsumerTag,
			"record", message.MessageId,
			"delivery_tag", message.DeliveryTag,
			"reason", reason,
			"redelivered", message.Redelivered,
			"in_flight", inFlight,
			"outcome", outcome.String(),
			"terminal", terminal,
			"total", messageCount,
		)
		if complete {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return outcome
	}
}

func publishMessages(publisher *grabbit.Publisher) error {
	for i := 0; i < messageCount; i++ {
		message := amqp.Publishing{
			MessageId: strconv.Itoa(i),
			Body:      fmt.Appendf(nil, "record-%03d", i),
		}
		confirmation, err := publisher.PublishDeferredConfirm(message)
		if err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
		result := publisher.AwaitDeferredConfirmation(confirmation, 5*time.Second)
		if result.Outcome != grabbit.ConfirmationACK {
			return fmt.Errorf("publish %d not confirmed: %s", i, result.Outcome)
		}
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level:       levelTrace,
		ReplaceAttr: replaceLevel,
	})))
	slog.Info("demo starting",
		"broker", brokerURL,
		"queue", queueName,
		"messages", messageCount,
		"workers", workers,
	)

	events := make(chan grabbit.Event, 32)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-events:
				slog.Log(ctx, eventLevel(event), "grabbit event", eventAttrs(event)...)
			}
		}
	}()

	conn := grabbit.NewConnection(
		brokerURL, amqp.Config{},
		grabbit.WithConnectionOptionContext(ctx),
		grabbit.WithConnectionOptionName("conn.per_record"),
		grabbit.WithConnectionOptionNotification(events),
	)
	defer func() {
		_ = conn.Close()
		_ = conn.AwaitManager(false, 5*time.Second)
	}()

	topos := []*grabbit.TopologyOptions{{
		Name:          queueName,
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	}}

	optPublisher := grabbit.DefaultPublisherOptions()
	optPublisher.WithKey(queueName).WithContext(ctx).WithConfirmationsCount(messageCount)
	publisher := grabbit.NewPublisher(conn, optPublisher,
		grabbit.WithChannelOptionName("chan.per_record.publisher"),
		grabbit.WithChannelOptionTopology(topos),
	)
	if !publisher.AwaitStatus(true, 30*time.Second) {
		slog.Error("publisher not ready")
		cancel()
		return
	}
	// Keep reruns deterministic: no messages left over from a previous run.
	if purged, err := publisher.Channel().QueuePurge(queueName, false); err != nil {
		slog.Error("queue purge failed", "error", err)
	} else if purged != 0 {
		logTrace("purged stale records", "count", purged)
	}
	if err := publishMessages(publisher); err != nil {
		slog.Error("publish failed", "error", err)
		cancel()
		return
	}
	slog.Info("published records", "count", messageCount, "queue", queueName)
	if err := publisher.Close(); err != nil {
		slog.Warn("publisher close failed", "error", err)
	}
	_ = publisher.AwaitManager(false, 5*time.Second)

	stats := newOutcomeStats()
	done := make(chan struct{}, 1)

	optConsumer := grabbit.DefaultConsumerOptions()
	optConsumer.
		WithName("consumer.per_record").
		WithQueue(queueName).
		WithPrefetchCount(prefetch).
		WithProcessorConcurrency(workers)
	consumer := grabbit.NewConsumer(conn, optConsumer,
		grabbit.WithChannelOptionName("chan.per_record"),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionProcessRecord(recordProcessor(stats, done)),
	)
	defer func() {
		_ = consumer.Close()
		_ = consumer.AwaitManager(false, 5*time.Second)
	}()
	if !consumer.AwaitStatus(true, 30*time.Second) {
		slog.Error("consumer not ready")
		cancel()
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	slog.Info("consuming records",
		"count", messageCount,
		"workers", workers,
		"delay", recordDelay,
	)

	exit := "all records terminal"
	select {
	case <-done:
		// Let the last outcomes reach the wire before reporting.
		<-time.After(500 * time.Millisecond)
	case sig := <-sigs:
		exit = "signal: " + sig.String()
	case <-time.After(runTimeout):
		exit = "timeout"
	}

	summary := stats.snapshot()
	summaryAttrs := []any{
		"exit", exit,
		"attempts", summary.attempts,
		"acks", summary.acks,
		"requeues", summary.requeues,
		"drops", summary.drops,
		"terminal", summary.terminalOutcomes,
		"total", messageCount,
		"peak_overlap", summary.peak,
	}
	if summary.terminalOutcomes == messageCount {
		slog.Info("demo complete", summaryAttrs...)
	} else {
		slog.Error("demo incomplete", summaryAttrs...)
	}
	slog.Info("exit")
}
