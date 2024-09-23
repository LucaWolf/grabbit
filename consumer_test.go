package grabbit

import (
	"context"
	"testing"
	"time"

	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// HandleMsgRegistry accumulates in the registry and tests the uniqueness of the ACK-ed messages (delivered via 'ch')
func HandleMsgRegistry(
	ctx context.Context,
	registry *SafeRegisterMap,
	ch chan DeliveryPayload,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, chAlive := <-ch:
			if !chAlive {
				return
			}
			if registry.Has(string(m)) {
				// !!!NOTE!!! this keeps happening. Perhaps due to in-flight during the cut?
				// log.Printf("\033[91mWARNING\033[0m duplicate; msg [%s] already ACK-ed!\n", string(m))
			} else {
				registry.Set(string(m))
			}
		}
	}
}

func RegisterSlice(messages []DeliveryData, ch chan DeliveryPayload) {
	bodies := make([][]byte, len(messages))
	for n, msg := range messages {
		bodies[n] = msg.Body
		ch <- msg.Body
	}
}

func MsgHandler(r *SafeRand, chReg chan DeliveryPayload) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		const FULL_BATCH_ACK_THRESHOLD = 5

		if !mustAck {
			return
		}
		idxPivot := 2 * len(messages) / 3
		idxLast := len(messages) - 1
		pivotDeliveryTag := messages[idxPivot].DeliveryTag
		lastDeliveryTag := messages[idxLast].DeliveryTag

		if len(messages) < FULL_BATCH_ACK_THRESHOLD {
			RegisterSlice(messages, chReg)
			ch.Ack(lastDeliveryTag, true)
		} else {
			// select one of the halves for Ack-ing and re-enqueue the other
			if r.Int()%2 == 0 {
				RegisterSlice(messages[:idxPivot+1], chReg)
				ch.Ack(pivotDeliveryTag, true)
				ch.Nack(lastDeliveryTag, true, true)
			} else {
				RegisterSlice(messages[idxPivot+1:], chReg)
				ch.Nack(pivotDeliveryTag, true, true)
				ch.Ack(lastDeliveryTag, true)
			}
		}
	}
}

// TestBatchConsumer evaluates if several processing repeats over a broken connection
// successfully reads all messages, eventually. For this scenario, large batches are split into two parts: one ACKed
// and the other rejected, randomly selected for ACK-ing. Over several attempts,
// the chunk size is reduced sufficiently (ref.FULL_BATCH_ACK_THRESHOLD) for the whole batch to be ACKed.
func TestBatchConsumer(t *testing.T) {
	QueueName := "workload"
	const MSG_COUNT = 2345

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel() // 'goleak' would complain w/out final clean-up

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionName("conn.main"),
	)

	// massive publishing causes a lot of events. Mutex flipping is not the fastest :-)
	statusChCap := 2 * MSG_COUNT / 3
	statusCh := make(chan Event, statusChCap)
	// capture only what we're interested in
	evtCounters := &EventCounters{
		DataExhausted: &SafeCounter{}, // marks the consuming completion
		MsgPublished:  &SafeCounter{}, // so we know when publishing is done
	}
	go procStatusEvents(ctxMaster, statusCh, evtCounters, nil)

	opt := DefaultPublisherOptions()
	opt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(20)

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
		// Note: use the defaultNotifyPublish to activate EventMessagePublished events we need for this test
	)

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}
	if _, err := PublishMsgBulk(publisher, opt, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster,
		func() bool { return evtCounters.MsgPublished.Value() == MSG_COUNT },
		15*time.Second, time.Second) {
		t.Error("timeout pushing all the messages", evtCounters.MsgPublished.Value())
	}

	validate := make(chan DeliveryPayload, 300)

	batchSelector := NewSafeRand(763482)
	registry := NewSafeRegisterMap()
	go HandleMsgRegistry(ctxMaster, registry, validate)

	optConsumer := DefaultConsumerOptions()
	optConsumer.WithQueue(QueueName).WithPrefetchTimeout(7 * time.Second)

	// start many consumers feeding events into the same pot
	type ConsumerAttr struct {
		Name          string
		PrefetchCount int
	}
	consumers := []ConsumerAttr{
		{"consumer.one", 8},
		{"consumer.two", 3},
		{"consumer.three", 4},
		{"consumer.four", 7},
		{"consumer.five", 13},
		{"consumer.six", 19},
		{"consumer.seven", 21},
	}
	for _, consumer := range consumers {
		_ = NewConsumer(conn, *optConsumer.WithPrefetchCount(consumer.PrefetchCount).WithName(consumer.Name),
			WithChannelOptionName("chan."+consumer.Name),
			WithChannelOptionProcessor(MsgHandler(batchSelector, validate)),
			WithChannelOptionNotification(statusCh),
		)
	}

	// we want consumers to be able to recover ...
	rhClient, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
	if err != nil {
		t.Error("rabbithole controller unavailable")
	}
	xs, _ := rhClient.ListConnections()
	// therefore sever the link some way through consuming or some timer
	ConditionWait(
		ctxMaster,
		func() bool { return registry.Length() >= MSG_COUNT/4 },
		5*time.Second, 30*time.Millisecond,
	)
	for _, x := range xs {
		if _, err := rhClient.CloseConnection(x.Name); err != nil {
			t.Error("rabbithole failed to close connection", err, " for: ", x.Name)
		}
	}

	// ... and get all messages consumed (eventually)
	if !ConditionWait(ctxMaster,
		func() bool { return evtCounters.DataExhausted.Value() >= len(consumers) },
		30*time.Second, time.Second) {
		t.Error("timeout waiting for all consumers to exhaust the queue", evtCounters.DataExhausted.Value())
	}
	regLen := registry.Length()
	if regLen != MSG_COUNT {
		t.Errorf("incomplete consumption: expected %d vs. read %d", MSG_COUNT, regLen)
	}
}
