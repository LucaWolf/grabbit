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

func MsgHandlerBulk(r *SafeRand, chReg chan DeliveryPayload) CallbackProcessMessages {
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

// msgSequential returns true if messages are sequential (same QoS batch)
// func msgSequential(messages []DeliveryData) bool {
// 	for i := 1; i < len(messages); i++ {
// 		if messages[i].DeliveryTag != messages[i-1].DeliveryTag+1 {
// 			return false
// 		}
// 	}
// 	return true
// }

func MsgHandlerQoS(
	countQos, countAutoAck *SafeCounter,
	expectedQoS int,
	expectOrdered bool,
) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		if len(messages) == expectedQoS {
			// even though some consumers are Qos bound, there is no guarantee of ordering
			// hence 'msgSequential' is useless...cannot use expectOrdered
			countQos.Add(1)
		}

		if !mustAck {
			countAutoAck.Add(1)
			return
		}

		idxLast := len(messages) - 1
		lastDeliveryTag := messages[idxLast].DeliveryTag
		ch.Ack(lastDeliveryTag, true)
	}
}

// TestBatchConsumer evaluates if several processing repeats over a broken connection
// successfully reads all messages, eventually. For this scenario, large batches are split into two parts: one ACKed
// and the other rejected, randomly selected for ACK-ing. Over several attempts,
// the chunk size is reduced sufficiently (ref.FULL_BATCH_ACK_THRESHOLD) for the whole batch to be ACKed.
func TestBatchConsumer(t *testing.T) {
	QueueName := "workload"
	const MSG_COUNT = 2345
	chCapacity := 2 * MSG_COUNT / 3
	statusCh := make(chan Event, chCapacity)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel() // 'goleak' would complain w/out final clean-up

	eventCounters := &EventCounters{
		Up:            &SafeCounter{},
		DataExhausted: &SafeCounter{},
		MsgPublished:  &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, eventCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)
	// await connection which should have raised a series of events
	if !ConditionWait(ctxMaster, eventCounters.Up.NotZero, 40*time.Second, time.Second) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	// massive publishing causes a lot of events.
	opt := DefaultPublisherOptions()
	opt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(chCapacity)

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
	)

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}
	if _, err := PublishMsgBulk(publisher, opt, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster,
		func() bool { return eventCounters.MsgPublished.Value() == MSG_COUNT },
		15*time.Second, time.Second) {
		t.Error("timeout pushing all the messages", eventCounters.MsgPublished.Value())
	}
	// what to kill for testing consumers recovery
	rhClient, err := rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
	if err != nil {
		t.Error("rabbithole controller unavailable")
	}
	xs, _ := rhClient.ListConnections()

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
	consumersAttr := []ConsumerAttr{
		{"consumer.one", 8},
		{"consumer.two", 3},
		{"consumer.three", 4},
		{"consumer.four", 7},
		{"consumer.five", 13},
		{"consumer.six", 19},
		{"consumer.seven", 21},
	}
	consumers := make([]*Consumer, 0, len(consumersAttr))
	for _, consumerAttr := range consumersAttr {
		consumer := NewConsumer(conn, *optConsumer.WithPrefetchCount(consumerAttr.PrefetchCount).WithName(consumerAttr.Name),
			WithChannelOptionName("chan."+consumerAttr.Name),
			WithChannelOptionProcessor(MsgHandlerBulk(batchSelector, validate)),
			WithChannelOptionNotification(statusCh),
		)
		consumers = append(consumers, consumer)
	}

	// sever the link some way through consuming
	ConditionWait(
		ctxMaster,
		func() bool { return registry.Length() >= MSG_COUNT/4 },
		30*time.Second, 30*time.Millisecond,
	)
	for _, x := range xs {
		if _, err := rhClient.CloseConnection(x.Name); err != nil {
			t.Error("rabbithole failed to close connection", err, " for: ", x.Name)
		}
	}
	// just to cover the code path of AwaitAvailable
	for i, c := range consumers {
		if !c.AwaitAvailable(30*time.Second, 1*time.Second) {
			t.Fatalf("consumer [%s] not ready yet", consumersAttr[i].Name)
		}
	}

	// ... and get all messages consumed (eventually)
	if !ConditionWait(ctxMaster,
		func() bool { return eventCounters.DataExhausted.Value() >= len(consumersAttr) },
		30*time.Second, time.Second) {
		t.Error("timeout waiting for all consumers to exhaust the queue", eventCounters.DataExhausted.Value())
	}

	regLen := registry.Length()
	if regLen != MSG_COUNT {
		t.Errorf("incomplete consumption: expected %d vs. read %d", MSG_COUNT, regLen)
	}
}

func TestAutoAckAndQos(t *testing.T) {
	QueueName := "workload"
	const MSG_COUNT = 1024
	const CONSUMER_BATCH_SIZE = 5
	chCapacity := 2 * MSG_COUNT / 3
	statusCh := make(chan Event, chCapacity)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel()

	eventCounters := &EventCounters{
		Up:            &SafeCounter{},
		MsgPublished:  &SafeCounter{},
		DataExhausted: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, eventCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)
	// await connection which should have raised a series of events
	if !ConditionWait(ctxMaster, eventCounters.Up.NotZero, 40*time.Second, time.Second) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos, &TopologyOptions{
		Name:    QueueName,
		Durable: true,
		Declare: true,
	})

	// make a publisher + queues infra
	opt := DefaultPublisherOptions()
	opt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(chCapacity)

	publisher := NewPublisher(conn, opt,
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
	)
	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}

	// make two consumers, each with a different autoACK policy
	qosCounter := &SafeCounter{}
	countAutoAck := &SafeCounter{}

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchTimeout(3 * time.Second).
		WithPrefetchCount(CONSUMER_BATCH_SIZE)

	alphaConsumer := NewConsumer(conn,
		*optConsumer.WithAutoAck(false).WithQosGlobal(true),
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAutoAck, CONSUMER_BATCH_SIZE, true)),
	)
	betaConsumer := NewConsumer(conn,
		*optConsumer.WithAutoAck(true),
		WithChannelOptionName("chan.beta"),
		WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAutoAck, CONSUMER_BATCH_SIZE, false)),
	)
	// to ensure even distribution must have the consumers ready
	if !alphaConsumer.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("alphaConsumer not ready yet")
	}
	if !betaConsumer.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("betaConsumer not ready yet")
	}

	// push all the messages
	if _, err := PublishMsgBulk(publisher, opt, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster,
		func() bool { return eventCounters.MsgPublished.Value() == MSG_COUNT },
		15*time.Second, time.Second) {
		t.Error("timeout pushing all the messages", eventCounters.MsgPublished.Value())
	}

	// await full consumption
	if !ConditionWait(ctxMaster,
		func() bool { return eventCounters.DataExhausted.Value() >= 2 },
		30*time.Second, time.Second) {
		t.Error("timeout waiting for all consumers to exhaust the queue", eventCounters.DataExhausted.Value())
	}

	batchesCount := MSG_COUNT / CONSUMER_BATCH_SIZE

	// Validate the counters:
	// only beta bumps the countAutoAck and is so fast paced that it steals almost all work.
	coverageAutoAck := countAutoAck.Value() * 100 / batchesCount
	if coverageAutoAck < 80 || coverageAutoAck > 98 {
		t.Errorf("auto ack coverage: expected ~95%% vs.read %d%% (%d)", coverageAutoAck, countAutoAck.Value())
	}

	// tally-up the total batches count
	if qosCounter.Value() != batchesCount {
		t.Errorf("qosCounter: expected %d vs. read %d", batchesCount, qosCounter.Value())
	}
}
