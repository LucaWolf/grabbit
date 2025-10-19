package grabbit

import (
	"context"
	"reflect"
	"testing"
	"time"

	trace "traceutils"

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
	countQos, countAck *SafeCounter,
	expectedQoS int,
) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		if len(messages) == expectedQoS {
			countQos.Add(1)
		}
		if mustAck {
			idxLast := len(messages) - 1
			lastDeliveryTag := messages[idxLast].DeliveryTag
			ch.Ack(lastDeliveryTag, true)
			countAck.Add(1)
		}
	}
}

// TestBatchConsumer evaluates if several processing repeats over a broken connection
// successfully reads all messages, eventually. For this scenario, large batches are split into two parts: one ACKed
// and the other rejected, randomly selected for ACK-ing. Over several attempts,
// the chunk size is reduced sufficiently (ref.FULL_BATCH_ACK_THRESHOLD) for the whole batch to be ACKed.
func TestBatchConsumer(t *testing.T) {
	QueueName := "workload_consumer_batch"
	const MSG_COUNT = 2345
	chCapacity := 2 * MSG_COUNT / 3
	statusCh := make(chan Event, chCapacity)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel() // 'goleak' would complain w/out final clean-up

	chCounters := &EventCounters{
		Up:            &SafeCounter{},
		DataExhausted: &SafeCounter{},
		MsgPublished:  &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)
	// connection is up
	if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
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
	defer publisher.Channel().QueueDelete(QueueName, false, false, true)

	if !publisher.AwaitAvailable(LongPoll.Timeout, LongPoll.Frequency) {
		t.Fatal("publisher not ready yet")
	}
	if _, err := PublishMsgBulkOptions(publisher, opt, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster, chCounters.MsgPublished.ValueEquals(MSG_COUNT), LongPoll) {
		t.Error("timeout pushing all the messages", chCounters.MsgPublished.Value())
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
	ConditionWait(ctxMaster, func() bool { return registry.Length() >= MSG_COUNT/4 }, LongVeryFrequentPoll)
	if err := rmqc.killConnections(); err != nil {
		t.Error(err)
	}

	// just to cover the code path of AwaitAvailable
	for i, c := range consumers {
		if !c.AwaitAvailable(30*time.Second, 1*time.Second) {
			t.Fatalf("consumer [%s] not ready yet", consumersAttr[i].Name)
		}
	}

	// ... and get all messages consumed (eventually)
	if !ConditionWait(ctxMaster, chCounters.DataExhausted.GreaterEquals(len(consumersAttr)), LongPoll) {
		t.Error("timeout waiting for all consumers to exhaust the queue", chCounters.DataExhausted.Value())
	}

	regLen := registry.Length()
	if regLen != MSG_COUNT {
		t.Errorf("incomplete consumption: expected %d vs. read %d", MSG_COUNT, regLen)
	}
}

func TestConsumerExclusive(t *testing.T) {
	QueueName := "workload_consumer_exclusive"
	const MSG_COUNT = 256
	const CONSUMER_BATCH_SIZE = 5
	chCapacity := 2 * MSG_COUNT / 3
	statusCh := make(chan Event, chCapacity)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel()

	chCounters := &EventCounters{
		Up:            &SafeCounter{},
		MsgPublished:  &SafeCounter{},
		DataExhausted: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)
	// connection is up
	if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos,
		&TopologyOptions{
			Name:    QueueName,
			Durable: true,
			Declare: true,
		},
	)

	// make a publisher + queues infra
	optPub := DefaultPublisherOptions()
	optPub.
		WithKey(QueueName).
		WithContext(ctxMaster).
		WithConfirmationsCount(chCapacity)

	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionTopology(topos),
	)
	defer publisher.Channel().QueueDelete(QueueName, false, false, true)
	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}

	// make two consumers, each with a different autoACK policy
	qosCounter := &SafeCounter{}
	countAckAlpha := &SafeCounter{}
	countAckBeta := &SafeCounter{}

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchTimeout(2 * time.Second).
		WithPrefetchCount(CONSUMER_BATCH_SIZE)

	// all messages should go here
	alphaConsumer := NewConsumer(conn,
		*optConsumer.WithName("consumer.alpha").WithExclusive(true),
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAckAlpha, CONSUMER_BATCH_SIZE)),
	)
	if !alphaConsumer.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("alphaConsumer not ready yet")
	}
	// nothing for this guy on the same queue, different connection though
	// THIS NOT WORKING due to amqp dead-locks on the super channel "Consume()"
	// optConsumerBeta := DefaultConsumerOptions()
	// optConsumerBeta.
	// 	WithQueue(QueueName).
	// 	WithPrefetchTimeout(2 * time.Second).
	// 	WithPrefetchCount(CONSUMER_BATCH_SIZE).
	// 	WithName("consumer.beta").
	// 	WithExclusive(false)
	// // try with own connection, so far this blocked on same connection
	// betaConsumer := NewConsumer(conn,
	// 	optConsumerBeta,
	// 	WithChannelOptionName("chan.beta"),
	// 	WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAckBeta, CONSUMER_BATCH_SIZE)),
	// )
	// if !betaConsumer.AwaitAvailable(30*time.Second, 1*time.Second) {
	// 	t.Fatal("betaConsumer not ready yet")
	// }

	// push all the messages
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster, chCounters.MsgPublished.ValueEquals(MSG_COUNT), LongPoll) {
		t.Error("timeout pushing all the messages", chCounters.MsgPublished.Value())
	}

	// await full consumption
	if !ConditionWait(ctxMaster, chCounters.DataExhausted.GreaterEquals(2), LongPoll) {
		t.Error("timeout waiting for all consumers to exhaust the queue", chCounters.DataExhausted.Value())
	}

	// Validate the counters: only alpha  should bump the countAck
	batchesCount := MSG_COUNT / CONSUMER_BATCH_SIZE
	ceilAckCount := (MSG_COUNT + CONSUMER_BATCH_SIZE - 1) / CONSUMER_BATCH_SIZE
	if countAckAlpha.Value() != ceilAckCount {
		t.Errorf("Failed -> exclusive Ack: expected %d vs. read %d", ceilAckCount, countAckAlpha.Value())
	}
	if countAckBeta.Value() != 0 {
		t.Errorf("Failed -> excluded Ack: expected %d vs. read %d", 0, countAckBeta.Value())
	}
	// tally-up the total batches count
	if qosCounter.Value() != batchesCount {
		t.Errorf("qosCounter: expected %d vs. read %d", batchesCount, qosCounter.Value())
	}
}

func TestConsumerOptions(t *testing.T) {
	QueueName := "workload_consumer_options"
	MSG_COUNT := 30
	PREFETCH_COUNT := 4
	PREFETCH_SIZE := 0 // avoid using it, any non-zero will block
	CONSUMER_NAME := "punter"
	AUTO_ACK := false
	QOS_GLOBAL := true
	EXCLUSIVE := true
	NO_WAIT := true
	NO_LOCAL := true

	ARG := make(amqp.Table)
	ARG["foo"] = "bar"

	statusCh := make(chan Event, 16)

	validators := []trace.TraceValidator{
		{
			Name: "Consume",
			Tag:  CONSUMER_NAME,
			Expectations: []trace.FieldExpectation{
				{
					Field: "Queue",
					Value: reflect.ValueOf(QueueName),
				},
				{
					Field: "Consumer",
					Value: reflect.ValueOf(CONSUMER_NAME),
				},
				{
					Field: "AutoAck",
					Value: reflect.ValueOf(AUTO_ACK),
				},
				{
					Field: "Exclusive",
					Value: reflect.ValueOf(EXCLUSIVE),
				},
				{
					Field: "NoWait",
					Value: reflect.ValueOf(NO_WAIT),
				},
				{
					Field: "NoLocal",
					Value: reflect.ValueOf(NO_LOCAL),
				},
				{
					Field: "Args",
					Value: reflect.ValueOf(ARG),
				},
			},
		},
		{
			Name: "Qos",
			Expectations: []trace.FieldExpectation{
				{
					Field: "Global",
					Value: reflect.ValueOf(QOS_GLOBAL),
				},
				{
					Field: "PrefetchCount",
					Value: reflect.ValueOf(PREFETCH_COUNT),
				},
				{
					Field: "PrefetchSize",
					Value: reflect.ValueOf(0),
				},
			},
		},
	}
	ctxMaster, ctxCancel := trace.ConsumeTracesContext(validators)
	defer ctxCancel()

	chCounters := &EventCounters{
		Up:            &SafeCounter{},
		MsgPublished:  &SafeCounter{},
		DataExhausted: &SafeCounter{},
		MsgReceived:   &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.main"),
	)

	// we want testing consumers/publishers reliability as soon as
	// w/out any artificial delay induced by testing the connection up status (tested elsewhere)
	// if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
	// 	t.Fatal("timeout waiting for connection to be ready")
	// }

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos, &TopologyOptions{
		Name:    QueueName,
		Durable: true,
		Declare: true,
	})

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchTimeout(3 * time.Second).
		WithPrefetchCount(PREFETCH_COUNT).
		WithAutoAck(AUTO_ACK).
		WithQosGlobal(QOS_GLOBAL).
		WithPrefetchSize(PREFETCH_SIZE).
		WithName(CONSUMER_NAME).
		WithExclusive(EXCLUSIVE).
		WithNoLocal(NO_LOCAL).
		WithNoWait(NO_WAIT).
		WithArgs(ARG)

	alphaConsumer := NewConsumer(conn,
		optConsumer,
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionTopology(topos),
		WithChannelOptionProcessor(defaultPayloadProcessor),
	)
	// to ensure even distribution must have the consumers ready
	if !alphaConsumer.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("alphaConsumer not ready yet")
	}

	optPub := DefaultPublisherOptions()
	optPub.
		WithKey(QueueName).
		WithContext(ctxMaster).
		WithConfirmationsCount(2 * MSG_COUNT / 3)

	publisher := NewPublisher(conn, optPub,
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionNotification(statusCh),
	)
	defer publisher.Close()
	defer publisher.Channel().QueueDelete(QueueName, false, false, true)
	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		t.Fatal("publisher not ready yet")
	}
	if _, err := PublishMsgBulkOptions(publisher, optPub, MSG_COUNT, "exch.default"); err != nil {
		t.Error(err)
	}
	if !ConditionWait(ctxMaster, chCounters.MsgPublished.ValueEquals(MSG_COUNT), LongPoll) {
		t.Error("timeout pushing all the messages", chCounters.MsgPublished.Value())
	}

	// data comes through
	ceilReceivedCount := (MSG_COUNT + PREFETCH_COUNT - 1) / PREFETCH_COUNT
	if !ConditionWait(ctxMaster, chCounters.MsgReceived.GreaterEquals(ceilReceivedCount/2), LongPoll) {
		t.Error("timeout waiting data", chCounters.MsgReceived.Value())
	}

	// await full consumption
	if !ConditionWait(ctxMaster, chCounters.DataExhausted.GreaterEquals(1), DefaultPoll) {
		t.Error("timeout waiting for consumer to exhaust the queue", chCounters.DataExhausted.Value())
	}

	// parameters via API match the ones passed to the wrapped channel
	resultsCh := ctxMaster.Value(trace.TraceChannelResultsName).(chan trace.ErrorTrace)
	if len(resultsCh) > 0 {
		trace := <-resultsCh
		t.Errorf("Failed -> %v", trace.Err)
	}

	// general testing the consumer close/cancel/etc.
	if err := alphaConsumer.Cancel(); err != nil {
		t.Error("cancel", err)
	}
	if err := alphaConsumer.Close(); err != nil {
		t.Error("close", err)
	}
	// still have access to the Channel but ops would now fail
	c := alphaConsumer.Channel()
	if !c.IsClosed() {
		t.Error("channel should be closed")
	}
	conn.Close()
	// cannot cancel with no connection
	if err := c.Cancel(CONSUMER_NAME, false); err == nil {
		t.Error("subsequent cancel (no conn) expected error")
	}
	// but Close is idempotent
	if err := c.Close(); err != nil {
		t.Error("subsequent close (no conn)", err)
	}
	if alphaConsumer.AwaitAvailable(0, 0) {
		t.Error("consumer context should be done")
	}
}
