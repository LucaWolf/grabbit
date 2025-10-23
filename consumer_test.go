package grabbit

import (
	"context"
	"log"
	"reflect"
	"testing"
	"time"

	trace "traceutils"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RegisterSlice simulates the messages processing by validating (skipping) the duplicates
// and spending some work load time
func RegisterSlice(messages []DeliveryData, registry *SafeRegisterMap, source string) {
	for _, msg := range messages {
		if msg.Redelivered {
			if origin, has := registry.Has(string(msg.Body)); has {
				log.Printf(
					"[DUPLICATE] [%s] from [%s] first by [%s]\n",
					string(msg.Body),
					source,
					origin,
				)
				_ = origin
				continue
			}
		}
		registry.Set(string(msg.Body), source)
	}
	// simulate realisting processing load
	<-time.After(time.Duration(len(messages)*10) * time.Millisecond)
}

// msgSequential returns true if messages are sequential (same QoS batch)
// func msgSequential(messages []DeliveryData, name string) bool {
// 	for i := 1; i < len(messages); i++ {
// 		if messages[i].DeliveryTag != messages[i-1].DeliveryTag+1 {
// 			fmt.Println(
// 				" --- unordered registered range from",
// 				string(messages[0].Body),
// 				string(messages[len(messages)-1].Body),
// 				name,
// 			)
// 			return false
// 		}
// 	}
// 	return true
// }

func MsgHandlerBulk(r *SafeRand, registry *SafeRegisterMap) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		const FULL_BATCH_ACK_THRESHOLD = 10

		// if !mustAck {
		// 	log.Println(ch.Name(), "ACK disabled, flush during recovering", len(messages))
		// 	log.Println(string(messages[0].Body), "->", string(messages[len(messages)-1].Body))
		// 	return
		// }

		idxPivot := 2 * len(messages) / 3
		idxLast := len(messages) - 1
		pivotDeliveryTag := messages[idxPivot].DeliveryTag
		lastDeliveryTag := messages[idxLast].DeliveryTag

		// Is it possible the client has successful ACK state but server not?
		// Duplicate deliveries have been obeserved after link cut-off/recovery.
		// Adress by testing ch.AwaitStatus(false, timer) -- degrades app side throughput, or
		// deduplicate at worker side, like RegisterSlice does
		if len(messages) < FULL_BATCH_ACK_THRESHOLD {
			// msgSequential(messages, ch.Name())
			if err := ch.Ack(lastDeliveryTag, true); err == nil {
				RegisterSlice(messages, registry, ch.Name())
			}
		} else {
			// select one of the halves for Ack-ing and re-enqueue the other
			if r.Int()%2 == 0 {
				// msgSequential(messages[:idxPivot+1], ch.Name())
				if ch.Ack(pivotDeliveryTag, true) == nil &&
					ch.Nack(lastDeliveryTag, true, true) == nil {
					RegisterSlice(messages[:idxPivot+1], registry, ch.Name())
				}
			} else {
				// msgSequential(messages[idxPivot+1:], ch.Name())
				if ch.Nack(pivotDeliveryTag, true, true) == nil &&
					ch.Ack(lastDeliveryTag, true) == nil {
					RegisterSlice(messages[idxPivot+1:], registry, ch.Name())
				}
			}
		}

	}
}

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

func MsgHandlerSlow(count *SafeCounter, delay time.Duration) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		count.Add(1)
		if mustAck {
			idxLast := len(messages) - 1
			lastDeliveryTag := messages[idxLast].DeliveryTag
			ch.Ack(lastDeliveryTag, true)
		}
		<-time.After(delay)
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
		DataExhausted: &SafeCounter{tag: EventDataExhausted.String(), dedupe: true},
		MsgPublished:  &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, &recoveringCallbackCounter)

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionNotification(statusCh),
		WithConnectionOptionName("conn.consumer.batch"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	// conn manager is up
	if !conn.AwaitManager(true, LongPoll.Timeout) {
		t.Fatalf("connection manager should be running")
	}
	// connection is up notice
	if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for connection to be ready")
	}
	// connection is up internally
	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatalf("connection should have gone initially UP")
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
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready yet")
	}
	defer publisher.Channel().QueueDelete(QueueName, false, false, true)

	if _, err := PublishMsgBulkOptions(publisher, opt, MSG_COUNT, "exch.default"); err != nil {
		t.Fatal(err)
	}
	if !ConditionWait(ctxMaster, chCounters.MsgPublished.ValueEquals(MSG_COUNT), LongPoll) {
		t.Error("timeout pushing all the messages", chCounters.MsgPublished.Value())
	}
	publisher.Close()

	batchSelector := NewSafeRand(763482)
	registry := NewSafeRegisterMap()

	// start many consumers feeding events into the same pot
	delayer := DefaultDelayer{Value: 7 * time.Second, RetryCap: 7}
	type ConsumerAttr struct {
		Name          string
		PrefetchCount int
	}
	consumersAttr := []ConsumerAttr{
		{"one", 8},
		// {"two", 3},
		// {"three", 4},
		// {"four", 7},
		// {"five", 13},
		{"six", 19},
		{"seven", 21},
	}
	consumers := make([]*Consumer, 0, len(consumersAttr))
	tConsumerInit := time.Now()
	for _, consumerAttr := range consumersAttr {
		optConsumer := DefaultConsumerOptions()
		opt := optConsumer.
			WithQueue(QueueName).
			WithPrefetchTimeout(DefaultPoll.Timeout).
			WithPrefetchCount(consumerAttr.PrefetchCount).
			WithName(consumerAttr.Name)

		consumer := NewConsumer(conn, *opt,
			WithChannelOptionName("chan."+consumerAttr.Name),
			WithChannelOptionProcessor(MsgHandlerBulk(batchSelector, registry)),
			WithChannelOptionNotification(statusCh),
			WithChannelOptionDelay(delayer),
		)
		consumers = append(consumers, consumer)
	}
	for i, c := range consumers {
		if !c.AwaitStatus(true, LongPoll.Timeout) {
			t.Fatalf("consumer [%s] not ready yet", consumersAttr[i].Name)
		}
	}
	log.Println("INFO: channel(s) probed for initial UP after", time.Since(tConsumerInit))

	// sever the link some way through consuming
	ConditionWait(ctxMaster, registry.LenGreaterEquals(MSG_COUNT/7), LongVeryFrequentPoll)
	if err := rmqc.killConnections(conn.opt.name); err != nil {
		t.Error(err)
	}
	tConnectionKill := time.Now()

	// make sure both DOWN and back UP happened
	if !conn.AwaitStatus(false, LongPoll.Timeout) {
		t.Fatalf("connection should have gone DOWN")
	}
	for i, c := range consumers {
		if !c.AwaitStatus(false, LongPoll.Timeout) {
			t.Fatalf("consumer [%s] should have gone down", consumersAttr[i].Name)
		}
	}
	log.Println("INFO: channel(s) probed for DOWN after", time.Since(tConnectionKill))

	if !conn.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatalf("connection should have gone back UP")
	}
	// just to cover the code path of AwaitStatus
	for i, c := range consumers {
		if !c.AwaitStatus(true, LongPoll.Timeout) {
			t.Fatalf("consumer [%s] not ready yet", consumersAttr[i].Name)
		}
	}
	log.Println("INFO: channel(s) probed for UP after", time.Since(tConnectionKill))

	// closing a particular consumer should cleanly redirect the messages to the rest
	// do we need using Cancel() to guarantee success?
	// victim := consumers[len(consumersAttr)/2]
	// victim.Cancel()
	// victim.Close()

	// ... and get all messages consumed (eventually) via strict & unique exhaustion tested.
	if !ConditionWait(ctxMaster, chCounters.DataExhausted.ValueEquals(len(consumersAttr)), LongPoll) {
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
		WithConnectionOptionName("conn.consumer.excl"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	// connection is up
	if !ConditionWait(ctxMaster, chCounters.Up.NotZero, DefaultPoll) {
		t.Fatal("timeout waiting for connection to be ready")
	}

	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos,
		&TopologyOptions{
			Name:      QueueName,
			Durable:   true,
			Declare:   true,
			Exclusive: true,
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
	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("publisher not ready yet")
	}

	// make two consumers, each with a different autoACK policy
	qosCounter := &SafeCounter{}
	countAckAlpha := &SafeCounter{}
	countAckBeta := &SafeCounter{}

	optConsumer := DefaultConsumerOptions()
	optConsumer.
		WithQueue(QueueName).
		WithPrefetchTimeout(ShortPoll.Timeout).
		WithPrefetchCount(CONSUMER_BATCH_SIZE)

	// all messages should go here
	alphaConsumer := NewConsumer(conn,
		*optConsumer.WithName("consumer.alpha").WithExclusive(true),
		WithChannelOptionName("chan.alpha"),
		WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAckAlpha, CONSUMER_BATCH_SIZE)),
	)
	if !alphaConsumer.AwaitStatus(true, LongPoll.Timeout) {
		t.Fatal("alphaConsumer not ready yet")
	}
	// nothing for this guy on the same queue, different connection though
	// THIS NOT WORKING due to amqp dead-locks on the super channel "Consume()"
	// optConsumerBeta := DefaultConsumerOptions()
	// optConsumerBeta.
	// 	WithQueue(QueueName).
	// 	WithPrefetchTimeout(ShortPoll.Timeout).
	// 	WithPrefetchCount(CONSUMER_BATCH_SIZE).
	// 	WithName("consumer.beta").
	// 	WithExclusive(false)
	// // try with own connection, so far this blocked on same connection
	// betaConsumer := NewConsumer(conn,
	// 	optConsumerBeta,
	// 	WithChannelOptionName("chan.beta"),
	// 	WithChannelOptionProcessor(MsgHandlerQoS(qosCounter, countAckBeta, CONSUMER_BATCH_SIZE)),
	// )
	// if !betaConsumer.AwaitStatus(false, LongPoll.Timeout) {
	// 	t.Fatal("betaConsumer should be in lock/not ready yet state due to alphaConsumer")
	// }
	// betaConsumer.Close()

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
		WithConnectionOptionName("conn.consumer.opt"),
	)
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

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
		WithPrefetchTimeout(ShortPoll.Timeout).
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
	if !alphaConsumer.AwaitStatus(true, LongPoll.Timeout) {
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

	if !publisher.AwaitStatus(true, LongPoll.Timeout) {
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
	if !alphaConsumer.AwaitStatus(false, LongPoll.Timeout) {
		t.Error("consumer should be done/closed")
	}
}
