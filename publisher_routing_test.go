package grabbit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishMsgBulkOptions(pub *Publisher, opt PublisherOptions, records int, tag string) (int, error) {
	CONF_DELAY := LongPoll.Timeout
	ackCount := 0

	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	confs := make([]*DeferredConfirmation, records)

	for i := range records {
		buff.Reset()
		fmt.Fprintf(buff, "data-%s-%04d", tag, i)
		message.Body = buff.Bytes()

		if conf, err := pub.PublishDeferredConfirmWithOptions(opt, message); err != nil {
			return ackCount, err
		} else {
			confs[i] = conf
		}
	}
	for i := range records {
		conf := confs[i]

		switch pub.AwaitDeferredConfirmation(conf, CONF_DELAY).Outcome {
		case ConfirmationPrevious:
			// log.Printf("\033[91mprevious\033[0m message confirmed request [%04X] vs.response [%04X]. TODO: keep waiting.\n",
			// 	conf.RequestSequence, conf.DeliveryTag)
		case ConfirmationDisabled:
			return ackCount, errors.New("Not in confirmation mode (no DeferredConfirmation available)")
		case ConfirmationACK:
			// log.Printf("[%s][%s] \033[92m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
			ackCount++
		case ConfirmationNAK:
			// log.Printf("[%s][%s] \033[91m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
		default:
			// log.Printf("[%s][%s] \033[93m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
		}
	}
	return ackCount, nil
}

func PublishMsgBulk(pub *Publisher, records int, tag string) (int, error) {
	const CONF_DELAY = 7 * time.Second
	ackCount := 0

	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	confs := make([]*DeferredConfirmation, records)

	for i := 0; i < records; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("data-%s-%04d", tag, i))
		message.Body = buff.Bytes()

		if conf, err := pub.PublishDeferredConfirm(message); err != nil {
			return ackCount, err
		} else {
			confs[i] = conf
		}
	}
	for i := 0; i < records; i++ {
		conf := confs[i]

		switch pub.AwaitDeferredConfirmation(conf, CONF_DELAY).Outcome {
		case ConfirmationPrevious:
			// log.Printf("\033[91mprevious\033[0m message confirmed request [%04X] vs.response [%04X]. TODO: keep waiting.\n",
			// 	conf.RequestSequence, conf.DeliveryTag)
		case ConfirmationDisabled:
			return ackCount, errors.New("Not in confirmation mode (no DeferredConfirmation available)")
		case ConfirmationACK:
			// log.Printf("[%s][%s] \033[92m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
			ackCount++
		case ConfirmationNAK:
			// log.Printf("[%s][%s] \033[91m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
		default:
			// log.Printf("[%s][%s] \033[93m%s\033[0m with request [%04X] vs. response [%04X]\n",
			// 	conf.ChannelName, conf.Queue, conf.Outcome, conf.RequestSequence, conf.DeliveryTag,
			// )
		}
	}
	return ackCount, nil
}

func OnNotifyPublish(counter *SafeCounter) CallbackNotifyPublish {
	return func(confirmation amqp.Confirmation, ch *Channel) {
		counter.Add(1)
	}
}

func OnNotifyReturn(counter *SafeCounter) CallbackNotifyReturn {
	return func(confirmation amqp.Return, ch *Channel) {
		counter.Add(1)
	}
}

func MsgHandlerCount(count *SafeCounter) CallbackProcessMessages {
	return func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel) {
		if mustAck {
			idxLast := len(messages) - 1
			lastDeliveryTag := messages[idxLast].DeliveryTag
			ch.Ack(lastDeliveryTag, true)
		}
		count.Add(1)
	}
}

// TestPublisherRouting tests routing of messages involving a couple of exchanges
// and queues bound to both exchanges at different routing keys and topics. The topology is as follows:
//
//	exch.direct.logs -> (key.alert) -> queue.pagers
//	exch.direct.logs -> (key.info) -> queue.emails
//	exch.topic -> (*.alerts) -> queue.pagers
//	exch.topic -> (*.info) -> queue.emails
//
// The routing keys are:
func TestPublisherRouting(t *testing.T) {
	const KEY_ALERTS = "key.alert"           // routing key into pagers queue
	const KEY_INFO = "key.info"              // routing key into emails queue
	const KEY_TOPIC_ALERTS = "*.alerts"      // alerts key mask for topic exchange
	const KEY_TOPIC_INFO = "*.info"          // key mask for topic  exchange
	const QUEUE_PAGERS = "queue.pagers"      // alerts deposit for alert routed messages
	const QUEUE_EMAILS = "queue.emails"      // emails deposit for info routed messages
	const EXCHANGE_LOGS = "exch.direct.logs" // direct key dispatch exchange
	const EXCHANGE_GATEWAY = "exch.topic"    // by topic dispatch exchange

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	defer ctxCancel() // 'goleak' would complain w/out final clean-up

	conn := NewConnection(
		CONN_ADDR_RMQ_LOCAL, amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionName("conn.main"),
	)

	topos := make([]*TopologyOptions, 0, 8)
	// create an ephemeral 'logs' exchange
	topos = append(topos, &TopologyOptions{
		Name:          EXCHANGE_LOGS,
		Declare:       true,
		IsExchange:    true,
		IsDestination: false,
		Durable:       false,
		Kind:          "direct",
	})
	topos = append(topos, &TopologyOptions{
		Name:          EXCHANGE_GATEWAY,
		Declare:       true,
		IsExchange:    true,
		IsDestination: false,
		Durable:       false,
		Kind:          "topic",
	})
	// create an ephemeral 'pagers' queue, bound to 'logs' exchange and route key 'alert'
	topos = append(topos, &TopologyOptions{
		Name:    QUEUE_PAGERS,
		Declare: true,
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_LOGS,
			Key:     KEY_ALERTS,
		},
	})
	// same queue but bound to a topic exchange. Shame we have not implemented "bind" as array
	topos = append(topos, &TopologyOptions{
		Name:    QUEUE_PAGERS,
		Declare: true, // a must for applying a bind
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_GATEWAY,
			Key:     KEY_TOPIC_ALERTS,
		},
	})
	// create an ephemeral 'email' queue, bound to 'logs' exchange and route key 'info'
	topos = append(topos, &TopologyOptions{
		Name:    QUEUE_EMAILS,
		Declare: true,
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_LOGS,
			Key:     KEY_INFO,
		},
	})
	// same queue but bound to a topic exchange. Shame we have not implemented "bind" as array
	topos = append(topos, &TopologyOptions{
		Name:    QUEUE_EMAILS,
		Declare: true, // a must for applying a bind
		Bind: TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_GATEWAY,
			Key:     KEY_TOPIC_INFO,
		},
	})

	statusCh := make(chan Event, 10)
	chCounters := &EventCounters{
		Up:     &SafeCounter{},
		Down:   &SafeCounter{},
		Closed: &SafeCounter{},
	}
	go procStatusEvents(ctxMaster, statusCh, chCounters, nil)

	totalReadyCounter := &SafeCounter{}
	opt := DefaultPublisherOptions()
	opt.WithContext(ctxMaster).WithConfirmationsCount(20)

	publisher := NewPublisher(conn, opt,
		WithChannelOptionContext(ctxMaster),
		WithChannelOptionName("chan.publisher"),
		WithChannelOptionTopology(topos),
		WithChannelOptionNotification(statusCh),
		WithChannelOptionNotifyPublish(OnNotifyPublish(totalReadyCounter)),
	)
	defer publisher.Channel().ExchangeDelete(EXCHANGE_LOGS, false, true)
	defer publisher.Channel().ExchangeDelete(EXCHANGE_GATEWAY, false, true)
	defer publisher.Channel().QueueDelete(QUEUE_PAGERS, false, false, true)
	defer publisher.Channel().QueueDelete(QUEUE_EMAILS, false, false, true)

	if !publisher.AwaitAvailable(LongPoll.Timeout, 0) {
		t.Fatal("publisher not ready yet")
	}

	// via direct exchange: these should end up on the QUEUE_PAGERS
	opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_ALERTS)

	totalAckCount := &SafeCounter{}
	count, err := PublishMsgBulkOptions(publisher, opt, 5, EXCHANGE_LOGS)
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	// via topic exchange: these should also end up on the QUEUE_PAGERS
	opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.alerts")
	count, err = PublishMsgBulkOptions(publisher, opt, 6, EXCHANGE_GATEWAY)
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	// via default gateway: straight onto the queue
	opt.WithExchange("").WithKey(QUEUE_PAGERS)
	count, err = PublishMsgBulkOptions(publisher, opt, 7, "exch.default")
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	expectPagers := 18 // 5+6+7

	// these should end up on the QUEUE_EMAILS
	opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_INFO)
	count, err = PublishMsgBulkOptions(publisher, opt, 5, EXCHANGE_LOGS)
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	// these should also end up on the QUEUE_EMAILS
	opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.info")
	count, err = PublishMsgBulkOptions(publisher, opt, 4, EXCHANGE_GATEWAY)
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	// via default gateway: straight onto the queue
	opt.WithExchange("").WithKey(QUEUE_EMAILS)
	count, err = PublishMsgBulkOptions(publisher, opt, 3, "exch.default")
	if err != nil {
		t.Error(err)
	}
	totalAckCount.Add(count)
	expectEmails := 12 // 5+4+3

	// Prove right all routing on QUEUE_EMAILS and QUEUE_PAGERS
	// 0.this is an accumulation of all published & ACK-ed messages
	if totalAckCount.Value() != 30 {
		t.Errorf("expecting 30 messages total sent, got %d", totalAckCount.Value())
	}

	if !ConditionWait(ctxMaster, totalReadyCounter.ValueEquals(30), ShortPoll) {
		// 1.this is an accumulation of all published and notified messages
		t.Errorf("expecting 30 messages total sent, got %d", totalReadyCounter.Value())
	}

	// even if ACK-ed, it still takes a while at engine to transition to "Ready"
	optConsumer := DefaultConsumerOptions()

	// Consume messages from QUEUE_PAGERS
	pagersReceived := &SafeCounter{}
	pagersConsumer := NewConsumer(conn,
		*optConsumer.WithName("consumer.pagers").WithQueue(QUEUE_PAGERS),
		WithChannelOptionContext(ctxMaster),
		WithChannelOptionName("chan.pagers"),
		WithChannelOptionProcessor(MsgHandlerCount(pagersReceived)),
	)
	defer pagersConsumer.Close()

	// Consume messages from QUEUE_EMAILS
	emailsReceived := &SafeCounter{}
	emailsConsumer := NewConsumer(conn,
		*optConsumer.WithName("consumer.email").WithQueue(QUEUE_EMAILS),
		WithChannelOptionContext(ctxMaster),
		WithChannelOptionName("chan.email"),
		WithChannelOptionProcessor(MsgHandlerCount(emailsReceived)),
	)
	defer emailsConsumer.Close()

	// Wait for consumers to receive messages
	if !ConditionWait(ctxMaster, pagersReceived.ValueEquals(expectPagers), ShortPoll) {
		t.Errorf("expecting %d messages on %s, got %d", expectPagers, QUEUE_PAGERS, pagersReceived.Value())
	}

	if !ConditionWait(ctxMaster, emailsReceived.ValueEquals(expectEmails), ShortPoll) {
		t.Errorf("expecting %d messages on %s, got %d", expectEmails, QUEUE_EMAILS, emailsReceived.Value())
	}
}
