package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	grabbit "github.com/LucaWolf/grabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishSomeLogs(
	publisher *grabbit.Publisher,
	opt grabbit.PublisherOptions,
	start, end int) {

	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("msg ID %04d for target (%s, %s)",
			i, opt.Exchange, opt.Key))

		message.Body = buff.Bytes()
		if err := publisher.PublishWithOptions(opt, message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

func main() {
	const KEY_ALERTS = "key.alert"           // routing key into pagers queue
	const KEY_INFO = "key.info"              // routing key into emails queue
	const KEY_TOPIC_ALERTS = "*.alerts"      // alerts key mask for topic exchange
	const KEY_TOPIC_INFO = "*.info"          // key mask for topic  exchange
	const QUEUE_PAGERS = "queue.pagers"      // alerts deposit for alert routed messages
	const QUEUE_EMAILS = "queue.emails"      // emails deposit for info routed messages
	const EXCHANGE_LOGS = "exch.direct.logs" // direct key dispatch exchange
	const EXCHANGE_GATEWAY = "exch.topic"    // by topic dispatch exchange

	connStatusChan := make(chan grabbit.Event, 10)
	defer close(connStatusChan)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())

	// await and log any infrastructure notifications
	go func() {
		for event := range connStatusChan {
			log.Println("notification: ", event)
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionContext(ctxMaster),
		grabbit.WithConnectionOptionName("conn.main"),
		grabbit.WithConnectionOptionNotification(connStatusChan),
	)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	// create an ephemeral 'logs' exchange
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          EXCHANGE_LOGS,
		Declare:       true,
		IsExchange:    true,
		IsDestination: false,
		Durable:       false,
		Kind:          "direct",
	})
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          EXCHANGE_GATEWAY,
		Declare:       true,
		IsExchange:    true,
		IsDestination: false,
		Durable:       false,
		Kind:          "topic",
	})
	// create an ephemeral 'pagers' queue, bound to 'logs' exchange and route key 'alert'
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QUEUE_PAGERS,
		Declare: true,
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_LOGS,
			Key:     KEY_ALERTS,
		},
	})
	// same queue but bound to a topic exchange. Shame we have not implemented "bind" as array
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QUEUE_PAGERS,
		Declare: true, // a must for applying a bind
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_GATEWAY,
			Key:     KEY_TOPIC_ALERTS,
		},
	})
	// create an ephemeral 'email' queue, bound to 'logs' exchange and route key 'info'
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QUEUE_EMAILS,
		Declare: true,
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_LOGS,
			Key:     KEY_INFO,
		},
	})
	// same queue but bound to a topic exchange. Shame we have not implemented "bind" as array
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QUEUE_EMAILS,
		Declare: true, // a must for applying a bind
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    EXCHANGE_GATEWAY,
			Key:     KEY_TOPIC_INFO,
		},
	})

	opt := grabbit.DefaultPublisherOptions()
	opt.WithContext(ctxMaster).WithConfirmationsCount(20)

	publisher := grabbit.NewPublisher(conn, opt,
		grabbit.WithChannelOptionName("chan.publisher"),
		grabbit.WithChannelOptionTopology(topos),
	)

	if !publisher.AwaitAvailable(10*time.Second, 1*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(3 * time.Second)
		log.Println("EXIT")
		return
	}

	// via direct exchange: these should end up on the QueueAlerts
	opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_ALERTS)
	publishSomeLogs(publisher, opt, 0, 5)
	// via topic exchanage: these should also end up on the QueueAlerts
	opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.alerts")
	publishSomeLogs(publisher, opt, 5, 10)
	// via default gateway: straight onto the queue
	opt.WithExchange("").WithKey(QUEUE_PAGERS)
	publishSomeLogs(publisher, opt, 10, 15)

	// these should end up on the QueueInfo
	opt.WithExchange(EXCHANGE_LOGS).WithKey(KEY_INFO)
	publishSomeLogs(publisher, opt, 15, 20)
	// these should also end up on the QueueInfo
	opt.WithExchange(EXCHANGE_GATEWAY).WithKey("gw.info")
	publishSomeLogs(publisher, opt, 20, 25)
	// via default gateway: straight onto the queue
	opt.WithExchange("").WithKey(QUEUE_EMAILS)
	publishSomeLogs(publisher, opt, 25, 30)

	// prove right all routing with consumers on QUEUE_EMAILS and QUEUE_PAGERS
	optConsumer := grabbit.DefaultConsumerOptions()
	optConsumer.WithPrefetchTimeout(7 * time.Second)

	_ = grabbit.NewConsumer(conn,
		*optConsumer.WithName("cons.emails").WithQueue(QUEUE_EMAILS),
		grabbit.WithChannelOptionName("chan.emails/info"),
		grabbit.WithChannelOptionProcessor(MsgHandler),
	)
	_ = grabbit.NewConsumer(conn,
		*optConsumer.WithName("cons.pagers").WithQueue(QUEUE_PAGERS),
		grabbit.WithChannelOptionName("chan.pagers/alert"),
		grabbit.WithChannelOptionProcessor(MsgHandler),
	)

	// block main thread - wait for shutdown signal
	log.Println("awaiting signal")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	ctxCancel()
	<-time.After(3 * time.Second)
	log.Println("EXIT")
}

func MsgHandler(
	props *grabbit.DeliveriesProperties,
	messages []grabbit.DeliveryData,
	mustAck bool,
	ch *grabbit.Channel) {

	for _, msg := range messages {
		log.Printf("  [%s][%s] got message: %s\n",
			ch.Name(), props.ConsumerTag,
			string(msg.Body))
		ch.Ack(msg.DeliveryTag, false)
	}
}
