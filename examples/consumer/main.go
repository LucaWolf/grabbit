package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	grabbit "github.com/LucaWolf/grabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

// CallbackWhenDown
func OnConsumerDown(name string, err grabbit.OptionalError) bool {
	log.Printf("callback: {%s} went down with {%s}", name, err)
	return true // want continuing
}

// CallbackWhenUp
func OnConsumerUp(name string) {
	log.Printf("callback: {%s} went up", name)
}

// CallbackWhenRecovering
func OnConsumerReattempting(name string, retry int) bool {
	log.Printf("callback: connection establish {%s} retry count {%d}", name, retry)
	return true // want continuing
}

// CallbackNotifyPublish
func OnNotifyPublish(confirm amqp.Confirmation, ch *grabbit.Channel) {
	log.Printf("callback: publish confirmed [%v] from  [%s][%s]\n", confirm, ch.Name(), ch.Queue())
}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	log.Printf("callback: publish returned from [%s][%s]\n", ch.Name(), ch.Queue())
}

func PublishMsg(publisher *grabbit.Publisher, start, end int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("test number %04d", i))
		message.Body = buff.Bytes()

		if err := publisher.Publish(message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

func MsgHandler(rnd *rand.Rand, mu *sync.Mutex) grabbit.CallbackProcessMessages {
	return func(props *grabbit.DeliveriesProperties, messages []grabbit.DeliveryData, mustAck bool, ch *grabbit.Channel) {
		for _, msg := range messages {
			// only global function is goroutine safe, not rand.Rand objects
			mu.Lock()
			r := rnd.Int()
			mu.Unlock()
			action := "ACK"
			if r%2 == 0 {
				action = "NAK - expect this again later on"
			}
			log.Printf("  [%s][%s] got message: %s -- [%s]\n", ch.Name(), props.ConsumerTag, string(msg.Body), action)

			if mustAck && len(messages) != 0 {
				// for fun, put back on the queue for other consumers to reap the benefits
				if r%2 == 0 {
					ch.Nack(msg.DeliveryTag, false, true)
				} else {
					ch.Ack(msg.DeliveryTag, false)
				}
			}
		}
	}
}

func main() {
	ConnectionName := "conn.main"
	PublisherName := "chan.publisher.example"
	ConsumerOneName := "consumer.one"
	ConsumerTwoName := "consumer.two"
	QueueName := "workload"

	events := make(chan grabbit.Event, 10)
	defer close(events)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())

	// await and log any infrastructure notifications
	go func() {
		for event := range events {
			log.Println("notification: ", event)
			// _ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionContext(ctxMaster),
		grabbit.WithConnectionOptionName(ConnectionName),
		grabbit.WithConnectionOptionNotification(events),
	)

	opt := grabbit.DefaultPublisherOptions()
	opt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(20)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          QueueName,
		IsDestination: true, // this also sets the consumer queue link
		Durable:       true,
		Declare:       true,
	})

	publisher := grabbit.NewPublisher(conn, opt,
		grabbit.WithChannelOptionName(PublisherName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
	)

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}
	PublishMsg(publisher, 0, 35)

	rnd := rand.New(rand.NewSource(773274))
	mu := sync.Mutex{}

	optConsumerOne := grabbit.DefaultConsumerOptions()
	optConsumerOne.WithName(ConsumerOneName).
		WithPrefetchCount(1).
		WithQueue("auto_generated_will_take_over"). // IsDestination above
		WithPrefetchTimeout(7 * time.Second)
	// start this consumer
	_ = grabbit.NewConsumer(conn, optConsumerOne,
		grabbit.WithChannelOptionName("chan."+ConsumerOneName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionDown(OnConsumerDown),
		grabbit.WithChannelOptionUp(OnConsumerUp),
		grabbit.WithChannelOptionRecovering(OnConsumerReattempting),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu)),
	)

	optConsumerTwo := grabbit.DefaultConsumerOptions()
	optConsumerTwo.WithName(ConsumerTwoName).
		WithPrefetchCount(1).
		WithQueue("auto_generated_will_take_over"). // IsDestination above
		WithPrefetchTimeout(7 * time.Second)
	// start this consumer
	_ = grabbit.NewConsumer(conn, optConsumerTwo,
		grabbit.WithChannelOptionName("chan."+ConsumerTwoName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionDown(OnConsumerDown),
		grabbit.WithChannelOptionUp(OnConsumerUp),
		grabbit.WithChannelOptionRecovering(OnConsumerReattempting),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu)),
	)

	defer func() {
		ctxCancel()
		<-time.After(3 * time.Second)
		log.Println("EXIT")
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan struct{})

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Println(sig)
		close(done)
	}()

	log.Println("awaiting signal")
	// if we restart the Rabbit engine we should get
	// all the topology recreated when conn/channels recovery has completed
	<-done
}
