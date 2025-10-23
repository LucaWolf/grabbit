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
		buff.WriteString(fmt.Sprintf("msg-%d", i))
		message.Body = buff.Bytes()

		if err := publisher.Publish(message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

// HandleMsgRegistry accumulates in 'r' and test uniqueness of the ACK-ed messages (delivered via 'ch')
func HandleMsgRegistry(ctx context.Context, r map[string]struct{}, mu *sync.Mutex, ch chan grabbit.DeliveryPayload) {
	mu.Lock()
	defer mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case m, chAlive := <-ch:
			if !chAlive {
				return
			}
			if _, has := r[string(m)]; has {
				log.Printf("\033[91mERROR\033[0m duplicate; msg [%s] already ACK-ed!\n", string(m))
			} else {
				r[string(m)] = struct{}{}
			}
		}
	}
}

func BodySlice(messages []grabbit.DeliveryData, ch chan grabbit.DeliveryPayload) string {
	bodies := make([][]byte, len(messages))
	for n, msg := range messages {
		bodies[n] = msg.Body
		ch <- msg.Body
	}
	return string(bytes.Join(bodies, []byte(",")))
}

func MsgHandler(rnd *rand.Rand, mu *sync.Mutex, chReg chan grabbit.DeliveryPayload) grabbit.CallbackProcessMessages {
	return func(props *grabbit.DeliveriesProperties, messages []grabbit.DeliveryData, mustAck bool, ch *grabbit.Channel) {
		if !mustAck {
			return
		}

		idxPivot := 2 * len(messages) / 3
		idxLast := len(messages) - 1
		firstDeliveryTag := messages[0].DeliveryTag
		pivotDeliveryTag := messages[idxPivot].DeliveryTag
		lastDeliveryTag := messages[idxLast].DeliveryTag

		if len(messages) < 5 {
			log.Printf("\n\t[%s] bulk ACK all deliveries [%04d - %04d] %s\n",
				props.ConsumerTag, firstDeliveryTag, lastDeliveryTag,
				BodySlice(messages, chReg),
			)
			ch.Ack(lastDeliveryTag, true)
		} else {
			mu.Lock()
			r := rnd.Int()
			mu.Unlock()
			// select one of the halves for Ack-ing and re-enqueue the other
			if r%2 == 0 {
				log.Printf("\n\t[%s] bulk ACK 1st deliveries [%04d - %04d] %s"+
					"\n\t[%s] bulk NAK 2nd deliveries (%04d - %04d]\n",
					props.ConsumerTag, firstDeliveryTag, pivotDeliveryTag,
					BodySlice(messages[:idxPivot+1], chReg),
					props.ConsumerTag, pivotDeliveryTag, lastDeliveryTag,
				)
				ch.Ack(pivotDeliveryTag, true)
				ch.Nack(lastDeliveryTag, true, true)
			} else {
				log.Printf("\n\t[%s] bulk NAK 1st deliveries [%04d - %04d]"+
					"\n\t[%s] bulk ACK 2nd deliveries (%04d - %04d] %s\n",
					props.ConsumerTag, firstDeliveryTag, pivotDeliveryTag,
					props.ConsumerTag, pivotDeliveryTag, lastDeliveryTag,
					BodySlice(messages[idxPivot+1:], chReg),
				)
				ch.Nack(pivotDeliveryTag, true, true)
				ch.Ack(lastDeliveryTag, true)
			}
		}
	}
}

func main() {
	ConnectionName := "conn.main"
	PublisherName := "chan.publisher.example"
	QueueName := "workload"
	const MSG_COUNT = 500

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
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	})

	publisher := grabbit.NewPublisher(conn, opt,
		grabbit.WithChannelOptionName(PublisherName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
	)

	if !publisher.AwaitStatus(true, 30*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}
	PublishMsg(publisher, 0, MSG_COUNT)

	rnd := rand.New(rand.NewSource(543752))
	mu := sync.Mutex{}
	validate := make(chan grabbit.DeliveryPayload, 300)

	msgRegistry := make(map[string]struct{})
	muReg := sync.Mutex{}
	go HandleMsgRegistry(ctxMaster, msgRegistry, &muReg, validate)

	optConsumer := grabbit.DefaultConsumerOptions()
	optConsumer.WithQueue(QueueName).WithPrefetchTimeout(5 * time.Second)

	// start many consumers
	_ = grabbit.NewConsumer(conn, *optConsumer.WithPrefetchCount(8).WithName("consumer.one"),
		grabbit.WithChannelOptionName("chan.cons.one"),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu, validate)),
	)
	_ = grabbit.NewConsumer(conn, *optConsumer.WithPrefetchCount(3).WithName("consumer.two"),
		grabbit.WithChannelOptionName("chan.cons.two"),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu, validate)),
	)
	_ = grabbit.NewConsumer(conn, *optConsumer.WithPrefetchCount(4).WithName("consumer.three"),
		grabbit.WithChannelOptionName("chan.cons.three"),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu, validate)),
	)
	_ = grabbit.NewConsumer(conn, *optConsumer.WithPrefetchCount(7).WithName("consumer.four"),
		grabbit.WithChannelOptionName("chan.cons.four"),
		grabbit.WithChannelOptionProcessor(MsgHandler(rnd, &mu, validate)),
	)

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
	ctxCancel()
	<-time.After(3 * time.Second)
	// finally test we got all messages ACK-ed
	muReg.Lock()
	if len(msgRegistry) != MSG_COUNT {
		log.Printf("\033[91mERROR\033[0m missing ACK %d vs. %d", len(msgRegistry), MSG_COUNT)
	} else {
		log.Print("\033[92mOK\033[0m", " all messages were registered fine.")
	}
	muReg.Unlock()
	log.Println("EXIT")
}
