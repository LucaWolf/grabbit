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

var (
	ConnectionName = "conn.main"
	ChannelName    = "chan.publisher.example"
	QueueName      = "workload"
)

// CallbackWhenDown
func OnDown(name string, err error) bool {
	log.Printf("callback: {%s} went down with {%s}", name, err)
	return true // want continuing
}

// CallbackWhenUp
func OnUp(name string) {
	log.Printf("callback: {%s} went up", name)
}

// CallbackWhenRecovering
func OnReattempting(name string, retry int) bool {
	log.Printf("callback: connection establish {%s} retry count {%d}", name, retry)
	return true // want continuing
}

// CallbackNotifyPublish
func OnNotifyPublish(confirm amqp.Confirmation, ch *grabbit.Channel) {
	log.Printf("callback: publish confirmed status [%v] from queue [%s]\n", confirm.Ack, ch.Queue())
}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	log.Printf("callback: publish returned from queue [%s]\n", ch.Queue())
}

func publishSomeIDs(publisher *grabbit.Publisher, start, end int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		<-time.After(3 * time.Second)
		buff.Reset()
		buff.WriteString(fmt.Sprintf("test number: %04d", i))

		log.Println("going to send:", buff.String())

		message.Body = buff.Bytes()
		if err := publisher.Publish(message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

func main() {
	connStatusChan := make(chan grabbit.Event, 10)
	defer close(connStatusChan)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())

	// await and log any infrastructure notifications
	go func() {
		for event := range connStatusChan {
			log.Println("notification: ", event)
			// _ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionContext(ctxMaster),
		grabbit.WithConnectionOptionName(ConnectionName),
		grabbit.WithConnectionOptionNotification(connStatusChan),
		grabbit.WithConnectionOptionDown(OnDown),
		grabbit.WithConnectionOptionUp(OnUp),
		grabbit.WithConnectionOptionRecovering(OnReattempting),
		// grabbit.WithConnectionOptionDelay(some_delayer), -- falls back to DefaultDelayer(7500ms)
	)

	params := grabbit.DefaultPublisherOptions()
	params.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(20)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          QueueName,
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	})

	publisher := grabbit.NewPublisher(conn, params,
		grabbit.WithChannelOptionContext(ctxMaster),
		grabbit.WithChannelOptionName(ChannelName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
		// grabbit.WithChannelOptionContext(ctxMaster), -- inherited from connection
		// grabbit.WithChannelOptionDelay(some_delayer), -- inherited from connection
		// grabbit.WithChannelOptionNotification(connStatusChan), -- inherited from connection
	)

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}

	publishSomeIDs(publisher, 0, 5) // smooth operator

	defer func() {
		log.Println("app closing connection and dependencies")

		if err := conn.Close(); err != nil {
			log.Println("cannot close conn: ", err)
		}

		publishSomeIDs(publisher, 5, 10) // expect failures

		if err := publisher.Close(); err != nil {
			log.Println("cannot close ch: ", err)
		}

		// reconnect loop should be dead by now... no further notifications should be observed
		<-time.After(15 * time.Second)
		log.Println("EXIT")
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Println(sig)
		close(done)
	}()

	log.Println("awaiting signal")
	<-done
}
