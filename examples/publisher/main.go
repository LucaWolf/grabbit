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

// CallbackWhenDown
func OnPubDown(name string, err grabbit.OptionalError) bool {
	log.Printf("callback_down: {%s} went down with {%s}", name, err)
	return true // want continuing
}

func OnPubUp(name string) {
	log.Printf("callback_up: {%s} went up", name)
}

func OnPubReattempting(name string, retry int) bool {
	log.Printf("callback_redo: {%s} retry count {%d}", name, retry)
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

func PublishMsg(publisher *grabbit.Publisher, start, end int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		<-time.After(1 * time.Second)
		buff.Reset()
		buff.WriteString(fmt.Sprintf("test number %04d", i))
		message.Body = buff.Bytes()
		log.Println("going to send:", buff.String())

		if err := publisher.Publish(message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

func main() {
	ConnectionName := "conn.main"
	ChannelName := "chan.publisher.example"
	QueueName := "workload"

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())
	pubStatusChan := make(chan grabbit.Event, 32)

	// await and log any infrastructure notifications
	go func() {
		for event := range pubStatusChan {
			log.Println("publisher.notification: ", event)
			// _ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionContext(ctxMaster),
		grabbit.WithConnectionOptionName(ConnectionName),
	)

	pubOpt := grabbit.DefaultPublisherOptions()
	pubOpt.WithKey(QueueName).WithContext(ctxMaster).WithConfirmationsCount(20)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          QueueName,
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	})

	publisher := grabbit.NewPublisher(conn, pubOpt,
		grabbit.WithChannelOptionContext(ctxMaster),
		grabbit.WithChannelOptionName(ChannelName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotification(pubStatusChan),
		grabbit.WithChannelOptionDown(OnPubDown),
		grabbit.WithChannelOptionUp(OnPubUp),
		grabbit.WithChannelOptionRecovering(OnPubReattempting),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
	)

	if !publisher.AwaitStatus(true, 30*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}

	PublishMsg(publisher, 0, 5)

	defer func() {
		log.Println("app closing connection and dependencies")

		if err := publisher.Close(); err != nil {
			log.Println("cannot close publisher: ", err)
		}
		// associated chan is gone, can no longer send data
		PublishMsg(publisher, 5, 10) // expect failures

		if err := conn.Close(); err != nil {
			log.Print("cannot close conn: ", err)
		}
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
	<-done
}
