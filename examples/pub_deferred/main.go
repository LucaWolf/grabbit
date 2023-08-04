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
func OnNotifyPublish(confirmation amqp.Confirmation, ch *grabbit.Channel) {
	log.Printf("callback: post [%04d] confirmed status [%v] from [%s][%s]\n",
		confirmation.DeliveryTag,
		confirmation.Ack,
		ch.Name(),
		ch.Queue())

}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	log.Printf("callback: publish returned from queue [%s]\n", ch.Queue())
}

func PublishMsgOneByOne(publisher *grabbit.Publisher, records int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := 0; i < records; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("one-by-one test number %04d", i))
		message.Body = buff.Bytes()

		if futureConfirm, err := publisher.PublishDeferredConfirm(message); err != nil {
			log.Println("publishing failed with: ", err)
		} else {
			confirmation := publisher.AwaitDeferedConfirmation(futureConfirm, 7*time.Second)

			switch confirmation.Outcome {
			case grabbit.ConfirmationPrevious:
				log.Printf("\033[91m previous \033[0m message confirmed request [%04X] vs.response [%04X]. TODO: keep waiting.\n",
					confirmation.RequestSequence, confirmation.DeliveryTag)
			case grabbit.ConfirmationDisabled:
				log.Println("Not in confirmation mode (no DeferredConfirmation available).")
			case grabbit.ConfirmationACK:
				log.Printf("[%s][%s] \033[92m%s\033[0m with request [%04X] vs. response [%04X]\n",
					confirmation.ChannelName, confirmation.Queue,
					confirmation.Outcome,
					confirmation.RequestSequence, confirmation.DeliveryTag,
				)
			case grabbit.ConfirmationNAK:
				log.Printf("[%s][%s] \033[91m%s\033[0m with request [%04X] vs. response [%04X]\n",
					confirmation.ChannelName, confirmation.Queue,
					confirmation.Outcome,
					confirmation.RequestSequence, confirmation.DeliveryTag,
				)
			default:
				log.Printf("[%s][%s] \033[93m%s\033[0m with request [%04X] vs. response [%04X]\n",
					confirmation.ChannelName, confirmation.Queue,
					confirmation.Outcome,
					confirmation.RequestSequence, confirmation.DeliveryTag,
				)
			}
		}
	}
}

// PublishMsgBulk tests that deliveries are received orderly
func PublishMsgBulk(publisher *grabbit.Publisher, records int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	confs := make([]*grabbit.DeferredConfirmation, records)

	for i := 0; i < records; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("bulk test number %04d", i))
		message.Body = buff.Bytes()

		if confirmation, err := publisher.PublishDeferredConfirm(message); err != nil {
			log.Println("publishing failed with: ", err)
		} else {
			confs[i] = confirmation
		}
	}
	for i := 0; i < records; i++ {
		confirmation := publisher.AwaitDeferedConfirmation(confs[i], 7*time.Second)

		switch confirmation.Outcome {
		case grabbit.ConfirmationPrevious:
			log.Printf("\033[91mprevious\033[0m message confirmed request [%04X] vs.response [%04X]. TODO: keep waiting.\n",
				confirmation.RequestSequence, confirmation.DeliveryTag)
		case grabbit.ConfirmationDisabled:
			log.Println("Not in confirmation mode (no DeferredConfirmation available).")
		case grabbit.ConfirmationACK:
			log.Printf("[%s][%s] \033[92m%s\033[0m with request [%04X] vs. response [%04X]\n",
				confirmation.ChannelName, confirmation.Queue,
				confirmation.Outcome,
				confirmation.RequestSequence, confirmation.DeliveryTag,
			)
		case grabbit.ConfirmationNAK:
			log.Printf("[%s][%s] \033[91m%s\033[0m with request [%04X] vs. response [%04X]\n",
				confirmation.ChannelName, confirmation.Queue,
				confirmation.Outcome,
				confirmation.RequestSequence, confirmation.DeliveryTag,
			)
		default:
			log.Printf("[%s][%s] \033[93m%s\033[0m with request [%04X] vs. response [%04X]\n",
				confirmation.ChannelName, confirmation.Queue,
				confirmation.Outcome,
				confirmation.RequestSequence, confirmation.DeliveryTag,
			)
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

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}

	log.Println("=========================================")
	PublishMsgOneByOne(publisher, 7)
	log.Println("=========================================")
	PublishMsgBulk(publisher, 5)
	log.Println("=========================================")

	defer func() {
		log.Println("app closing connection and dependencies")

		if err := publisher.Close(); err != nil {
			log.Println("cannot close publisher: ", err)
		}
		// associated chan is gone, can no longer send data
		PublishMsgOneByOne(publisher, 5) // expect failures
		log.Println("=========================================")

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
