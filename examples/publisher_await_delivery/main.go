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

type ConfirmationOutcome int

//go:generate stringer -type=ConfirmationOutcome  -linecomment
const (
	ConfirmationTimeOut ConfirmationOutcome = iota // no timely response
	ConfirmationClosed                             // data channel is closed
	ConfirmationACK                                // publish confirmed by server
	ConfirmationNAK                                // publish negative acknowledgement
)

type ConfirmationData struct {
	Outcome     ConfirmationOutcome // acknowledgment received stats
	ReqSequence uint64              // DeliveryTag of our request
	RspSequence uint64              // DeliveryTag of the notification
}

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
func OnNotifyPublish(confCh chan amqp.Confirmation) grabbit.CallbackNotifyPublish {
	return func(confirmation amqp.Confirmation, ch *grabbit.Channel) {
		log.Printf("callback: post [%04d] confirmed status [%v] from [%s][%s]\n",
			confirmation.DeliveryTag,
			confirmation.Ack,
			ch.Name(),
			ch.Queue())

		select {
		case confCh <- confirmation:
		default: // no more capacity in 'ch'
			log.Println("confirmation channel out of bandwidth")
		}
	}
}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	log.Printf("callback: publish returned from queue [%s]\n", ch.Queue())
}

func AwaitPublishConfirmation(seqNo uint64, confCh chan amqp.Confirmation, tmr time.Duration) ConfirmationData {
	result := ConfirmationData{
		ReqSequence: seqNo,
	}

	select {
	case <-time.After(tmr):
		result.Outcome = ConfirmationTimeOut
		return result
	case confirmation, ok := <-confCh:
		if !ok {
			result.Outcome = ConfirmationClosed
			return result
		}
		// can we get out of sync reports: delays, re-posts, etc?
		result.RspSequence = confirmation.DeliveryTag
		// do something useful here
		if confirmation.Ack {
			result.Outcome = ConfirmationACK
		} else {
			result.Outcome = ConfirmationNAK
		}
	}
	return result
}

func PublishMsg(confCh chan amqp.Confirmation, publisher *grabbit.Publisher, start, end int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		<-time.After(1 * time.Second)
		buff.Reset()
		buff.WriteString(fmt.Sprintf("test number %04d", i))
		message.Body = buff.Bytes()
		log.Println("going to send:", buff.String())

		nextSeqNo := publisher.Channel().GetNextPublishSeqNo()

		if err := publisher.Publish(message); err != nil {
			log.Println("publishing failed with: ", err)
		} else {
			confirmation := AwaitPublishConfirmation(nextSeqNo, confCh, 7*time.Second)
			log.Printf("confirmation: [%s] for req.[%04X] with rsp.[%04X]\n",
				confirmation.Outcome,
				confirmation.ReqSequence,
				confirmation.RspSequence)

			if confirmation.ReqSequence != confirmation.RspSequence {
				log.Println("FIXME! how do we recover from this?")
			}
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

	// collects from publishers notifications
	confCh := make(chan amqp.Confirmation, 10)

	publisher := grabbit.NewPublisher(conn, pubOpt,
		grabbit.WithChannelOptionContext(ctxMaster),
		grabbit.WithChannelOptionName(ChannelName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotification(pubStatusChan),
		grabbit.WithChannelOptionDown(OnPubDown),
		grabbit.WithChannelOptionUp(OnPubUp),
		grabbit.WithChannelOptionRecovering(OnPubReattempting),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish(confCh)),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
	)

	if !publisher.AwaitStatus(true, 30*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}

	PublishMsg(confCh, publisher, 0, 5)

	defer func() {
		log.Println("app closing connection and dependencies")

		if err := publisher.Close(); err != nil {
			log.Println("cannot close publisher: ", err)
		}
		// associated chan is gone, can no longer send data
		PublishMsg(confCh, publisher, 5, 10) // expect failures

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
