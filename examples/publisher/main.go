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
	ChannelName    = "chan.main"
)

func OnDown(name string, err error) bool {
	log.Printf("callback_down {%s} went down with {%s}", name, err)
	return true // want continuing
}

func OnUp(name string) {
	log.Printf("callback_up: {%s} went up", name)
}

func OnReattempting(name string, retry int) bool {
	log.Printf("callback_redo: {%s} retry count {%d}", name, retry)
	return true // want continuing
}

// CallbackNotifyPublish
func OnNotifyPublish(confirm amqp.Confirmation, ch *grabbit.Channel) {
	fmt.Printf("confirm status [%v] over queue [%s]\n", confirm.Ack, ch.Queue().Name)
}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	fmt.Printf("returned over queue [%s]\n", ch.Queue().Name)
}

func main() {
	connStatusChan := make(chan grabbit.Event, 10)

	// await and log any infrastructure notifications
	go func() {
		for event := range connStatusChan {
			log.Print("notification: ", event)
			// _ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionName(ConnectionName),
		grabbit.WithConnectionOptionNotification(connStatusChan),
		grabbit.WithConnectionOptionDown(OnDown),
		grabbit.WithConnectionOptionUp(OnUp),
		grabbit.WithConnectionOptionRecovering(OnReattempting),
	)

	params := grabbit.ImplementationParameters{
		ConfirmationNoWait: false,
	}

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          "publisher_example",
		IsDestination: true,
		Durable:       true,
		Declare:       true,
	})

	pub := grabbit.NewPublisher(conn, params,
		grabbit.WithChannelOptionName("chan_publisher_example"),
		grabbit.WithChannelOptionNotification(connStatusChan),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
	)

	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := 0; i < 5; i++ {
		buff.Reset()
		buff.WriteString(fmt.Sprintf("test number: %04d", i))

		fmt.Println("going to send:", buff.String())

		message.Body = buff.Bytes()
		if err := pub.PublishWithContext(context.TODO(), "", "publisher_example", true, false, message); err != nil {
			fmt.Println("publishing failed with:", err)
		}
		<-time.After(7 * time.Second)
	}

	defer func() {
		fmt.Println("app closing connection and dependencies")

		if err := conn.Close(); err != nil {
			log.Print("cannot close conn: ", err)
		}

		// try publishing some more
		for i := 5; i < 10; i++ {
			buff.Reset()
			buff.WriteString(fmt.Sprintf("test number: %04d", i))

			fmt.Println("going to send:", buff.String())

			message.Body = buff.Bytes()
			if err := pub.PublishWithContext(context.TODO(), "", "publisher_example", true, false, message); err != nil {
				fmt.Println("publishing failed with:", err)
			}
			<-time.After(7 * time.Second)
		}

		if err := pub.Close(); err != nil {
			log.Print("cannot close ch: ", err)
		}

		// reconnect loop should be dead now
		<-time.After(15 * time.Second)
		fmt.Println("EXIT")
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(done)
	}()

	fmt.Println("awaiting signal")
	<-done
}
