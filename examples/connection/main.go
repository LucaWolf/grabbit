package main

import (
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

func Down(name string, err grabbit.OptionalError) bool {
	log.Printf("callback_down {%s} went down with {%s}", name, err)
	return true // want continuing
}

func Up(name string) {
	log.Printf("callback_up: {%s} went up", name)
}

func Reattempting(name string, retry int) bool {
	log.Printf("callback_redo: {%s} retry count {%d}", name, retry)
	return true // want continuing
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
		grabbit.WithConnectionOptionDown(Down),
		grabbit.WithConnectionOptionUp(Up),
		grabbit.WithConnectionOptionRecovering(Reattempting),
	)

	ch := grabbit.NewChannel(conn,
		grabbit.WithChannelOptionName(ChannelName),
		grabbit.WithChannelOptionNotification(connStatusChan),
	)

	defer func() {
		fmt.Println("app closing connection and dependencies")
		if err := ch.Close(); err != nil {
			log.Print("cannot close ch: ", err)
		}
		if err := conn.Close(); err != nil {
			log.Print("cannot close conn: ", err)
		}

		// reconnect loop should be dead now
		<-time.After(15 * time.Second)
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
