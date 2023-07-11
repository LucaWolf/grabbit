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
)

func Down(name string, err grabbit.OptionalError) bool {
	log.Printf("callback_down: {%s} went down with {%s}", name, err)
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
	connStatusChan := make(chan grabbit.Event, 32)

	// capture status notifications and perform desired actions based on this
	// like e.g. cancel the global context, adjust metrics, exit the app... whatever your requirements fancy
	go func() {
		for event := range connStatusChan {
			log.Print("notification: ", event)
			// process any event by its combination of .SourceType, .SourceName and .Kind
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

	defer func() {
		fmt.Println("app closing connection and dependencies")

		if err := conn.Close(); err != nil {
			log.Print("cannot close conn: ", err)
		}

		// reconnect loop should be dead now
		<-time.After(15 * time.Second)
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan struct{})

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
