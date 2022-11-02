package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	grabbit "github.com/LucaWolf/grabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ConnectionName = "conn.main"

func main() {
	connStatusChan := make(chan grabbit.Event, 10)

	// await and log any infrastructure notifications
	go func() {
		for event := range connStatusChan {
			fmt.Printf("Got rabbit notification: %s\n",
				event)
		}
	}()

	conn, err := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionName(ConnectionName),
		grabbit.WithConnectionOptionNotification(connStatusChan),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Fatal(err)
		}
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
	fmt.Println("stopping consumer")
}
