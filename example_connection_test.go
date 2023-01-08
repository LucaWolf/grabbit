package grabbit

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ExampleNewConnection() {
	// global cancelling context
	ctxMaster, ctxCancel := context.WithCancel(context.TODO())

	// Events notification handler
	connStatusChan := make(chan Event, 10)
	defer close(connStatusChan)
	// await and log any infrastructure notifications
	go func() {
		for event := range connStatusChan {
			log.Println("notification: ", event)
		}
	}()

	// callbacks
	onDown := func(name string, err OptionalError) bool {
		log.Printf("callback: {%s} went down with {%s}", name, err)
		return true // want continuing
	}
	onUp := func(name string) {
		log.Printf("callback: {%s} went up", name)
	}
	onReconnect := func(name string, retry int) bool {
		log.Printf("callback: connection establish {%s} retry count {%d}", name, retry)
		return true // want continuing
	}

	// usually one global connection per application
	conn := NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		WithConnectionOptionContext(ctxMaster),
		WithConnectionOptionName("conn.main.example"),
		WithConnectionOptionNotification(connStatusChan),
		WithConnectionOptionDown(onDown),
		WithConnectionOptionUp(onUp),
		WithConnectionOptionRecovering(onReconnect),
		WithConnectionOptionDelay(DefaultDelayer{Value: 750}),
	)
	// use it for creating publishers and consumers

	// clean close & terminate on demand
	conn.Close()
	// alternatively, environment closing, clean up, etc.
	ctxCancel()
}
