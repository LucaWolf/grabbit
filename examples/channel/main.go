package main

import (
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

func ConnDown(name string, err grabbit.OptionalError) bool {
	// log.Printf("callback_down: {%s} went down with {%s}", name, err)
	return true // want continuing
}

func ConnUp(name string) {
	// log.Printf("callback_up: {%s} went up", name)
}

func ConnReattempting(name string, retry int) bool {
	// log.Printf("callback_redo: {%s} retry count {%d}", name, retry)
	return true // want continuing
}

func DataChanDown(name string, err grabbit.OptionalError) bool {
	log.Printf("callback_down: {%s} went down with {%s}", name, err)
	return true // want continuing
}

func DataChanUp(name string) {
	log.Printf("callback_up: {%s} went up", name)
}

func DataChanReattempting(name string, retry int) bool {
	log.Printf("callback_redo: {%s} retry count {%d}", name, retry)
	return true // want continuing
}

func main() {
	connStatusChan := make(chan grabbit.Event, 32)

	// capture status notifications and perform desired actions based on this
	// like e.g. cancel the global context, adjust metrics, exit the app... whatever your requirements fancy
	go func() {
		for event := range connStatusChan {
			// let's isolate a bit the noise, we want to see only the 'cmd' related notifications
			// log.Print("conn.notification: ", event)
			// process any event by its combination of .SourceType, .SourceName and .Kind
			_ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionName("conn.main"),
		grabbit.WithConnectionOptionNotification(connStatusChan),
		grabbit.WithConnectionOptionDown(ConnDown),
		grabbit.WithConnectionOptionUp(ConnUp),
		grabbit.WithConnectionOptionRecovering(ConnReattempting),
	)

	dataStatusChan := make(chan grabbit.Event, 5)
	ctxData, ctxDataCancel := context.WithCancel(context.TODO())

	dataCh := grabbit.NewChannel(conn,
		grabbit.WithChannelOptionContext(ctxData),
		grabbit.WithChannelOptionName("chan.data"),
		grabbit.WithChannelOptionNotification(dataStatusChan),
		grabbit.WithChannelOptionDown(DataChanDown),
		grabbit.WithChannelOptionUp(DataChanUp),
		grabbit.WithChannelOptionRecovering(DataChanReattempting),
		// we also support these:
		// WithChannelOptionDelay(...)      // see DelayProvider
		// WithChannelOptionNotifyPublish() // see CallbackNotifyPublish, publishers
		// WithChannelOptionNotifyReturn()  // see CallbackNotifyReturn, publishers
		// WithChannelOptionProcessor()     // see CallbackProcessMessages, consumers
		// WithChannelOptionTopology()      // see TopologyOptions, publishers & consumers
		// WithChannelOptionUsageParams()   // see ChanUsageParameters, publishers and consumers
	)

	// data channel related notifications and perform desired actions based on this
	go func(ch *grabbit.Channel) {
		for event := range dataStatusChan {
			log.Print("cmd.notification: ", event)

			if event.Kind == grabbit.EventUp {
				// on error QueueDeclarePassive() throws and kills your channel
				// q, err := dataCh.QueueDeclarePassive("grabbit_demo_data", true, true, false, false, make(amqp.Table))

				q, err := dataCh.QueueDeclare("grabbit_demo_data", false, true, false, false, make(amqp.Table))
				if err != nil {
					log.Print("cmd error: ", err)
				} else {
					log.Printf("queue [%s] has [%d] pending messages", q.Name, q.Messages)
				}
			}
		}
	}(dataCh)

	defer func() {
		fmt.Println("app closing connection and dependencies")
		// -------
		// Note: both close and context cancellation are safe to call multiple times
		// -------
		_ = dataCh.Close() // either/or via closing
		ctxDataCancel()    // either/or via ctx. Warning: no EventDown/EventClosed pushed

		<-time.After(3 * time.Second)
		// reconnect loop should be dead now
		if err := conn.Close(); err != nil {
			log.Print("cannot close conn: ", err)
		}
		<-time.After(3 * time.Second)
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
