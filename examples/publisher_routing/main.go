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
	ChannelName    = "chan.publisher.routing.example"
	KeyAlerts      = "alert"
	KeyInfo        = "info"
	QueueAlerts    = "pagers"
	QueueInfo      = "emails"
	ExchangeName   = "log"
)

// CallbackWhenDown
func OnDown(name string, err grabbit.OptionalError) bool {
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
	log.Printf("callback: publish confirmed [%v] from queue [%s]\n", confirm, ch.Queue())
}

// CallbackNotifyReturn
func OnNotifyReturn(confirm amqp.Return, ch *grabbit.Channel) {
	log.Printf("callback: publish returned from queue [%s]\n", ch.Queue())
}

func publishSomeLogs(publisher *grabbit.Publisher,
	opt grabbit.PublisherOptions,
	start, end int) {
	message := amqp.Publishing{}
	data := make([]byte, 0, 64)
	buff := bytes.NewBuffer(data)

	for i := start; i < end; i++ {
		<-time.After(3 * time.Second)
		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s number %04d", opt.Key, i))

		log.Println("going to send:", buff.String())

		message.Body = buff.Bytes()
		if err := publisher.PublishWithOptions(opt, message); err != nil {
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

	opt := grabbit.DefaultPublisherOptions()
	opt.WithContext(ctxMaster).WithConfirmationsCount(20)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	// create an ephemeral logs exchange
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          ExchangeName,
		Declare:       true,
		IsExchange:    true,
		IsDestination: true,
		Kind:          "direct",
	})
	// create an ephemeral 'pagers' queue, bound to 'logs' exchange and route 'alert'
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QueueAlerts,
		Declare: true,
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    ExchangeName,
			Key:     KeyAlerts,
		},
	})
	// create an ephemeral 'email' queue, bound to 'logs' exchange and route 'info'
	topos = append(topos, &grabbit.TopologyOptions{
		Name:    QueueInfo,
		Declare: true,
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    ExchangeName,
			Key:     KeyInfo,
		},
	})

	publisher := grabbit.NewPublisher(conn, opt,
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

	// these should end up on the QueueAlerts
	opt.WithExchange(ExchangeName).WithKey(KeyAlerts)
	publishSomeLogs(publisher, opt, 0, 5)
	// these should end up on the QueueInfo
	opt.WithExchange(ExchangeName).WithKey(KeyInfo)
	publishSomeLogs(publisher, opt, 0, 5)

	defer func() {
		ctxCancel()
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
	// if we restart the Rabbit engine we should get
	// all the topology recreated when conn/channels recovery has completed
	<-done
}
