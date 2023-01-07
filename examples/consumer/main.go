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
	ExchangeName   = "log"
)

// CallbackWhenDown
func OnDown(name string, err error) bool {
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
		<-time.After(250 * time.Millisecond)
		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s number %04d", opt.Key, i))

		log.Println("going to send:", buff.String())

		message.Body = buff.Bytes()
		if err := publisher.PublishWithOptions(opt, message); err != nil {
			log.Println("publishing failed with: ", err)
		}
	}
}

func MsgHandler(props *grabbit.DeliveriesProperties, tags grabbit.DeliveriesRange, messages []grabbit.DeliveryPayload, ch *grabbit.Channel) {
	log.Printf("APP: consumer [%s] received [%d] messages:\n", props.ConsumerTag, len(messages))
	for _, msg := range messages {
		log.Printf("  [%s] -- messages: %s\n", props.ConsumerTag, string(msg))
	}

	if tags.MustAck {
		ch.Channel().Super.Ack(tags.Last, true) // cb executes in library space, no need to lock safeguard
	}
}

func consumeSome(conn *grabbit.Connection, tag string) {
	// create an ephemeral un-named queue, bound to the exchange
	topos := make([]*grabbit.TopologyOptions, 0, 8)
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          "", // leave blank for auto-allocation
		IsDestination: true,
		Declare:       true,
		AutoDelete:    true, // when all clients disconnect
		Bind: grabbit.TopologyBind{
			Enabled: true,
			Peer:    ExchangeName,
		},
	})

	opt := grabbit.DefaultConsumerOptions()

	opt.WithName(tag).
		WithPrefetchCount(3).
		WithQueue("auto_generated_will_take_over"). // IsDestination above
		WithPrefetchTimeout(20 * time.Second)

	// start a consumer
	grabbit.NewConsumer(conn, opt,
		grabbit.WithChannelOptionName("chan:"+tag),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionProcessor(MsgHandler),
	)

	// exit here but the consumer library executes till completion
	// We could implement a waiter here if really wanted: consumer channel will close on timeout
}

func main() {
	events := make(chan grabbit.Event, 10)
	defer close(events)

	ctxMaster, ctxCancel := context.WithCancel(context.TODO())

	// await and log any infrastructure notifications
	go func() {
		for event := range events {
			log.Println("notification: ", event)
			// _ = event
		}
	}()

	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionContext(ctxMaster),
		grabbit.WithConnectionOptionName(ConnectionName),
		grabbit.WithConnectionOptionNotification(events),
		grabbit.WithConnectionOptionDown(OnDown),
		grabbit.WithConnectionOptionUp(OnUp),
		grabbit.WithConnectionOptionRecovering(OnReattempting),
	)

	opt := grabbit.DefaultPublisherOptions()
	opt.WithContext(ctxMaster).WithConfirmationsCount(20)

	topos := make([]*grabbit.TopologyOptions, 0, 8)
	// create an ephemeral fanout type 'logs' exchange
	topos = append(topos, &grabbit.TopologyOptions{
		Name:          ExchangeName,
		Declare:       true,
		IsExchange:    true,
		IsDestination: true,
		Kind:          "fanout",
	})

	publisher := grabbit.NewPublisher(conn, opt,
		grabbit.WithChannelOptionName(ChannelName),
		grabbit.WithChannelOptionTopology(topos),
		grabbit.WithChannelOptionNotifyPublish(OnNotifyPublish),
		grabbit.WithChannelOptionNotifyReturn(OnNotifyReturn),
		// grabbit.WithChannelOptionContext(ctxMaster), -- inherited from connection
		// grabbit.WithChannelOptionDelay(some_delayer), -- inherited from connection
		// grabbit.WithChannelOptionNotification(events), -- inherited from connection
	)

	if !publisher.AwaitAvailable(30*time.Second, 1*time.Second) {
		log.Println("publisher not ready yet")
		ctxCancel()
		<-time.After(7 * time.Second)
		log.Println("EXIT")
		return
	}

	go consumeSome(conn, "CONSUMER--ONE")
	go consumeSome(conn, "CONSUMER--TWO")

	// routine key is ignored for fanout exchanges
	opt.WithExchange(ExchangeName).WithKey("alpha")
	publishSomeLogs(publisher, opt, 1, 10)
	opt.WithExchange(ExchangeName).WithKey("beta")
	publishSomeLogs(publisher, opt, 1, 20)
	opt.WithExchange(ExchangeName).WithKey("gamma")
	publishSomeLogs(publisher, opt, 1, 30)
	opt.WithExchange(ExchangeName).WithKey("delta")
	publishSomeLogs(publisher, opt, 1, 40)

	defer func() {
		ctxCancel()
		<-time.After(3 * time.Second)
		log.Println("EXIT")
	}()

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

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
