package grabbit

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ExampleNewPublisher() {
	// usually one global connection per application
	conn := NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		WithConnectionOptionContext(context.TODO()),
		WithConnectionOptionName("conn.example"),
	)

	// create a 'logs' direct exchange and route 'alert' into 'pagers' queue
	topos := make([]*TopologyOptions, 0, 8)
	topos = append(topos, &TopologyOptions{
		Name:          "logs",
		Declare:       true,
		IsExchange:    true,
		IsDestination: true,
		Kind:          "direct",
	})
	topos = append(topos, &TopologyOptions{
		Name:    "pagers",
		Declare: true,
		Bind: TopologyBind{
			Enabled: true,
			Peer:    "logs",
			Key:     "alert",
		},
	})

	opt := DefaultPublisherOptions()
	opt.WithExchange("").WithKey("pagers") // direct into queue

	publisher := NewPublisher(conn, opt,
		WithChannelOptionName("pub.chan.alert"),
		WithChannelOptionTopology(topos),
	)

	message := amqp.Publishing{
		Body: []byte("some alert payload"),
	}
	// the cached publisher options allow direct access to "pagers" queue
	// but the events will not indicate the correct queue (IsDestination is false)
	// and ch.Queue() will return the exchange name instead (IsDestination is true).
	publisher.Publish(message)
	// better: since we have a complex topology we prefer routing with explicit routing options
	opt.WithExchange("logs").WithKey("alert")
	publisher.PublishWithOptions(opt, message)
}
