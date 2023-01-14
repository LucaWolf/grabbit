package grabbit

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumerUsageOptions defines parameters for driving the consumers
// behavior and indicating to the supporting channel to start consuming.
type ConsumerUsageOptions struct {
	IsConsumer        bool          // indicates if this chan is used for consuming
	ConsumerName      string        // chanel wide consumers unique identifier
	PrefetchTimeout   time.Duration // how long to wait for PrefetchCount messages to arrive
	PrefetchCount     int           // Qos count
	PrefetchSize      int           // Qos payload size
	QosGlobal         bool          // all future channels
	ConsumerQueue     string        // queue name from which to receive. Overridden by engine assigned name.
	ConsumerAutoAck   bool          // see [amqp.Consume]
	ConsumerExclusive bool          // see [amqp.Consume]
	ConsumerNoLocal   bool          // see [amqp.Consume]
	ConsumerNoWait    bool          // see [amqp.Consume]
	ConsumerArgs      amqp.Table    // core proprieties
}

type ConsumerOptions struct {
	ConsumerUsageOptions
}

// RandConsumerName creates a random string for the consumers.
// It is used internally by DefaultConsumerOptions by setting the
// 'ConsumerName' property of [ConsumerOptions]
func RandConsumerName() string {
	randBytes := make([]byte, 8)
	rand.Read(randBytes)
	return hex.EncodeToString(randBytes)
}

// DefaultConsumerOptions creates some sane defaults for consuming messages.
func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		ConsumerUsageOptions: ConsumerUsageOptions{
			IsConsumer:    true,
			PrefetchCount: 1,
			PrefetchSize:  0,
			// enforce it, otherwise we lose track of server assigned value and
			// cannot cancel consumer afterwards
			ConsumerName: RandConsumerName(),
		},
	}
}

func (opt *ConsumerOptions) WithName(name string) *ConsumerOptions {
	opt.ConsumerName = name
	return opt
}

func (opt *ConsumerOptions) WithPrefetchTimeout(timeout time.Duration) *ConsumerOptions {
	opt.PrefetchTimeout = timeout
	return opt
}

func (opt *ConsumerOptions) WithPrefetchCount(count int) *ConsumerOptions {
	opt.PrefetchCount = count
	return opt
}

func (opt *ConsumerOptions) WithPrefetchSize(size int) *ConsumerOptions {
	opt.PrefetchSize = size
	return opt
}

func (opt *ConsumerOptions) WithQosGlobal(global bool) *ConsumerOptions {
	opt.QosGlobal = global
	return opt
}

func (opt *ConsumerOptions) WithQueue(queue string) *ConsumerOptions {
	opt.ConsumerQueue = queue
	return opt
}

func (opt *ConsumerOptions) WithAutoAck(autoAck bool) *ConsumerOptions {
	opt.ConsumerAutoAck = autoAck
	return opt
}

func (opt *ConsumerOptions) WithExclusive(exclusive bool) *ConsumerOptions {
	opt.ConsumerExclusive = exclusive
	return opt
}

func (opt *ConsumerOptions) WithNoLocal(noLocal bool) *ConsumerOptions {
	opt.ConsumerNoLocal = noLocal
	return opt
}

func (opt *ConsumerOptions) WithNoWait(noWait bool) *ConsumerOptions {
	opt.ConsumerNoWait = noWait
	return opt
}

func (opt *ConsumerOptions) WithArgs(args amqp.Table) *ConsumerOptions {
	opt.ConsumerArgs = args
	return opt
}
