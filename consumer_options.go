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
	ConsumerArgs      amqp.Table    // core properties
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

// WithName sets the name of the ConsumerOptions.
//
// name: the name to set for the ConsumerOptions.
// return: the updated ConsumerOptions.
func (opt *ConsumerOptions) WithName(name string) *ConsumerOptions {
	opt.ConsumerName = name
	return opt
}

// WithPrefetchTimeout sets the prefetch timeout for the ConsumerOptions struct.
//
// timeout - The duration of the prefetch timeout.
// Returns the updated ConsumerOptions struct.
func (opt *ConsumerOptions) WithPrefetchTimeout(timeout time.Duration) *ConsumerOptions {
	opt.PrefetchTimeout = timeout
	return opt
}

// WithPrefetchCount sets the prefetch count for the ConsumerOptions.
//
// count: the number of messages to prefetch.
// returns: a pointer to the updated ConsumerOptions.
func (opt *ConsumerOptions) WithPrefetchCount(count int) *ConsumerOptions {
	opt.PrefetchCount = count
	return opt
}

// WithPrefetchSize sets the prefetch size for the ConsumerOptions struct.
//
// It takes an integer `size` as a parameter and sets the PrefetchSize field of the ConsumerOptions struct to that value.
// It returns a pointer to the modified ConsumerOptions struct.
func (opt *ConsumerOptions) WithPrefetchSize(size int) *ConsumerOptions {
	opt.PrefetchSize = size
	return opt
}

// WithQosGlobal sets the global QoS option for the ConsumerOptions struct.
//
// It takes a boolean value, `global`, to determine whether the QoS option should be set globally.
// The function returns a pointer to the updated ConsumerOptions struct.
func (opt *ConsumerOptions) WithQosGlobal(global bool) *ConsumerOptions {
	opt.QosGlobal = global
	return opt
}

// WithQueue sets the consumer queue for the ConsumerOptions struct.
//
// queue: the name of the queue.
// returns: the updated ConsumerOptions struct.
func (opt *ConsumerOptions) WithQueue(queue string) *ConsumerOptions {
	opt.ConsumerQueue = queue
	return opt
}

// WithAutoAck sets the ConsumerAutoAck field of the ConsumerOptions struct
// to the provided boolean value.
//
// autoAck: A boolean value indicating whether the consumer should automatically
// acknowledge messages.
//
// *ConsumerOptions: A pointer to the ConsumerOptions struct.
// Returns: A pointer to the updated ConsumerOptions struct.
func (opt *ConsumerOptions) WithAutoAck(autoAck bool) *ConsumerOptions {
	opt.ConsumerAutoAck = autoAck
	return opt
}

// WithExclusive sets the exclusive flag for the ConsumerOptions.
//
// exclusive: a boolean indicating whether the ConsumerOptions should be exclusive.
// Returns a pointer to the updated ConsumerOptions.
func (opt *ConsumerOptions) WithExclusive(exclusive bool) *ConsumerOptions {
	opt.ConsumerExclusive = exclusive
	return opt
}

// WithNoLocal sets the ConsumerNoLocal field of the ConsumerOptions struct.
//
// It takes a boolean parameter named noLocal.
// It returns a pointer to the ConsumerOptions struct.
func (opt *ConsumerOptions) WithNoLocal(noLocal bool) *ConsumerOptions {
	opt.ConsumerNoLocal = noLocal
	return opt
}

// WithNoWait sets the ConsumerNoWait field of ConsumerOptions struct and returns the modified ConsumerOptions object.
//
// Parameters:
// - noWait: a boolean value indicating whether the consumer should wait or not.
//
// Return type:
// - *ConsumerOptions: the modified ConsumerOptions object.
func (opt *ConsumerOptions) WithNoWait(noWait bool) *ConsumerOptions {
	opt.ConsumerNoWait = noWait
	return opt
}

// WithArgs sets the arguments for the consumer options.
//
// args: The arguments to be set.
// Returns: The updated consumer options.
func (opt *ConsumerOptions) WithArgs(args amqp.Table) *ConsumerOptions {
	opt.ConsumerArgs = args
	return opt
}
