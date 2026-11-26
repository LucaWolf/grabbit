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
	IsConsumer           bool          // indicates if this chan is used for consuming
	ConsumerName         string        // chanel wide consumers unique identifier
	PrefetchTimeout      time.Duration // how long to wait for PrefetchCount messages to arrive
	PrefetchCount        int           // Qos count
	PrefetchSize         int           // Qos payload size
	ProcessorConcurrency int           // >0 per-record dispatch with N workers; 0 or negative = sequential batches
	QosGlobal            bool          // apply the prefetch window to all consumers on the channel (deprecated on RabbitMQ 4.3+)
	ConsumerQueue        string        // queue name from which to receive. Overridden by engine assigned name.
	ConsumerAutoAck      bool          // see [amqp.Consume]
	ConsumerExclusive    bool          // see [amqp.Consume]
	ConsumerNoLocal      bool          // see [amqp.Consume]
	ConsumerNoWait       bool          // see [amqp.Consume]
	ConsumerArgs         amqp.Table    // core properties
}

type ConsumerOptions struct {
	ConsumerUsageOptions
}

// RandConsumerName creates a random string for the consumers label.
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

// WithName assigns a label/name to the consumer.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithName(name string) *ConsumerOptions {
	opt.ConsumerName = name
	return opt
}

// WithPrefetchTimeout sets the consumer's prefetch timeout.
//
// The duration of the prefetch timeout will kick-in after messages are no longer received.
// It either sends a `EventDataExhausted` (when no more messages) or a `EventDataPartial` when fewer messages
// than expected [WithPrefetchCount] are received. Partial batches are still processed.
//
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithPrefetchTimeout(timeout time.Duration) *ConsumerOptions {
	opt.PrefetchTimeout = timeout
	return opt
}

// WithPrefetchCount sets the consumer's prefetch count.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithPrefetchCount(count int) *ConsumerOptions {
	opt.PrefetchCount = count
	return opt
}

// WithProcessorConcurrency switches the consumer channel to per-record
// concurrent processing with at most count workers.
//
// Zero or negative keeps the default sequential batch path unchanged. When
// positive:
//   - deliveries are dispatched to workers as they arrive; the batch hook
//     registered via [WithChannelOptionProcessor] is ignored, and there is no
//     batch fill, no PrefetchTimeout timer, and no EventDataPartial or
//     EventDataExhausted events;
//   - the record callback registered via [WithChannelOptionProcessRecord] runs
//     once per message and the library settles its [RecordOutcome];
//   - effective parallelism is min(count, PrefetchCount) under manual
//     acknowledgments; with [ConsumerOptions.WithAutoAck] the worker pool is the
//     only bound;
//   - PrefetchCount <= 0 is outside the supported contract: 0 removes the
//     broker's prefetch window (unlimited) while the pool still caps callbacks
//     at count;
//   - consecutive records may be processed concurrently, so the callback must
//     be concurrency-safe and must not rely on processing order.
//
// WARNING: under manual acknowledgment (the default) this option is effectively
// inert at the default PrefetchCount of 1, because the broker allows only one
// unacknowledged delivery in flight. Auto-ack ignores prefetch, so the worker
// pool remains the bound there.
//
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithProcessorConcurrency(count int) *ConsumerOptions {
	opt.ProcessorConcurrency = count
	return opt
}

// WithPrefetchSize sets the consumer's prefetch size.
// Not supported by RabbitMQ and seems blocking the consumers. AVOID using it!
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithPrefetchSize(size int) *ConsumerOptions {
	opt.PrefetchSize = size
	return opt
}

// WithQosGlobal applies the prefetch window globally to all consumers on the
// channel instead of per consumer (RabbitMQ's global QoS is per-channel, not
// per-connection). grabbit consumers each own a channel, so this does not widen
// the channel's bound; RabbitMQ 4.3+ deprecates and disables global QoS by
// default. Leave it at the false default.
//
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithQosGlobal(global bool) *ConsumerOptions {
	opt.QosGlobal = global
	return opt
}

// WithQueue sets the consumer queue name.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithQueue(queue string) *ConsumerOptions {
	opt.ConsumerQueue = queue
	return opt
}

// WithAutoAck sets whether the consumer should automatically acknowledge messages.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithAutoAck(autoAck bool) *ConsumerOptions {
	opt.ConsumerAutoAck = autoAck
	return opt
}

// WithExclusive sets whether the consuming mode should be exclusive.
// Returns the updated ConsumerOptions.
//
// WARNING: a true exclusive consumer blocks all other consumers on the same queue
// regardless of (using different) connection and channel.
// Ref: https://github.com/rabbitmq/amqp091-go/issues/253
func (opt *ConsumerOptions) WithExclusive(exclusive bool) *ConsumerOptions {
	opt.ConsumerExclusive = exclusive
	return opt
}

// WithNoLocal sets whether the consuming mode should be non-local.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithNoLocal(noLocal bool) *ConsumerOptions {
	opt.ConsumerNoLocal = noLocal
	return opt
}

// WithNoWait sets whether the noWaiting consuming channel mode.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithNoWait(noWait bool) *ConsumerOptions {
	opt.ConsumerNoWait = noWait
	return opt
}

// WithArgs sets the consumer's additional arguments.
// Returns the updated ConsumerOptions.
func (opt *ConsumerOptions) WithArgs(args amqp.Table) *ConsumerOptions {
	opt.ConsumerArgs = args
	return opt
}
