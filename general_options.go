package grabbit

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SecretProvider allows passing a bespoke method for providing the
// secret required when connecting to the Rabbit engine.
// See [WithConnectionOptionPassword].
type SecretProvider interface {
	Password() (string, error)
}

// DelayProvider allows passing a bespoke method for providing the
// delay policy for waiting between reconnection attempts.
// See [WithConnectionOptionDelay], [WithChannelOptionDelay]. TIP:
// one could pass en exponential delayer derived from the 'retry' counter.
type DelayProvider interface {
	Delay(retry int) time.Duration
}

// DefaultDelayer allows defining a basic (constant) delay policy.
// The implementation defaults used by new connections and channels
// has a value of 7.5 seconds.
type DefaultDelayer struct {
	Value time.Duration
}

// Delay implements the DelayProvider i/face for the DefaultDelayer.
func (delayer DefaultDelayer) Delay(retry int) time.Duration {
	return delayer.Value
}

// CallbackWhenDown defines a function type used when connection was lost.
// Returns false when want aborting this connection.
// Pass your implementations via [WithChannelOptionDown] and [WithConnectionOptionDown].
type CallbackWhenDown func(name string, err OptionalError) bool

// CallbackWhenUp defines a function type used after a successful connection or channel recovery.
// Applications can define their own handler and pass it via
// [WithConnectionOptionUp] and [WithChannelOptionUp].
type CallbackWhenUp func(name string)

// CallbackNotifyPublish defines a function type for handling the publish notifications.
// Applications can define their own handler and pass it via [WithChannelOptionNotifyPublish].
type CallbackNotifyPublish func(confirm amqp.Confirmation, ch *Channel)

// CallbackNotifyReturn defines a function type for handling the return notifications.
// Applications can define their own handler and pass it via [WithChannelOptionNotifyReturn].
type CallbackNotifyReturn func(confirm amqp.Return, ch *Channel)

// DeliveriesRange indicates the first and last DeliveryTag of the received [Delivery].
// One could make use of this in the and bulk acknowledge (or reject)
// the deliveries via the provided channel (see [CallbackProcessMessages]).
type DeliveriesRange struct {
	First   uint64 // delivery Tag of the first msg in the batch
	Last    uint64 // delivery Tag of the last msg in the batch
	MustAck bool   // manual Ack/Nak is required
}

// DeliveriesProperties captures the common attributes of multiple commonly grouped
// (i.e. received over same channel in one go) deliveries. It is an incomplete [amqp.Delivery]
type DeliveriesProperties struct {
	// Acknowledger amqp.Acknowledger // the channel from which this delivery arrived
	Headers amqp.Table // Application or header exchange table

	// Properties
	ContentType     string // MIME content type
	ContentEncoding string // MIME content encoding
	DeliveryMode    uint8  // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8  // queue implementation use - 0 to 9
	Expiration      string // implementation use - message expiration spec

	ConsumerTag string
	Exchange    string // basic.publish exchange
	RoutingKey  string // basic.publish routing key
}

// From fills the 'prop' attributes with the values from
// the original amqp delivery.
func (prop *DeliveriesProperties) From(d *amqp.Delivery) {
	// prop.Acknowledger = d.Acknowledger
	prop.Headers = d.Headers
	prop.ContentType = d.ContentType
	prop.ContentEncoding = d.ContentEncoding
	prop.DeliveryMode = d.DeliveryMode
	prop.Priority = d.Priority
	prop.Expiration = d.Expiration
	prop.ConsumerTag = d.ConsumerTag
	prop.Exchange = d.Exchange
	prop.RoutingKey = d.RoutingKey
}

// DeliveryPayload subtypes the actual content of deliveries
type DeliveryPayload []byte

// CallbackProcessMessages defines a user passed function for processing the received messages.
// Applications can define their own handler and pass it via [WithChannelOptionProcessor].
type CallbackProcessMessages func(props *DeliveriesProperties, tags DeliveriesRange, messages []DeliveryPayload, ch *Channel)

// CallbackWhenRecovering defines a function used prior to recovering a connection.
// Returns false when want aborting this connection.
// Applications can define their own handler and pass it via
// [WithChannelOptionRecovering] and [WithConnectionOptionRecovering].
type CallbackWhenRecovering func(name string, retry int) bool

// callbackAllowedRecovery performs the user test
// (when provided via WithConnectionOptionRecovering, WithChannelOptionRecovering)
// for allowing the recovery process. Returning 'false' will break out the reconnecting loop
// (impl.details chanReconnectLoop, connReconnectLoop).
func callbackAllowedRecovery(cb CallbackWhenRecovering, name string, attempt int) bool {
	return cb == nil || cb(name, attempt)
}

// callbackAllowedDown performs the user test
// (when provided via [WithChannelOptionDown], [WithConnectionOptionDown])
// for allowing continuing to the recovery process. Returning 'false' will break out the reconnecting loop
// (impl.details connRecover, chanRecover).
func callbackAllowedDown(cb CallbackWhenDown, name string, err OptionalError) bool {
	return cb == nil || cb(name, err)
}

// callbackDoUp performs the user action (when provided via WithChannelOptionUp, WithConnectionOptionUp)
// as part of completion of a new connection (chanReconnectLoop->chanGetNew) or
// channel (chanReconnectLoop->chanGetNew).
func callbackDoUp(want bool, cb CallbackWhenUp, name string) {
	if want && cb != nil {
		cb(name)
	}
}

// delayerCompleted waits for the provided (WithConnectionOptionDelay, WithChannelOptionDelay)
// or default (DefaultDelayer) timing-out policy to complete as part of the recovery loop
// (see chanReconnectLoop, connReconnectLoop).
func delayerCompleted(ctx context.Context, delayer DelayProvider, attempt int) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delayer.Delay(attempt)):
	}

	return true
}
