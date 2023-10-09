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

// DeliveriesProperties captures the common attributes of multiple commonly grouped
// (i.e. received over same channel in one go) deliveries. It is an incomplete [amqp.Delivery]
type DeliveriesProperties struct {
	Headers amqp.Table // Application or header exchange table
	// Properties; assume all are common
	ContentType     string // MIME content type
	ContentEncoding string // MIME content encoding
	DeliveryMode    uint8  // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8  // queue implementation use - 0 to 9
	ConsumerTag     string // client tag as provided during consumer registration
	Exchange        string // basic.publish exchange
	RoutingKey      string // basic.publish routing key
}

// DeliveryPropsFrom generates a DeliveriesProperties struct from an amqp.Delivery.
//
// Takes a pointer to an amqp.Delivery as the parameter and returns a DeliveriesProperties struct.
func DeliveryPropsFrom(d *amqp.Delivery) (prop DeliveriesProperties) {
	return DeliveriesProperties{
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		ConsumerTag:     d.ConsumerTag,
		Exchange:        d.Exchange,
		RoutingKey:      d.RoutingKey,
	}
}

// DeliveryPayload subtypes the actual content of deliveries
type DeliveryPayload []byte

// DeliveryData isolates the data part of each specific delivered message
type DeliveryData struct {
	Body        DeliveryPayload // actual data payload
	DeliveryTag uint64          // sequential number of this message
	Redelivered bool            // message has been re-enqueued
	Expiration  string          // message expiration spec
	MessageId   string          // message identifier
	Timestamp   time.Time       // message timestamp
	Type        string          // message type name
	UserId      string          // user of the publishing connection
	AppId       string          // application id
}

// DeliveryDataFrom creates a DeliveryData object from an amqp.Delivery object.
//
// It takes a pointer to an amqp.Delivery object as its parameter and returns a DeliveryData object.
func DeliveryDataFrom(d *amqp.Delivery) (data DeliveryData) {
	return DeliveryData{
		Body:        d.Body,
		DeliveryTag: d.DeliveryTag,
		Redelivered: d.Redelivered,
		Expiration:  d.Expiration,
		MessageId:   d.MessageId,
		Timestamp:   d.Timestamp,
		Type:        d.Type,
		UserId:      d.UserId,
		AppId:       d.AppId,
	}
}

// CallbackProcessMessages defines a user passed function for processing the received messages.
// Applications can define their own handler and pass it via [WithChannelOptionProcessor].
type CallbackProcessMessages func(props *DeliveriesProperties, messages []DeliveryData, mustAck bool, ch *Channel)

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
