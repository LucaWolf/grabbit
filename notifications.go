package grabbit

import (
	"sync"
)

// SafeBool wraps a boolean in a concurrency safe way so it can
// be set, reset and tested from different coroutines.
type SafeBool struct {
	value bool
	mu    sync.RWMutex
}

// ClientType defines the class of objects that interact with the amqp functionality.
// Used mostly for sending alerts about specific functionality areas.
type ClientType int

//go:generate stringer -type=ClientType -trimprefix=Cli
const (
	CliConnection ClientType = iota
	CliChannel
	CliConsumer
	CliPublisher
)

// EventType defines the class of alerts sent to the application layer.
type EventType int

//go:generate stringer -type=EventType -trimprefix=Event
const (
	EventUp EventType = iota
	EventDown
	EventCannotEstablish
	EventBlocked
	EventUnBlocked
	EventClosed
	EventMessageReceived
	EventMessagePublished
	EventMessageReturned
	EventConfirm
	EventQos
	EventConsume
	EventDefineTopology
	EventDataExhausted
)

// Event defines a simple body structure for the alerts received
// via the notification channels passed in [WithChannelOptionNotification]
// and [WithConnectionOptionNotification].
type Event struct {
	SourceType ClientType    // origin type
	SourceName string        // origin tag
	Kind       EventType     // type of event
	Err        OptionalError // low level error
}

// raiseEvent pushes an event type from a particular connection or channel
// over the provided notification channel.
// See WithChannelOptionNotification and WithConnectionOptionNotification
func raiseEvent(ch chan Event, event Event) {
	select {
	case ch <- event:
	default:
		// chan has not enough capacity, dump this alert
		_ = event
	}
}
