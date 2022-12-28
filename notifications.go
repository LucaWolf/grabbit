package grabbit

import (
	"sync"
)

type SafeBool struct {
	Value bool
	mu    sync.RWMutex
}

type ClientType int

//go:generate stringer -type=ClientType -trimprefix=Cli
const (
	CliConnection ClientType = iota
	CliChannel
	CliConsumer
	CliPublisher
)

type EventType int

//go:generate stringer -type=EventType -trimprefix=Event
const (
	EventUp EventType = iota
	EventDown
	EventCannotEstablish
	EventBlocked
	EventUnBlocked
	EventClosed
	EventMessagePublished
	EventMessageReturned
	EventDefineTopology
)

type Event struct {
	SourceType ClientType // origin type
	SourceName string     // origin tag
	Kind       EventType  // type of event
	Err        error      // low level error
}

// RaiseEvent pushes an event type from a particular connection or channel
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
