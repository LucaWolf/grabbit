package grabbit

import (
	"sync"
)

type ClientType int

//go:generate stringer -type=ClientType -trimprefix=Cli
const (
	CliConnection ClientType = iota
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
)

type Event struct {
	SourceType ClientType // origin type
	SourceName string     // origin tag
	Kind       EventType  // type of event
	Err        error      // low level error
}

func RaiseEvent(ch chan Event, event Event) {
	select {
	case ch <- event:
	default:
		// chan has not enough capacity, dump this alert
		_ = event
	}
}

type SafeBool struct {
	Value bool
	mu    sync.RWMutex
}
