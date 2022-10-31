package grabbit

import (
	"fmt"
	"sync"
)

type ClientType int

const (
	CliConnection ClientType = iota // connection
	CliConsumer              = 1    // consumer
	CliPublisher             = 2    // publisher
)

type EventType int

const (
	EventUp EventType = iota
	EventDown
	EventCannotRecoverYet
	EventBlocked
	EventClosed
)

type Event struct {
	SourceType ClientType // origin type
	SourceName string     // origin tag
	Kind       EventType  // type of event
	Err        error      // low level error
}

func RaiseEvent(ch chan Event, event Event) {
	// TODO poll test if ch can takes new data then push
	// otherwise just discard the message
	fmt.Printf("Got event: %v\n", event)
}

type SafeBool struct {
	Value bool
	mu    sync.RWMutex
}
