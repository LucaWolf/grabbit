package grabbit

import (
	"context"
	"time"
)

// SecretProvider allows passing a bespoke method for providing the
// secret required when connecting to the Rabbit engine.
// See WithConnectionOptionPassword
type SecretProvider interface {
	Password() (string, error)
}

// DelayProvider allows passing a bespoke method for providing the
// delay policy for waiting between reconnection attempts.
// See WithConnectionOptionDelay, WithChannelOptionDelay
type DelayProvider interface {
	Delay(retry int) time.Duration
}

// DefaultDelayer allows defining a basic (constant) delay policy.
// The implementation defaults used by new connections and channels
// has a value of 7.5 seconds
type DefaultDelayer struct {
	Value time.Duration
}

func (delayer DefaultDelayer) Delay(retry int) time.Duration {
	return delayer.Value
}

// CallbackWhenDown defines a function used when connection was lost
// Returns false when want aborting this connection.
type CallbackWhenDown func(name string, err error) bool

// CallbackWhenUp defines a function used after a successful connection recovery
type CallbackWhenUp func(name string)

// CallbackWhenRecovering defines a function used prior to recovering a connection.
// Returns false when want aborting this connection.
type CallbackWhenRecovering func(name string, retry int) bool

func callbackAllowedRecovery(cb CallbackWhenRecovering, name string, attempt int) bool {
	return cb == nil || cb(name, attempt)
}

func callbackAllowedDown(cb CallbackWhenDown, name string, err error) bool {
	return cb == nil || cb(name, err)
}

func callbackDoUp(want bool, cb CallbackWhenUp, name string) {
	if want && cb != nil {
		cb(name)
	}
}

func delayerCompleted(ctx context.Context, delayer DelayProvider, attempt int) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(delayer.Delay(attempt)):
	}

	return true
}
