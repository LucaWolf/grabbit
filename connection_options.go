package grabbit

import (
	"context"
	"time"
)

type SecretProvider interface {
	Password() (string, error)
}

type DelayProvider interface {
	Delay(retry int) time.Duration
}

type DefaultDelayer struct {
	Value time.Duration
}

func (delayer DefaultDelayer) Delay(retry int) time.Duration {
	return delayer.Value
}

// OnConnectionDown defines a function used when connection was lost
// Returns false when want aborting this connection.
type OnConnectionDown func(name string, err error) bool

// OnConnectionUp defines a function used after a successful connection recovery
type OnConnectionUp func(name string)

// OnConnectionAttempt defines a function used prior to recovering a connection
// Returns false when want aborting this connection.
type OnConnectionAttempt func(name string, retry int) bool

type ConnectionOptions struct {
	notifier    chan Event          // feedback channel
	name        string              // tag for this connection
	credentials SecretProvider      // value for UpdateSecret()
	delayer     DelayProvider       // how much to wait between re-attempts
	cbDown      OnConnectionDown    // callback on conn lost
	cbUp        OnConnectionUp      // callback when conn recovered
	cbReconnect OnConnectionAttempt // callback when recovering
	ctx         context.Context     // cancellation context
}

func WithConnectionOptionDown(down OnConnectionDown) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbDown = down
	}
}
func WithConnectionOptionUp(up OnConnectionUp) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbUp = up
	}
}
func WithConnectionOptionRecovering(recover OnConnectionAttempt) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbReconnect = recover
	}
}

func WithConnectionOptionContext(ctx context.Context) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.ctx = ctx
	}
}

func WithConnectionOptionPassword(credentials SecretProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.credentials = credentials
	}
}

func WithConnectionOptionDelay(delayer DelayProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.delayer = delayer
	}
}

func WithConnectionOptionName(name string) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.name = name
	}
}

func WithConnectionOptionNotification(ch chan Event) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.notifier = ch
	}
}
