package grabbit

import (
	"context"
)

type ConnectionOptions struct {
	notifier    chan Event             // feedback channel
	name        string                 // tag for this connection
	credentials SecretProvider         // value for UpdateSecret()
	delayer     DelayProvider          // how much to wait between re-attempts
	cbDown      CallbackWhenDown       // callback on conn lost
	cbUp        CallbackWhenUp         // callback when conn recovered
	cbReconnect CallbackWhenRecovering // callback when recovering
	ctx         context.Context        // cancellation context
}

func WithConnectionOptionDown(down CallbackWhenDown) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbDown = down
	}
}
func WithConnectionOptionUp(up CallbackWhenUp) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbUp = up
	}
}
func WithConnectionOptionRecovering(recover CallbackWhenRecovering) func(options *ConnectionOptions) {
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
