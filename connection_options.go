package grabbit

import (
	"context"
)

// ConnectionOptions defines a collection of attributes used internally
// by the [Connection].
//
// Attributes can be set via optionFuncs parameters of [NewConnection]
// via WithConnectionOption<Fct> family, ex:
// [WithConnectionOptionDown], [WithConnectionOptionContext], [WithConnectionOptionNotification].
type ConnectionOptions struct {
	notifier    chan Event             // status events feedback channel
	name        string                 // tag for this connection
	credentials SecretProvider         // value for UpdateSecret()
	delayer     DelayProvider          // how much to wait between re-attempts
	cbDown      CallbackWhenDown       // callback on conn lost
	cbUp        CallbackWhenUp         // callback when conn recovered
	cbReconnect CallbackWhenRecovering // callback when recovering
	ctx         context.Context        // cancellation context
	cancelCtx   context.CancelFunc     // aborts the reconnect loop
}

// WithConnectionOptionDown stores the application space callback for
// connection down events.
func WithConnectionOptionDown(down CallbackWhenDown) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbDown = down
	}
}

// WithConnectionOptionUp stores the application space callback for
// connection established events.
func WithConnectionOptionUp(up CallbackWhenUp) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbUp = up
	}
}

// WithConnectionOptionRecovering stores the application space callback for
// connection recovering events.
func WithConnectionOptionRecovering(recover CallbackWhenRecovering) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.cbReconnect = recover
	}
}

// WithConnectionOptionContext stores the application provided context.
// Cancelling this context will terminate the recovery loop and also close down the
// connection (and indirectly its channel dependents).
func WithConnectionOptionContext(ctx context.Context) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.ctx = ctx
	}
}

// WithConnectionOptionPassword provides password refresh capabilities
// for dynamically protected services (future IAM)
func WithConnectionOptionPassword(credentials SecretProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.credentials = credentials
	}
}

// WithConnectionOptionDelay provides an application space defined
// delay (between re-connection attempts) policy. An example of
// [DelayProvider] could be an exponential timeout routine based on the
// retry parameter.
func WithConnectionOptionDelay(delayer DelayProvider) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.delayer = delayer
	}
}

// WithConnectionOptionName assigns a tag to this connection.
func WithConnectionOptionName(name string) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.name = name
	}
}

// WithConnectionOptionNotification provides an application defined
// [Event] receiver to handle various alerts about the connection status.
func WithConnectionOptionNotification(ch chan Event) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.notifier = ch
	}
}
