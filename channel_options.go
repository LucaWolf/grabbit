package grabbit

import "context"

type ChannelOptions struct {
	notifier    chan Event             // feedback channel
	name        string                 // tag for this connection
	delayer     DelayProvider          // how much to wait between re-attempts
	cbDown      CallbackWhenDown       // callback on conn lost
	cbUp        CallbackWhenUp         // callback when conn recovered
	cbReconnect CallbackWhenRecovering // callback when recovering
	ctx         context.Context        // cancellation context
}

func WithChannelOptionDown(down CallbackWhenDown) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbDown = down
	}
}
func WithChannelOptionUp(up CallbackWhenUp) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbUp = up
	}
}
func WithChannelOptionRecovering(recover CallbackWhenRecovering) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbReconnect = recover
	}
}
func WithChannelOptionContext(ctx context.Context) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.ctx = ctx
	}
}

func WithChannelOptionDelay(delayer DelayProvider) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.delayer = delayer
	}
}

func WithChannelOptionName(name string) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.name = name
	}
}

func WithChannelOptionNotification(ch chan Event) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.notifier = ch
	}
}
