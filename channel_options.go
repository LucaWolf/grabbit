package grabbit

import "context"

type OnChannelDown OnConnectionDown
type OnChannelUp OnConnectionUp
type OnChannelAttempt OnConnectionAttempt

type ChannelOptions struct {
	notifier    chan Event       // feedback channel
	name        string           // tag for this connection
	delayer     DelayProvider    // how much to wait between re-attempts
	cbDown      OnChannelDown    // callback on conn lost
	cbUp        OnChannelUp      // callback when conn recovered
	cbReconnect OnChannelAttempt // callback when recovering
	ctx         context.Context  // cancellation context
}

func WithChannelOptionContext(ctx context.Context) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.ctx = ctx
	}
}
