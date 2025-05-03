package grabbit

import (
	"context"
)

// ChanUsageParameters embeds [PublisherUsageOptions] and [ConsumerUsageOptions].
// It is a private member of the ChannelOptions and can be passed
// via [WithChannelOptionUsageParams].
type ChanUsageParameters struct {
	PublisherUsageOptions
	ConsumerUsageOptions
}

// ChannelOptions represents the options for configuring a channel.
type ChannelOptions struct {
	notifier          chan Event              // feedback channel
	name              string                  // tag for this channel
	delayer           DelayProvider           // how much to wait between re-attempts
	cbDown            CallbackWhenDown        // callback on conn lost
	cbUp              CallbackWhenUp          // callback when conn recovered
	cbReconnect       CallbackWhenRecovering  // callback when recovering
	cbNotifyPublish   CallbackNotifyPublish   // publish notification handler
	cbNotifyReturn    CallbackNotifyReturn    // returned message notification handler
	cbProcessMessages CallbackProcessMessages // user defined message processing routine
	topology          []*TopologyOptions      // the _whole_ infrastructure involved as array of queues and exchanges
	implParams        ChanUsageParameters     // implementation trigger for publishers or consumers
	ctx               context.Context         // cancellation context
	cancelCtx         context.CancelFunc      // aborts the reconnect loop
}

// WithChannelOptionDown stores the application space callback for
// channel's down events.
func WithChannelOptionDown(down CallbackWhenDown) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbDown = down
	}
}

// WithChannelOptionUp stores the application space callback for
// channel's established events.
func WithChannelOptionUp(up CallbackWhenUp) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbUp = up
	}
}

// WithChannelOptionRecovering stores the application space callback for
// channel's recovering events.
func WithChannelOptionRecovering(recover CallbackWhenRecovering) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbReconnect = recover
	}
}

// WithChannelOptionContext stores the application provided context.
// Cancelling this context will terminate the recovery loop, thus closing down the channel.
func WithChannelOptionContext(ctx context.Context) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.ctx = ctx
	}
}

// WithChannelOptionDelay stores the application provided
// delay (between re-connection attempts) policy. An example of
// [DelayProvider] could be an exponential timeout routine based on the
// retry parameter.
func WithChannelOptionDelay(delayer DelayProvider) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.delayer = delayer
	}
}

// WithChannelOptionName assigns a tag to this channel.
func WithChannelOptionName(name string) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.name = name
	}
}

// WithChannelOptionNotification stores the application provided
// [Event] receiver to handle various alerts about the channel status.
func WithChannelOptionNotification(ch chan Event) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.notifier = ch
	}
}

// WithChannelOptionTopology stores the application provided topology options for a channel.
func WithChannelOptionTopology(topology []*TopologyOptions) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.topology = topology
	}
}

// WithChannelOptionNotifyPublish stores the application provided handler
// for publish events notifications .
func WithChannelOptionNotifyPublish(publishNotifier CallbackNotifyPublish) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbNotifyPublish = publishNotifier
	}
}

// WithChannelOptionNotifyReturn stores the application provided handler
// for a returned notifications.
func WithChannelOptionNotifyReturn(returnNotifier CallbackNotifyReturn) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbNotifyReturn = returnNotifier
	}
}

// WithChannelOptionProcessor stores the application provided handler
// for processing received messages.
func WithChannelOptionProcessor(proc CallbackProcessMessages) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbProcessMessages = proc
	}
}

// WithChannelOptionUsageParams stores the provided usage parameters for the specific channel type.
//
// It takes a parameter of type ChanUsageParameters and returns a function that takes a pointer to a ChannelOptions struct.
func WithChannelOptionUsageParams(params ChanUsageParameters) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.implParams = params

	}
}
