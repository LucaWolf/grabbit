package grabbit

import (
	"context"
)

// ChanUsageParameters embeds [PublisherUsageOptions] and [ConsumerUsageOptions].
// It is a private member of the ChannelOptions and cen be passed
// via [WithChannelOptionUsageParams].
type ChanUsageParameters struct {
	PublisherUsageOptions
	ConsumerUsageOptions
}

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

// WithChannelOptionNotification provides an application defined
// [Event] receiver to handle various alerts about the channel status.
func WithChannelOptionNotification(ch chan Event) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.notifier = ch
	}
}

func WithChannelOptionTopology(topology []*TopologyOptions) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.topology = topology
	}
}

func WithChannelOptionNotifyPublish(publishNotifier CallbackNotifyPublish) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbNotifyPublish = publishNotifier
	}
}

func WithChannelOptionNotifyReturn(returnNotifier CallbackNotifyReturn) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbNotifyReturn = returnNotifier
	}
}

func WithChannelOptionProcessor(proc CallbackProcessMessages) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.cbProcessMessages = proc
	}
}

func WithChannelOptionUsageParams(params ChanUsageParameters) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.implParams = params

	}
}
