package grabbit

import (
	"context"
)

type ChanUsageParameters struct {
	ConfirmationCount  int  // size of publishing confirmations over the amqp channel
	ConfirmationNoWait bool // publisher confirmation mode parameter
	IsPublisher        bool // indicates if this chan is used for publishing
}

type ChannelOptions struct {
	notifier        chan Event             // feedback channel
	name            string                 // tag for this connection
	delayer         DelayProvider          // how much to wait between re-attempts
	cbDown          CallbackWhenDown       // callback on conn lost
	cbUp            CallbackWhenUp         // callback when conn recovered
	cbReconnect     CallbackWhenRecovering // callback when recovering
	cbNotifyPublish CallbackNotifyPublish  // publish notification handler
	cbNotifyReturn  CallbackNotifyReturn   // returned message notification handler
	topology        []*TopologyOptions     // the _whole_ infrastructure involved as array of queues and exchanges
	implParams      ChanUsageParameters    // implementation trigger for publishers or consumers
	ctx             context.Context        // cancellation context
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

func WithChannelOptionUsageParams(params ChanUsageParameters) func(options *ChannelOptions) {
	return func(options *ChannelOptions) {
		options.implParams = params

	}
}
