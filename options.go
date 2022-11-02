package grabbit

import "time"

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

type ConnectionOptions struct {
	notifier    chan Event     // feedback channel
	name        string         // tag for this connection
	credentials SecretProvider // value for UpdateSecret()
	delayer     DelayProvider  // how much to wait between re-attempts
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
