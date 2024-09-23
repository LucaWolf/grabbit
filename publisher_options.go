package grabbit

import "context"

// PublisherUsageOptions defines parameters for driving the publishers
// behavior and indicating to the supporting channel that publishing
// operations are enabled.
type PublisherUsageOptions struct {
	ConfirmationCount  int  // size of publishing confirmations over the amqp channel
	ConfirmationNoWait bool // publisher confirmation mode parameter
	IsPublisher        bool // indicates if this chan is used for publishing

}

// PublisherOptions defines publisher specific parameters. Mostly used as defaults for
// sending messages and inner channel functionality.
type PublisherOptions struct {
	PublisherUsageOptions
	Context   context.Context // controlling environment
	Exchange  string          // routing exchange
	Key       string          // routing key (usually queue name)
	Mandatory bool            // delivery is mandatory
	Immediate bool            // delivery is immediate
}

// DefaultPublisherOptions creates some sane defaults for publishing messages.
// Note: The Message/payload itself must still be an amqp.Publishing object,
// fully under application's control.
func DefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		PublisherUsageOptions: PublisherUsageOptions{
			ConfirmationCount:  10,
			ConfirmationNoWait: false,
			IsPublisher:        true,
		},
		Context:   context.TODO(),
		Exchange:  "",
		Key:       "",
		Mandatory: false,
		Immediate: false,
	}
}

// WithConfirmationNoWait sets publisher's confirmation mode.
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithConfirmationNoWait(confNoWait bool) *PublisherOptions {
	opt.ConfirmationNoWait = confNoWait
	return opt
}

// WithContext sets the publisher's context.
//
// This context is specific to publishing operations and may be different than the supporting channel's context.
// We still recommended using the same value for both though unless you want strict control of e.g.
// [Publish, PublishWithOptions, PublishDeferredConfirm, PublishDeferredConfirmWithOptions, AwaitDeferredConfirmation].
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithContext(ctx context.Context) *PublisherOptions {
	opt.Context = ctx
	return opt
}

// WithExchange sets the publisher's routing exchange.
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithExchange(exchange string) *PublisherOptions {
	opt.Exchange = exchange
	return opt
}

// WithKey sets the publisher's routing key.
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithKey(key string) *PublisherOptions {
	opt.Key = key
	return opt
}

// WithMandatory sets the deliveries being mandatory flag.
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithMandatory(mandatory bool) *PublisherOptions {
	opt.Mandatory = mandatory
	return opt
}

// WithImmediate sets the deliveries being immediate flag.
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithImmediate(immediate bool) *PublisherOptions {
	opt.Immediate = immediate
	return opt
}

// WithConfirmationsCount sets the number of confirmations required (capacity of amqp.Confirmation).
// Returns the updated PublisherOptions.
func (opt *PublisherOptions) WithConfirmationsCount(count int) *PublisherOptions {
	opt.ConfirmationCount = count
	return opt
}
