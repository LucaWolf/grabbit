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

// WithConfirmationNoWait sets the ConfirmationNoWait field of the PublisherOptions struct.
//
// It takes a boolean parameter `confNoWait` and updates the `ConfirmationNoWait` field of the `PublisherOptions` struct to the value of `confNoWait`.
// It returns a pointer to the `PublisherOptions` struct.
func (opt *PublisherOptions) WithConfirmationNoWait(confNoWait bool) *PublisherOptions {
	opt.ConfirmationNoWait = confNoWait
	return opt
}

// WithContext sets the context for the PublisherOptions.
//
// ctx: The context to be set.
// Returns: A pointer to PublisherOptions.
func (opt *PublisherOptions) WithContext(ctx context.Context) *PublisherOptions {
	opt.Context = ctx
	return opt
}

// WithExchange sets the exchange for the PublisherOptions struct.
//
// Parameters:
// - exchange: The exchange to set.
//
// Returns:
// - *PublisherOptions: The updated PublisherOptions struct.
func (opt *PublisherOptions) WithExchange(exchange string) *PublisherOptions {
	opt.Exchange = exchange
	return opt
}

// WithKey sets the key for the PublisherOptions.
//
// key: the key to set.
// returns: a pointer to the PublisherOptions.
func (opt *PublisherOptions) WithKey(key string) *PublisherOptions {
	opt.Key = key
	return opt
}

// WithMandatory sets the mandatory flag in the PublisherOptions struct.
//
// Parameters:
// - mandatory: a boolean indicating whether the field should be mandatory.
//
// Returns:
// - *PublisherOptions: a pointer to the PublisherOptions struct.
func (opt *PublisherOptions) WithMandatory(mandatory bool) *PublisherOptions {
	opt.Mandatory = mandatory
	return opt
}

// WithImmediate sets the immediate flag of the PublisherOptions struct.
//
// It takes a boolean parameter `immediate` and returns a pointer to the updated PublisherOptions.
func (opt *PublisherOptions) WithImmediate(immediate bool) *PublisherOptions {
	opt.Immediate = immediate
	return opt
}

// WithConfirmationsCount sets the number of confirmations required for publishing.
//
// count: The number of confirmations required.
// *PublisherOptions: The updated PublisherOptions object.
func (opt *PublisherOptions) WithConfirmationsCount(count int) *PublisherOptions {
	opt.ConfirmationCount = count
	return opt
}
