package grabbit

import "context"

// PublisherOptions defines publisher specific parameters. Mostly used as defaults for
// sending messages and inner channel functionality.
type PublisherOptions struct {
	ConfirmationCount  int             // size of the amqp channel for publishing confirmations
	ConfirmationNoWait bool            // Confirmation mode parameter
	Context            context.Context // controlling environment
	Exchange           string          // routing exchange
	Key                string          // routing key (usually queue name)
	Mandatory          bool            // delivery is mandatory
	Immediate          bool            // delivery is immediate
}

// DefaultPublisherOptions creates some sane defaults for publishing messages.
// Note: The Message/payload itself must still be an amqp.Publishing object,
// fully under application's control.
func DefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ConfirmationCount:  10,
		ConfirmationNoWait: false,
		Context:            context.TODO(),
		Exchange:           "",
		Key:                "",
		Mandatory:          false,
		Immediate:          false,
	}
}

func (opt *PublisherOptions) WithConfirmationNoWait(confNoWait bool) *PublisherOptions {
	opt.ConfirmationNoWait = confNoWait
	return opt
}

func (opt *PublisherOptions) WithContext(ctx context.Context) *PublisherOptions {
	opt.Context = ctx
	return opt
}

func (opt *PublisherOptions) WithExchange(exchange string) *PublisherOptions {
	opt.Exchange = exchange
	return opt
}

func (opt *PublisherOptions) WithKey(key string) *PublisherOptions {
	opt.Key = key
	return opt
}

func (opt *PublisherOptions) WithMandatory(mandatory bool) *PublisherOptions {
	opt.Mandatory = mandatory
	return opt
}

func (opt *PublisherOptions) WithImmediate(immediate bool) *PublisherOptions {
	opt.Immediate = immediate
	return opt
}

func (opt *PublisherOptions) WithConfirmationsCount(count int) *PublisherOptions {
	opt.ConfirmationCount = count
	return opt
}
