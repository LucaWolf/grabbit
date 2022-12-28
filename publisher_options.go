package grabbit

// PublisherOptions wraps the topology () and the channel options.
type obsolete_PublisherOptions struct {
	ExchangeOptions *TopologyOptions        // desired topology
	ChanOptions     []func(*ChannelOptions) // array of chan options
}
