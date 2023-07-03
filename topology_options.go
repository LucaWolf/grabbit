package grabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyBind defines the possible binding relation between exchanges or queues and exchanges.
type TopologyBind struct {
	Enabled bool       // want this re-routing
	Peer    string     // other end of routing
	Key     string     // routing key / filter
	NoWait  bool       // re-routing confirmation required
	Args    amqp.Table // core properties
}

// TopologyOptions defines the infrastructure topology, i.e. exchange and queues definition
// when wanting handling automatically on recovery or one time creation
type TopologyOptions struct {
	Name          string       // tag of exchange or queue
	IsDestination bool         // end target, i.e. if messages should be routed to it
	IsExchange    bool         // indicates if this an exchange or queue
	Bind          TopologyBind // complex routing
	Kind          string       // empty string for default exchange or: direct, topic, fanout, headers.
	Durable       bool         // maps the durable amqp attribute
	AutoDelete    bool         // maps the auto-delete amqp attribute
	Exclusive     bool         // if queue is exclusive
	Internal      bool         //
	NoWait        bool         // // maps the noWait amqp attribute
	Passive       bool         // if false, it will be created on the server when missing
	Args          amqp.Table   // wraps the amqp Table parameters
	Declare       bool         // gets created on start and also during recovery if Durable is false
}

// GetRouting returns the direction of binding two exchanges.
func (t *TopologyOptions) GetRouting() (source, destination string) {
	if t.IsDestination {
		return t.Bind.Peer, t.Name
	} else {
		return t.Name, t.Bind.Peer
	}
}
