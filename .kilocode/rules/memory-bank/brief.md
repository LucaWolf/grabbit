Library that wraps the low level amqp091-go RabbitMQ client library.

It provides managed connections and channels that wrap the amqp library with 
auto-recovery on Rabbit server link error or other exception events.

Channels are used as base for building consumers and publisher higher level objects. All the
core API functionality of the amqp library should be proxied throuh in a concurency safe manner by the
wrapped channels.

The desired goals (working towards key features) of this library are:

  - make use of the latest [amqp091-go](https://github.com/rabbitmq/amqp091-go) library;
  - be able to share a connection between multiple channels
  - have connection and channels auto-recover (on infrastructure failure) via managers
  - replace the internal logging with an alternative. Current thought is to have some
      buffered channel over which detailed events are submitted (non-blocking)
  - have the topology defined by consumers and publishers. Once when creating and then
      during channels recovery (ephemeral queues/exchanges only)
  - provide an optional callback to the caller space during recoveries. This supplements
     in a synchronous (blocking) mode the logging replacement mechanism.
  - awaiting confirmation of the published events via deferred methods (PublishDeferredConfirm
     or PublishDeferredConfirmWithOptions together with AwaitDeferredConfirmation).      
  - consumers to accept user defined handlers for processing the received messages
  - the consumer handlers to also allow batch processing (with support for 
      partial fulfillment of QoS expectations based on a timeout)
  - Bonus: optionally provide the users with access to the low level `amqp.Channel`. **Unsafe**
      initially. **Note**: safety comes for free if using the slightly higher level `grabbit.Channel` wrappers.