# grabbit
🚧 Yet another Go wrapper for the RabbitMQ client 🚧 

## Rationale
This is an alternative library providing auto-reconnection support. It's been heavily inspired by other projects (listed in credits) and my previous experiments. The reason for a new
project (instead of cloning/contributing to an existing one) is that internals may start to 
diverge too much from the original and risk non-adoption.

## Goals
What I'd like this library to provide is:

  * make use of the latest [amqp091-go](https://github.com/rabbitmq/amqp091-go) library;
      this is the up to date version building on [streadway's](https://github.com/streadway/amqp) original work
  * be able to share a connection between multiple channels
  * have connection and channels auto-recover (on infrastructure failure) via managers
  * replace the internal logging with an alternative. Current thought is to have some
      buffered channel over which detailed events are submitted (non-blocking)
  * have the topology defined by consumers and publishers. Once when creating and then
      during channels recovery (ephemeral queues/exchanges only)
  * provide an optional callback to the caller space during recoveries. This supplements
     in a synchronous (blocking) mode the logging replacement mechanism.
  * awaiting confirmation of the published events to be handled within the library. 
      (perhaps allow an user defined function if needed)
  * consumers to accept user defined handlers for processing the received messages
  * the consumer handlers to also allow batch processing (with support for 
      partial fulfillment of QoS expectations based on a timeout)
  * Bonus: optionally provide the users with access to the low level amqp.Channel. **Unsafe**
      initially.

## Non goals

  * not interested in concurrency safety of the channels. Publisher and consumers are relatively cheap, use plenty as needed
    instead of passing them across coroutines.

## Usage
The [examples](https://github.com/LucaWolf/grabbit/blob/main/examples) folder contains sample code for consumes and publishers.

## Credits

  * _wagslane_ from whom I got heavily inspired to do the sane parameters, 
      topology maintenance and consumer handlers. Please browse and star [his repository](https://github.com/wagslane/go-rabbitmq).
  * _Emir Ribic_ for his inspiring post that lead me to think about adding a resilience layer 
      for the RabbitMQ client. You may want to [read the full post](https://www.ribice.ba/golang-rabbitmq-client/)
  * _gbeletti_ from whose [project](https://github.com/gbeletti/rabbitmq) I might pinch a few ideas. Regardless of drawing inspiration or not, his version made for an interesting reading.
