# grabbit
Golang wrapper for RabbitMQ managed connections.

Version 1.0.0 and beyond 🚀.<br>

[![Go Reference](https://pkg.go.dev/badge/github.com/LucaWolf/grabbit.svg)](https://pkg.go.dev/github.com/LucaWolf/grabbit)
[![Go Coverage](https://github.com/LucaWolf/grabbit/wiki/coverage.svg)](https://raw.githack.com/wiki/LucaWolf/grabbit/coverage.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/LucaWolf/grabbit)](https://goreportcard.com/report/github.com/LucaWolf/grabbit)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/LucaWolf/grabbit)

## Rationale
This is an alternative library providing auto-reconnection support. It's been heavily inspired by other projects
 (listed in credits) and my previous experiments. 
The reason for a new project (instead of cloning/contributing to an existing one) is that internals may start to 
diverge too much from the original and risk non-adoption.

## Usage
Please use the [wiki](https://github.com/LucaWolf/grabbit/wiki#how-tos) page for a detailed list of how to get 
the most out of this library. We also have a DeepWiki page for the
 [core concepts](https://deepwiki.com/LucaWolf/grabbit/2-core-components)'s architecture and more.

## Goals
What I'd like this library to provide is:

  - [x] make use of the latest [amqp091-go](https://github.com/rabbitmq/amqp091-go) library;
      this is the up to date version building on [streadway's](https://github.com/streadway/amqp)
      original work
  - [x] be able to share a connection between multiple channels
  - [x] have connection and channels auto-recover (on infrastructure failure) via managers
  - [x] replace the internal logging with an alternative. Current thought is to have some
      buffered channel over which detailed events are submitted (non-blocking)
  - [x] have the topology defined by consumers and publishers. Once when creating and then
      during channels recovery (ephemeral queues/exchanges only)
  - [x] provide an optional callback to the caller space during recoveries. This supplements
     in a synchronous (blocking) mode the logging replacement mechanism.
  - [x] awaiting confirmation of the published events via deferred methods (PublishDeferredConfirm
     or PublishDeferredConfirmWithOptions together with AwaitDeferredConfirmation).      
  - [x] consumers to accept user defined handlers for processing the received messages
  - [x] the consumer handlers to also allow batch processing (with support for 
      partial fulfillment of QoS expectations based on a timeout)
  - [x] Bonus: optionally provide the users with access to the low level `amqp.Channel`. **Unsafe**
      initially. **Note**: safety comes for free if using the slightly higher level `grabbit.Channel` wrappers.

## Non goals

  * not interested in concurrency safety of the high level publisher and consumers. These are relatively 
   cheap therefore use plenty as needed, instead of passing them across coroutines.

## Credits

  * _wagslane_ from whom I got heavily inspired to do the sane parameters, 
      topology maintenance and consumer handlers. Please browse and star [his repository](https://github.com/wagslane/go-rabbitmq).
  * _Emir Ribic_ for his inspiring post that lead me to think about adding a resilience layer 
      for the RabbitMQ client. You may want to [read the full post](https://www.ribice.ba/golang-rabbitmq-client/)
  * _gbeletti_ from whose [project](https://github.com/gbeletti/rabbitmq) I might pinch a few ideas.
      Regardless of drawing inspiration or not, his version made for an interesting reading.
