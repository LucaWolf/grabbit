# Product Overview

This project, "grabbit," is a Go library designed to simplify interaction with RabbitMQ using the `amqp091-go` client library. It aims to provide a more robust and developer-friendly experience by offering managed connections and channels with auto-recovery capabilities.

## Problems Solved

- **Connection and Channel Management:** Abstracting away the complexities of managing RabbitMQ connections and channels, including their lifecycle and recovery from infrastructure failures.
- **Auto-Recovery:** Automatically re-establishing connections and channels in the event of network issues or RabbitMQ server restarts, ensuring application resilience.
- **Concurrency Safety:** Providing a concurrency-safe wrapper around the low-level `amqp091-go` library.
- **Topology Definition:** Allowing consumers and publishers to define their own topology (ephemeral queues/exchanges) which is re-established during recovery.
- **Improved Logging/Event Handling:** Replacing internal logging with a non-blocking buffered channel for detailed event submission and providing optional synchronous callbacks for recovery events.
- **Reliable Publishing:** Supporting deferred confirmation for published events to ensure delivery.
- **Flexible Consumption:** Enabling user-defined handlers for processing messages, including batch processing with QoS expectations.

## How it Should Work

The library should provide higher-level abstractions for common RabbitMQ operations:

- **Managed Connections:** Users should be able to obtain a managed connection that handles underlying `amqp.Connection` lifecycle and recovery.
- **Managed Channels:** Channels should be derived from managed connections, offering auto-recovery and concurrency-safe access to `amqp.Channel` functionalities.
- **Consumers:** A consumer abstraction should allow users to register handlers for message processing, with options for batching and QoS.
- **Publishers:** A publisher abstraction should facilitate message publishing, including support for deferred confirmations.
- **Eventing:** A clear mechanism for users to subscribe to internal events (e.g., connection/channel recovery, errors) for monitoring and custom logic.

## User Experience Goals

- **Simplicity:** Developers should find it easy to integrate and use the library with minimal boilerplate code.
- **Reliability:** Applications built with grabbit should be resilient to transient RabbitMQ failures.
- **Observability:** Users should have clear insights into the library's internal state and events.
- **Flexibility:** The library should be configurable to suit various use cases and deployment scenarios.