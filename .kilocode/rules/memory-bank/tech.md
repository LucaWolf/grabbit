# Technologies Used

*   **Primary Language:** Go
*   **RabbitMQ Client Library:** `github.com/rabbitmq/amqp091-go`
*   **Concurrency:** Go's native goroutines and `sync` package primitives (Mutex, RWMutex, WaitGroup).
*   **Testing:** Go's built-in testing framework.
*   **Dependency Management:** Go Modules.

## Development Setup

*   **Go Version:** Requires Go 1.21 or higher (implied by `go.mod` and modern Go features).
*   **Environment:** Standard Go development environment.
*   **RabbitMQ Instance:** A running RabbitMQ server is required for integration tests and examples. Docker is likely used for local development/testing environments (as suggested by `container_setup_test.go`).

## Technical Constraints

*   **`amqp091-go` dependency:** The library is built directly on `amqp091-go`, inheriting its API and any underlying limitations.
*   **Concurrency Model:** Relies heavily on Go's concurrency model for safe access to shared resources.
*   **Error Handling:** Go's idiomatic error handling.

## Dependencies

*   `github.com/rabbitmq/amqp091-go`
*   Standard Go library packages (e.g., `context`, `fmt`, `log`, `net`, `sync`, `time`).

## Tool Usage Patterns

*   **`go test`:** For running unit and integration tests.
*   **`go run`:** For executing examples.
*   **`go mod tidy`:** For managing Go module dependencies.