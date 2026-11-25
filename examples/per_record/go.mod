module per_record

go 1.25.7

require (
	github.com/LucaWolf/grabbit v0.0.0-20221102051845-2eb228c3ab34
	github.com/rabbitmq/amqp091-go v1.10.0
)

require github.com/LucaWolf/go-notifying-latch v1.1.0 // indirect

replace github.com/LucaWolf/grabbit => ../../../grabbit
