GO ?= go
TAGS ?= test_env
PKGS ?= ./...
LAVINMQ_HA_DIR ?= examples/lavinmq-ha

.DEFAULT_GOAL := help
.PHONY: help build test test-race cover vet fmt tidy examples lavinmq-up lavinmq-down lavinmq-reset run check clean

help: ## List available targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Compile the library and every example
	$(GO) build $(PKGS)

test: ## Run the test suite with trace instrumentation
	$(GO) test -tags $(TAGS) $(PKGS)

test-race: ## Run the test suite with the race detector
	$(GO) test -race -tags $(TAGS) $(PKGS)

cover: ## Run tests and print total statement coverage
	$(GO) test -tags $(TAGS) -covermode=atomic -coverprofile=coverage.out $(PKGS)
	@$(GO) tool cover -func=coverage.out | tail -1

vet: ## Run go vet with trace instrumentation enabled
	$(GO) vet -tags $(TAGS) $(PKGS)

fmt: ## Format all Go packages
	$(GO) fmt $(PKGS)

tidy: ## Synchronise go.mod/go.sum (examples share this module)
	$(GO) mod tidy

examples: ## Build every example into bin/
	@mkdir -p bin
	$(GO) build -o bin/ ./examples/...

lavinmq-up: ## Start the local LavinMQ HA cluster (Podman, AMQP on :5672)
	$(LAVINMQ_HA_DIR)/cluster_up.sh

lavinmq-down: ## Stop the local LavinMQ HA cluster (keeps broker data)
	$(LAVINMQ_HA_DIR)/cluster_down.sh

lavinmq-reset: ## Stop the cluster and delete its persisted data
	$(LAVINMQ_HA_DIR)/cluster_down.sh
	rm -rf $(LAVINMQ_HA_DIR)/lavin-data $(LAVINMQ_HA_DIR)/etcd-data

run: ## Run one example: make run EXAMPLE=publisher
	@test -n "$(EXAMPLE)" || { echo "usage: make run EXAMPLE=<name>"; exit 1; }
	$(GO) run ./examples/$(EXAMPLE)

check: fmt vet test ## Format, vet, and test

clean: ## Remove build and coverage artifacts
	rm -rf bin coverage.out
