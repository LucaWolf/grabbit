# `internal/` — Intermediate Blueprint

**Module:** `github.com/LucaWolf/grabbit` (Go 1.25.7)

## Overview

`internal/` groups packages that support the root `grabbit` library but are **not part
of its public API**. Under Go's `internal` rule, only code rooted at
`github.com/LucaWolf/grabbit` may import anything in this tree; external consumers of
the library cannot see it, and its surface may change freely between releases.

This directory contains **no Go source files of its own** — it exists purely to hold
internal packages.

## Child Packages

| Package | Purpose | Contract |
|---|---|---|
| `traceutils` | Build-tag-gated AMQP call tracing: no-op in production builds; under `-tags test_env` it emits `ParamsTrace` events to a context channel, validated by `ConsumeTracesContext`. Used by `channel.go` / `channel_safewrappers.go` and asserted against by `consumer_test.go` / `publisher_options_test.go`. | [traceutils/CONTEXT.md](./traceutils/CONTEXT.md) |

`traceutils` is the only child — there are no other subdirectories under `internal/`.

## How the Group Is Used

- **Production (default build):** the root package calls a `traceutils` function beside
  every wrapped AMQP operation; the calls compile to empty no-ops, so the shipped
  library carries no tracing overhead.
- **Tests (`-tags test_env`):** `ConsumeTracesContext(validators)` installs channels plus
  a consumer goroutine into a context, letting tests assert the exact parameters
  grabbit passes to AMQP methods without a live broker.
- Nothing under `internal/` is exercised directly by library consumers; it exists to
  serve the root package and its test suite.

## Verification

```bash
# This subtree (its single package):
go test -tags test_env ./internal/traceutils/...

# Whole-module (local, repository root; CI runs the same flags WITHOUT a
# package pattern, so it covers only the root package):
go test -tags test_env -coverprofile coverage.out -v ./...
```
