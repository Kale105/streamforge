# ADR 0001: Use Go as the primary backend language

- Status: Accepted
- Date: 2026-07-18

## Context

The platform requires concurrent source collectors, Kafka producers and consumers, HTTP APIs, PostgreSQL access, observability, small self-hosted containers, and operational command-line tools. The portfolio goal emphasizes backend infrastructure and distributed systems.

## Decision

Use Go as the primary backend language.

- Use `context.Context` for cancellation and deadlines.
- Use `net/http`, with a small router only when useful.
- Use `franz-go` for Kafka.
- Use `pgx`, `sqlc`, and `goose` for PostgreSQL.
- Use structured logging through `log/slog`.
- Use OpenTelemetry and Prometheus for telemetry.
- Compile bundled connectors into the connector worker.
- Defer third-party connector isolation to a versioned external-process protocol.

React and TypeScript remain the management-console technologies.

## Consequences

### Positive

- Lightweight services and containers
- Explicit concurrency, cancellation, and dependency construction
- Strong fit for infrastructure-oriented roles
- Built-in race detection, fuzzing, formatting, and testing
- Simple cross-platform command-line tools

### Negative

- No Kafka Streams equivalent
- Less framework-provided application structure than Spring Boot or ASP.NET Core
- Go native plugins are not a portable connector extension mechanism
- More SQL, wiring, validation, and retry behavior must be designed explicitly

