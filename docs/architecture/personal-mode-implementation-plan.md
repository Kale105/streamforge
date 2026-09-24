# StreamForge personal mode implementation plan

- Status: In progress
- Target: Testable single-host release
- Deployment: One Go binary plus PostgreSQL through Docker Compose
- Scope boundary: Read-only generated data APIs; no Kafka, Kubernetes, billing, arbitrary SQL, or untrusted plugin execution

## Release outcome

A fresh clone can start PostgreSQL, validate one dataset specification, poll a deterministic HTTP source, preserve every accepted raw event, normalize it into an idempotent dataset record, and expose bounded cursor-paginated JSON through a read-only HTTP API. Restarting the process must not duplicate logical records.

## Architectural invariants

1. Collectors only emit canonical events and never import persistence or API packages.
2. Raw events are admitted before normalized records are published.
3. Duplicate event delivery is expected; database constraints make its logical effect idempotent.
4. Configuration is validated before any collector starts.
5. All queues, HTTP bodies, page sizes, retries, and shutdown waits are bounded.
6. The generated API exposes allow-listed behavior rather than arbitrary SQL.
7. PostgreSQL-specific types remain behind project-owned interfaces.
8. Every background goroutine has an owner, cancellation path, and join path.

## Portable task graph

```text
P0 contracts and baseline tests
  |-- P1 dataset specification and normalization
  |-- P2 HTTP polling collector and fake source
  |-- P3 PostgreSQL persistence and migrations
  `-- P4 read-only HTTP API

P1 + P2 + P3 + P4
  `-- P5 application composition and configuration loading
        `-- P6 end-to-end and restart/idempotency tests
              `-- P7 packaging, quickstart, and release evidence
                    `-- P8 independent senior verification
```

P1 through P4 are intentionally package-isolated and can be implemented concurrently. P5 is the first integration barrier.

## Task catalog

### P0 - Stable contracts

Owns:

- Canonical event envelope and validation
- Synchronous collector and sink interfaces
- Bounded concurrent pipeline
- Storage-neutral normalized record and cursor contracts

Acceptance:

- Unit tests and vet pass.
- Interfaces contain no PostgreSQL, Kafka, or HTTP implementation types.

### P1 - Dataset specification and deterministic normalization

Owns `internal/dataset/**` and `internal/normalizer/**`.

Deliverables:

- Versioned dataset model for collector, normalization mapping, and a bounded API contract
- Semantic validation with actionable field paths
- Deterministic top-level JSON field selection/renaming for v0
- Stable record-key extraction
- Unit tests for valid, invalid, missing, and incompatible fields

Acceptance:

- No network, database, goroutine, or deployment dependency.
- Identical input plus revision produces identical normalized output.
- Unsupported transformation features fail validation instead of being ignored.

### P2 - HTTP polling collector

Owns `internal/collector/httpjson/**`.

Deliverables:

- Context-aware polling with injected `http.Client`
- Bounded response bodies, explicit status handling, conditional request support, and stable event identity
- Retry classification without hidden infinite retries
- Deterministic `httptest.Server` coverage for success, malformed JSON, oversized payload, timeout, cancellation, and non-success status

Acceptance:

- No real internet dependency in tests.
- Collector emits immutable valid events and stops promptly on cancellation.
- Provider types never escape the collector package.

### P3 - PostgreSQL persistence

Owns `internal/storage/postgres/**`, `migrations/**`, and the personal PostgreSQL Compose service.

Deliverables:

- Versioned SQL migrations
- Independently committed idempotent raw-event admission
- Per-dataset-revision materialization ledger plus transactional normalized-record upsert
- Stable keyset listing over `(updated_at, id)`
- Pool construction, readiness check, and bounded database operations
- Unit tests for pure helpers plus opt-in integration tests against `STREAMFORGE_TEST_DATABASE_URL`

Acceptance:

- Duplicate event admission does not duplicate raw or normalized logical records.
- Listing has deterministic ordering and no offset pagination.
- Migration and integration instructions run from a fresh clone.

### P4 - Read-only dataset API

Owns `internal/api/**`.

Deliverables:

- `GET /healthz`, `GET /readyz`, and `GET /v1/datasets/{dataset}/records`
- Strict page-size validation and opaque cursor encoding
- Consistent JSON success and problem responses
- Storage-neutral reader interface and `httptest` coverage
- Server timeouts and graceful shutdown hooks

Acceptance:

- Page sizes are bounded.
- Invalid datasets, cursors, and query fields return stable client errors.
- Handlers do not construct SQL.

### P5 - Application composition

Owns `cmd/platform/**` wiring and configuration loading.

Deliverables:

- `platform validate`, `platform server`, and the existing `platform demo`
- YAML loading with strict unknown-field rejection
- Construction of collector, normalizer, PostgreSQL store, pipeline, and API server
- Signal-driven shutdown with no orphaned goroutines

Acceptance:

- Business logic remains outside `cmd`.
- Invalid configuration fails before opening source connections.

### P6 - End-to-end evidence

Owns `internal/testkit/**` and integration workflows.

Scenarios:

- Fake HTTP source to raw event to normalized record to API response
- Duplicate source event and process restart
- PostgreSQL unavailable at startup and during ingestion
- Malformed and oversized source responses
- Cancellation while polling and while database work is pending

Acceptance:

- Tests are deterministic and time-bounded.
- Failure assertions describe acknowledged versus unacknowledged data honestly.

### P7 - Packaging and quickstart

Owns container build, Compose integration, sample dataset, and operator documentation.

Acceptance:

- A fresh clone reaches a queryable sample API using documented commands.
- Images run without root where practical and expose health checks.
- No plaintext production secret is committed.

### P8 - Independent verification

A separate senior reviewer checks architecture boundaries, concurrency, SQL semantics, HTTP safety, tests, documentation, and overengineering. All high-severity findings must be corrected or explicitly accepted before the personal release is called complete.

## Promotion gates

Kafka is considered only after PostgreSQL personal mode demonstrates a measured durability or horizontal-scaling limitation. Kubernetes is considered only after collector workloads are containerized, externally checkpointed, partition-aware, and safe under duplicate execution. Neither technology is required for the personal release.
