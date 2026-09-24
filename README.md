# StreamForge

An open-source, self-hosted dataset publishing platform. StreamForge turns configurable collectors into versioned, normalized datasets and read APIs while durably retaining admitted observations for inspection and future replay support.

The project is under active implementation. The current executable slice provides a validated event envelope, synchronous collector and sink contracts, a synthetic collector, a JSON-lines sink, and a bounded concurrent runtime with cancellation and graceful drain behavior. The [reviewed architecture](docs/architecture/platform-design-v2.md) defines the path from this laboratory transport to PostgreSQL personal mode and Kafka-backed provider mode.

## Product promise

Define a dataset, attach an approved source, normalize its records, and publish a self-hosted API without rebuilding ingestion infrastructure for every source. Source availability, freshness, licensing, and redistribution conditions remain explicit rather than being hidden by the platform.

## Run the current pipeline

```powershell
go run ./cmd/platform demo -count 10 -interval 250ms -buffer 4
```

The command writes newline-delimited CloudEvents-shaped JSON to stdout. Set `-count 0` to run until Ctrl+C.

## Personal-mode quickstart

Personal mode runs StreamForge, PostgreSQL, and a deterministic example source:

```powershell
$env:STREAMFORGE_DB_PASSWORD = "<unique-local-password>"
docker compose up --build
```

Once the services are healthy, query the API declared by the example dataset:

```powershell
Invoke-RestMethod http://localhost:8080/v1/sample-events
Invoke-RestMethod http://localhost:8080/readyz
```

The example collector polls every five seconds. Raw admissions are idempotent by `(source, id)`, normalized records are upserted transactionally, and the API uses bounded keyset pagination.

Personal mode v0 supports literal API paths, field projection, and cursor pagination. Custom filters, custom sorts, arbitrary transformations, schema execution, and declared secondary indexes are rejected until their runtime implementations exist.

Validate a dataset specification without starting infrastructure:

```powershell
go run ./cmd/platform validate -config examples/datasets/http-json/dataset.yaml
```

For a local PostgreSQL installed outside Compose:

```powershell
$env:STREAMFORGE_DATABASE_URL = "postgres://streamforge:${env:STREAMFORGE_DB_PASSWORD}@localhost:5432/streamforge?sslmode=disable"
go run ./cmd/platform server -config examples/datasets/http-json/dataset.yaml
```

Set `STREAMFORGE_DB_PASSWORD` to a unique local value before using Compose or the commands above. Do not commit it or reuse it in production. Existing PostgreSQL volumes retain their original database password; changing the variable alone does not rotate an existing role.

## Verify

```powershell
go test ./...
go vet ./...
```

PostgreSQL integration tests are destructive to the configured test database: they migrate and truncate StreamForge tables. Run them only against an isolated disposable database:

```powershell
$env:STREAMFORGE_TEST_DATABASE_URL = "postgres://streamforge:${env:STREAMFORGE_DB_PASSWORD}@localhost:5432/streamforge?sslmode=disable"
go test -v ./internal/storage/postgres
```

Without `STREAMFORGE_TEST_DATABASE_URL`, those database integration cases are intentionally skipped; the remaining unit and in-memory integration suite still runs.

The race-enabled suite is also required in CI or any local Go environment with CGO enabled:

```powershell
go test -race ./...
```

## Delivery sequence

1. In-memory concurrent pipeline and lifecycle contracts.
2. Permitted real collectors and deterministic fake-source tests.
3. PostgreSQL durable journal, normalized storage, and read API.
4. Dataset packages, validation, replay, and operational telemetry.
5. Kafka-backed provider mode after benchmarks demonstrate its value.
