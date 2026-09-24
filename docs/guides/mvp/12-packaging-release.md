# Streamforge MVP — Packaging and Release

- Status: Draft MVP guide
- Scope: reproducible local packaging, startup, upgrade, and release evidence
- Teaching rule: the learner owns the build and release procedure

## Outcome

Make a fresh checkout usable by another person:

```text
fresh checkout
    -> documented configuration
    -> PostgreSQL Compose service
    -> reviewed migrations
    -> Go application
    -> smoke-tested read API
```

The MVP release is a personal/local Docker Compose profile. It is not a hosted SaaS release, Kubernetes distribution, API gateway, or infrastructure-as-code product.

## Prerequisites

Complete [09 — PostgreSQL persistence](09-postgresql-persistence.md), [10 — read-only HTTP API](10-read-only-http-api.md), and [11 — reliability and failure testing](11-reliability-failure-testing.md). Read the main [MVP learning guide](../mvp-learning-guide.md) scope section and use its [hint ladder](../mvp-learning-guide.md#hint-ladder) when a Docker, Go, or migration detail is unfamiliar.

You should have:

- a working Go toolchain matching the repository’s `go.mod`;
- Docker Desktop or a compatible Docker Engine/Compose installation;
- a migration path that can report status and apply migrations;
- deterministic offline tests; and
- a documented sample configuration with no committed secrets.

The [Go concurrency track](go-concurrency-track.md) must be complete. The release review uses its production concurrency budget rather than carrying every teaching experiment into the binary.

## Conceptual explanation

### Artifact versus environment

The Go binary and container image are artifacts. PostgreSQL data, source-policy configuration, network addresses, and retention choices belong to the operator environment. A release should make the boundary explicit:

- build artifacts contain application code, migration files, and version metadata;
- environment variables or mounted secret files provide deployment-specific configuration;
- Compose defines the small supported local environment; and
- the operator owns backups, data retention, and source permissions.

Do not bake credentials or local paths into an image. Do not claim that a container image includes rights to collect or redistribute third-party data.

### Compose is the MVP deployment boundary

Use one small Compose file/profile for PostgreSQL and the application. A health check should reflect the service’s readiness, not merely that a process exists. The API should not advertise ready before its required database schema is available.

Keep the deployment understandable enough that a learner can explain:

1. which service owns persistent data;
2. which command applies migrations;
3. how the application receives its database address;
4. how liveness differs from readiness; and
5. how to stop the stack without deleting the database volume.

Do not hide migration failures inside a repeated application restart loop. Prefer an explicit, visible migration step or a one-shot migration command that the operator can inspect.

### Versioning and migration safety

Every release should identify:

- source revision or release tag;
- Go/toolchain version used to build;
- dependency state from the module files;
- image/artifact digest or checksum; and
- database migration version expected by the application.

An upgrade applies forward migrations in order. Never rewrite an already-applied migration to “fix” a released schema. If a change requires a compatibility window, document expand/migrate/contract behavior even if the MVP has only a small schema.

### Release evidence over release adjectives

“Production-ready” is not an acceptance criterion. Record the exact commands, environment, tests, image identifier, migration status, smoke-test response, and known limitations. Avoid throughput, availability, or data-coverage claims without measurements.

## C#/Java comparison

| Familiar idea | Go/Compose MVP interpretation |
|---|---|
| `dotnet publish` or a JAR distribution | `go build` produces a platform-specific executable; a container packages it with runtime configuration and migrations. |
| Spring Boot schema migration at startup | An explicit migration command is easier to inspect in this MVP; the API process should reject incompatible schema rather than silently mutate it. |
| Maven/Gradle lock and dependency graph | `go.mod`/`go.sum` plus the selected Go toolchain provide build inputs; record them in release evidence. |
| `application.yml` profiles | Compose environment values and a documented sample configuration; do not commit secrets. |
| Docker Compose service health | A health check and application readiness endpoint are related but not identical; test both. |
| CI/CD release pipeline | A small repeatable sequence: format, test, vet, build, validate Compose, apply migrations to a disposable database, smoke test, checksum artifacts. |
| Native executable versus JVM/.NET runtime | Go can reduce runtime dependencies, but cross-platform builds still need explicit target testing and CGO/toolchain decisions. |

Go’s small binary is not a substitute for operational documentation. The database schema, migration policy, environment contract, and backup story remain part of the product.

## Targeted official and primary resources

- [Organizing a Go module](https://go.dev/doc/modules/layout) — repository conventions for commands and supporting packages.
- [Go command documentation](https://go.dev/cmd/go/) — build, test, module, and environment commands.
- [Go install documentation](https://go.dev/doc/install) — toolchain installation and compatibility assumptions.
- [Go build constraints](https://pkg.go.dev/cmd/go#hdr-Build_constraints) — only if the release needs platform-specific files.
- [Docker Compose application model](https://docs.docker.com/compose/intro/compose-application-model/) — services and project structure.
- [Docker Compose service health checks](https://docs.docker.com/reference/compose-file/services/#healthcheck) — health-check configuration.
- [Docker Compose CLI reference](https://docs.docker.com/reference/cli/docker/compose/) — validate, up, ps, logs, and config commands.
- [PostgreSQL backup and restore](https://www.postgresql.org/docs/current/backup.html) — read the operator responsibilities before calling the local stack durable.
- [goose primary repository](https://github.com/pressly/goose) — migration status, SQL migration format, and forward application.
- [Go security policy and vulnerability management](https://go.dev/security/) — use as a pointer for dependency/security review; do not claim a scan you did not run.

## Design questions

Answer these before writing build, Compose, or release automation:

1. What is the smallest supported local environment a fresh user needs?
2. Which configuration values belong in the environment rather than the artifact?
3. What should happen when the application binary and migration version are incompatible?
4. Why is an explicit migration step preferable to an invisible schema mutation at startup for this MVP?
5. Which health check proves PostgreSQL readiness, and which only proves that the process is alive?
6. What evidence is required before calling an artifact reproducible or a release supported on another platform?
7. Which post-MVP feature would materially change the threat model or operational boundary?

## Release contract to design

### Configuration

Document each variable, whether it is required, its default, and whether it is secret. Typical MVP values include:

| Setting | Required? | Purpose |
|---|---:|---|
| `DATABASE_URL` | yes | PostgreSQL connection target for the application/migration command |
| `HTTP_ADDR` | no | local bind address with a safe default |
| `SOURCE_MODE` | no | selects the documented fixture or Wikimedia mode without changing code |
| `WIKIMEDIA_STREAM_URL` | no | the reviewed EventStreams endpoint, injectable for local fake-source tests |
| `WIKIMEDIA_USER_AGENT` | yes for real-source mode | descriptive project/version/contact value required by source policy; not a secret |
| release/schema version | no | diagnostic compatibility information |

There are no source credentials, consumer API keys, quotas, billing controls, or public authentication features in this MVP. The descriptive Wikimedia User-Agent is configuration, but it is not authentication and must not be presented as a secret.

### Startup sequence

The documented sequence should make failure visible:

```text
validate configuration
start or verify PostgreSQL
apply/report reviewed migrations explicitly
start application
wait for liveness and readiness
run a bounded smoke request
```

The application must fail clearly when the database schema is missing or incompatible. It must not silently create arbitrary tables or continue serving partial data.

### Artifact identity

Choose one release version convention and use it consistently in the binary’s diagnostic endpoint/log and image tag. Record a checksum for downloadable artifacts or an image digest for containers. A Git commit alone is useful but does not replace a migration version or build-toolchain record.

## Deliberately non-compilable pseudocode

This is a release checklist, not a shell script or application implementation.

```text
NOT RUNNABLE — PSEUDOCODE ONLY

assert checkout is clean or record the intentional local changes
verify Go and Docker/Compose versions are supported
run formatting, unit tests, integration tests, vet, and race tests when available
build the application for the declared target
validate the Compose model
start an isolated PostgreSQL service
apply migrations and record the resulting schema version
start the application with sample non-secret configuration
wait for liveness and readiness
request one bounded API page and validate its JSON contract
stop and restart the application
request the same API again and verify data survived the restart
record artifact checksum/image digest, migration version, commands, and limitations
```

## Implementation constraints

- Support Docker Compose as the only MVP deployment orchestrator.
- Keep the local stack small: Go application plus PostgreSQL; use a fixture source for offline/demo operation.
- Make migrations explicit, ordered, and reviewable. Do not edit applied migration files.
- Do not run destructive volume deletion as part of the normal startup or release path.
- Keep secrets out of source control, images, logs, fixtures, and release notes.
- Provide a sample configuration with placeholders, not real credentials.
- Use health checks and readiness behavior that can be exercised from a fresh environment.
- Build and test from a clean checkout or clearly record the working-tree state.
- Keep the release concurrency budget explicit: one selected source collector task, one sink loop, one bounded event channel, one reconnect owner, and no goroutine-per-event or unbounded task queue.
- Expose bounded, low-cardinality evidence for queue depth/capacity, active work/limit, processing latency, failures, retries, and deliberate drops/rejections without adding a separate metrics platform solely for the MVP.
- Ensure shutdown stops intake, drains or hard-aborts by documented policy, and joins every application-owned goroutine before process exit.
- Pin or record tool and dependency inputs well enough to reproduce the build; do not claim bit-for-bit reproducibility until it is measured.
- Do not add Kafka, Redis, Kubernetes, Helm, OpenTofu, a custom IaC engine, an API gateway, API keys, quotas, or a frontend to make the MVP “release-like.”
- Do not provide a complete Dockerfile, Compose file, migration, or application solution in this learning guide. The learner writes those files and requests focused syntax hints only when blocked.

## Common failure modes

| Failure | Why it matters | Prevention/evidence |
|---|---|---|
| Image starts before schema is ready | API may serve misleading empty/error data | Explicit migration step plus readiness test. |
| Migration runs invisibly on every restart | Operators cannot distinguish upgrade from boot and may miss failure | Make migration status and application startup policy explicit. |
| Applied migration is edited | Existing installations diverge from fresh installs | Add a new migration and test upgrade from the prior version. |
| Compose file parses but service is unusable | Syntax validation is not a smoke test | Start the stack, wait for health, query the API. |
| Health check tests only a process port | Database outage can remain hidden | Separate liveness and readiness; stop PostgreSQL in a test. |
| Secret committed in `.env` or fixture | Credential exposure can persist in Git history and images | Use an ignored local file or environment/secret reference and scan the tree. |
| `latest` is the only image identifier | Rollback and evidence become ambiguous | Tag a release and record digest/checksum. |
| Cross-compiled binary is not exercised | Target-specific path or CGO issue appears at release time | Smoke-test each declared target or narrow the support matrix. |
| Teaching worker pool ships accidentally | Demonstration concurrency becomes unreviewed production behavior | Compare the binary/configuration against the documented concurrency budget. |
| Shutdown reports success with workers running | Process lifecycle evidence is incomplete | Repeat signal/failure shutdown tests and assert every owned task joins. |
| Retry layers multiply during source outage | Release amplifies provider failure | Record one retry owner, total budget, and attempt metric. |
| Release claims scalability without data | Users infer unsupported capacity/SLOs | Publish measured results or state that they are unknown. |
| `docker compose down -v` used routinely | Local database data is deleted | Use ordinary stop/down; isolate disposable test projects. |

## Tests to write

### Build and packaging

- all Go packages format, test, and vet cleanly;
- a clean checkout builds the declared command(s);
- version/build metadata is present and does not contain secrets;
- the Compose model validates;
- a disposable PostgreSQL instance applies migrations from zero;
- an upgrade path applies new migrations to a database containing prior MVP data;
- the application refuses or clearly reports an incompatible/missing schema;
- health checks distinguish process liveness from database readiness;
- the container or executable starts with sample configuration and no external sports network dependency.

### Smoke and restart

- a bounded API request returns the documented canonical envelope;
- invalid cursor/limit behavior remains stable in the packaged environment;
- application restart preserves normalized data;
- PostgreSQL restart produces visible readiness recovery rather than a false empty page;
- logs contain version, migration, and request diagnostics without credentials or raw secrets.

### Release hygiene

- repository search finds no real credential patterns or private local paths;
- artifact checksum/digest is recorded;
- release notes state supported platforms, startup commands, migration policy, known data-source limits, and non-guarantees;
- test results and environment versions are attached to the review submission.
- concurrency/load evidence records channel capacity, active-worker/request limits, retry budget, shutdown deadline, workload, duration, and environment;
- race-enabled tests pass on a supported target, and the intentional race/leak exercises are represented only by fixed regression tests;
- a release checklist review finds no fire-and-forget goroutine, goroutine-per-event path, unowned channel close, unbounded queue, shared mutable map without synchronization, copied lock/WaitGroup, or lock held during I/O.

## Commands to run

The exact image/build paths depend on the learner’s files. Keep the sequence recognizable and inspect targets before any cleanup.

```powershell
go version
docker version
docker compose version
go fmt ./...
go test ./...
go vet ./...
go test -race ./...
go build ./...
docker compose config
docker compose up -d postgres
goose -dir migrations postgres "$env:DATABASE_URL" status
goose -dir migrations postgres "$env:DATABASE_URL" up
docker compose up -d app
docker compose ps
```

Smoke-test and inspect logs:

```powershell
Invoke-WebRequest http://localhost:8080/health/live
Invoke-WebRequest http://localhost:8080/health/ready
Invoke-WebRequest 'http://localhost:8080/api/v1/records?limit=1'
docker compose logs --no-color app
```

For disposable validation, use an explicitly named test Compose project/database. Do not append volume deletion to a command unless the target is a disposable test resource and has been verified.

## Acceptance criteria

This milestone is accepted when:

- a fresh checkout has a documented, repeatable local startup path;
- Compose configuration validates and starts the supported PostgreSQL/application profile;
- migration status and application startup behavior are explicit;
- the database survives an ordinary application restart;
- health/readiness and API smoke tests pass;
- tests run without depending on the public source or committed credentials;
- the release records toolchain, artifact/image identity, migration version, commands, and limitations; and
- the release matches the reviewed concurrency budget, joins all owned goroutines, and exposes bounded overload behavior;
- race, repeated shutdown, leak, and bounded-load evidence is attached; and
- the documentation makes no unsupported API-key, quota, scaling, Kubernetes, or exactly-once claim.

## Explicit post-MVP promotion gates

The MVP is complete without these features. Promote only after the corresponding evidence exists:

1. **Kafka/provider-mode gate:** demonstrate replay, outage isolation, and catch-up with the PostgreSQL slice before adding broker services or a provider Compose profile.
2. **Authentication/usage gate:** demonstrate multiple untrusted consumers and an authorization/abuse requirement before API keys, quotas, or usage accounting are added.
3. **Scale/deployment gate:** publish measured CPU, memory, database pool, query latency, and recovery results before Redis, Kubernetes, Helm, OpenTofu, or a custom IaC engine.
4. **Connector-isolation gate:** demonstrate a crash/hang boundary and a versioned need for external connectors before gRPC plugins or other process protocols.
5. **Release-hardening gate:** add signed artifacts, automated dependency/container scanning, backup-restore drills, and formal support matrices only when the owner defines a release audience and threat model.
6. **Concurrency-scale gate:** add persistence workers, keyed partitions, multiple production collectors, or an API concurrency limiter only after representative load identifies the bottleneck and the proposal defines queue/worker bounds, ordering, idempotency, cancellation, shutdown, and observability.

## Reflection questions

- What exactly makes a fresh clone “usable” rather than merely buildable?
- Which release fact is missing if you record only a Git tag?
- Why should migrations be visible to the operator?
- What does a readiness check prove, and what does it not prove about data freshness?
- Which post-MVP feature would add the most operational complexity, and what evidence would justify it?

## Review submission template

```text
Milestone: 12 — packaging and release
Supported local profile:
Startup and migration sequence:
Configuration variables and secret handling:
What I implemented:
What I expected:
What actually happened:
Commands and tool versions:
Build/image identity and checksum/digest:
Fresh-start smoke result:
Restart/upgrade result:
Known limitations and non-guarantees:
Decision I am unsure about:
What I intentionally did not build:
Production goroutines and owners:
Channel/worker/request/retry limits:
Shutdown join evidence:
Race/leak/load evidence:
Concurrency experiments excluded from production:
```
