# Streamforge MVP — Reliability and Failure Testing

- Status: Draft MVP guide
- Scope: deterministic failure behavior for the bounded concurrent runtime and PostgreSQL vertical slice
- Teaching rule: the learner writes the implementation and the fault-injection tests

## Outcome

Turn the MVP’s reliability claims into observed behavior:

```text
source failure / process failure / database failure / API failure
    -> bounded response
    -> visible diagnostic
    -> safe retry or explicit rejection
```

The target is not a production availability promise. The target is an honest, testable statement:

> The MVP accepts source events into PostgreSQL with at-least-once semantics, keeps raw and normalized writes atomic, makes duplicate delivery idempotent at the managed database boundary, bounds work and retries, and reports failures without silently converting them into empty or successful data.

## Prerequisites

Complete [09 — PostgreSQL persistence](09-postgresql-persistence.md) and [10 — read-only HTTP API](10-read-only-http-api.md). Have deterministic source fixtures and a local fake HTTP server. Review the existing [hint ladder](../mvp-learning-guide.md#hint-ladder) before asking for help.

You should know how to:

- inject a dependency or failpoint without global mutable test state;
- cancel a context and set a bounded test timeout;
- distinguish retryable transport errors from deterministic input errors;
- inspect database state after a failed transaction; and
- run tests with the race detector when the local toolchain supports it.

Complete all required laboratories in the [Go concurrency track](go-concurrency-track.md), including the temporary race and leak variants. The reviewed tree must contain only the corrected implementation and regression tests.

## Conceptual explanation

### Failure is part of the contract

A green happy-path test does not show what the system does when the source times out, Wikimedia returns 429, PostgreSQL disappears, or a process dies between a commit and an acknowledgement. The test should name the boundary and the observable result.

Use a small failure taxonomy:

| Category | Examples | MVP response |
|---|---|---|
| Invalid/deterministic | malformed JSON, missing required field, same ID with changed hash | reject or quarantine visibly; do not retry forever |
| Transient source | timeout, connection reset, 429, 5xx | bounded retry/backoff according to source policy; no false success |
| Durable-store outage | PostgreSQL unavailable or transaction fails | stop durable admission, bound memory, return unhealthy/failed result |
| Process boundary | crash after commit before caller sees success | redelivery is possible; duplicate effect must be suppressed |
| Client/request | cancellation, deadline, disconnect | stop work promptly and release resources |
| Contract misuse | invalid limit/cursor, unknown query parameter | deterministic 400; do not query with unvalidated input |
| Concurrency/lifecycle | blocked send, unjoined goroutine, premature close, mutable payload race | cancel/release, join, preserve ownership, and fail the test visibly |
| Overload | full channel, database pool saturation, too many active workers/requests | apply a bounded wait/reject policy; never grow an unbounded queue |

### At-least-once, not exactly-once

The lean MVP has a commit/acknowledgement ambiguity. A process can commit PostgreSQL and then crash before it records or reports success. Retrying can execute the write path again. The raw-event and normalized-record uniqueness boundaries make that retry safe for the tested append-only dataset.

This does not prove exactly-once delivery. It does not cover arbitrary external side effects, all crash points, source behavior, or future transports. Phrase the guarantee as “at-least-once delivery with idempotent effects at the managed PostgreSQL boundary.”

### Fault injection should be deterministic

Prefer explicit seams:

- a fake source that returns scripted responses or blocks until released;
- an injected clock or backoff function for retry tests;
- a repository/test database that can fail at named transaction stages;
- an HTTP handler dependency that returns a chosen database error; and
- a bounded context to release blocked operations.

Avoid random sleeps and “hope the goroutine ran” assertions. If timing matters, use channels or a test-controlled signal. A test may use a small deadline as a last guard, but the assertion should be about a state transition or returned error.

Use `testing/synctest` for timer/goroutine tests that fit one isolated bubble. For network and PostgreSQL tests that do not fit, use explicit start/block/release signals. A leak test should prove the owner can join every task and release resources; an exact process-wide goroutine count is only supporting evidence.

### Health is not freshness

The process can be alive while the source has been failing for hours. Readiness can fail when PostgreSQL is unavailable, while a source freshness indicator reports the age of the last successful observation. Do not turn one boolean into a claim about the whole product.

## C#/Java comparison

| Familiar idea | Go reliability interpretation |
|---|---|
| `CancellationToken` / `CompletableFuture` cancellation | `context.Context` must be observed at network, channel, and database blocking points. |
| Polly/Resilience4j retry policy | Small explicit retry classification is preferable in the MVP; retry only bounded transient cases and respect provider signals such as `Retry-After`. |
| MockWebServer/WireMock | `httptest.Server` with a scripted handler gives an in-process primary test double. |
| Testcontainers integration test | A real disposable PostgreSQL instance proves database semantics that mocks cannot. Keep the test deterministic and clearly named. |
| Transaction rollback callback | Go’s explicit transaction methods plus a rollback fallback; return commit errors rather than logging them. |
| xUnit/JUnit parameterized tests | Table-driven Go tests keep failure cases close to their inputs and expected outcomes. |
| Thread interruption | Go cancellation is cooperative; every blocking operation must include a cancellation path. |

Go does not automatically supervise every goroutine or retry every failed operation. The owner of the boundary must define lifecycle, error propagation, and cancellation.

## Targeted official and primary resources

- [`context` package](https://pkg.go.dev/context) — cancellation, deadlines, and propagation.
- [Go: Accessing relational databases](https://go.dev/doc/database/) — query cancellation and transactions.
- [`pgxpool` package](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool) — context-aware PostgreSQL calls and pool behavior used by the MVP.
- [`net/http/httptest` package](https://pkg.go.dev/net/http/httptest) — deterministic HTTP test servers and recorders.
- [Go race detector](https://go.dev/doc/articles/race_detector) — what race-enabled tests can and cannot establish.
- [`testing/synctest`](https://pkg.go.dev/testing/synctest) — deterministic time and deadlock-aware test bubbles.
- [Go memory model](https://go.dev/ref/mem) — happens-before and data-race-free guarantees.
- [`sync` package](https://pkg.go.dev/sync) — `WaitGroup` joins, mutex rules, and non-copy requirements.
- [Go concurrency track](go-concurrency-track.md) — lifetime, backpressure, fan-in/fan-out, ordering, and overload invariants.
- [Go fuzzing](https://go.dev/doc/security/fuzz/) — useful targets such as cursor decoding and provider payload parsing.
- [PostgreSQL transactions tutorial](https://www.postgresql.org/docs/current/tutorial-transactions.html) — atomic commit/rollback behavior.
- [PostgreSQL `INSERT` and `ON CONFLICT`](https://www.postgresql.org/docs/current/sql-insert.html) — database-level idempotent write behavior.
- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — read enough to explain the chosen default and its limits.

## Design questions

Answer these before writing the failure-injection tests:

1. Which failures are deterministic input failures, and which are transient transport failures?
2. What exact event can be redelivered after a commit/acknowledgement crash boundary?
3. Which database state proves that a transaction rolled back completely?
4. Where must context cancellation be observed so a timeout cannot leave work running?
5. What is the retry budget, and which errors must never be retried automatically?
6. How will a test distinguish stale data from a process that is not alive?
7. Which evidence would justify adding a new transport rather than improving the current boundary?
8. Which goroutine owns every asynchronous failure scenario, and how is it joined?
9. What queue, worker, request, and retry limits prevent amplification during an outage?
10. Which ordering and idempotency guarantees remain valid if two attempts race?

## Failure scenarios to design

For each scenario, write the expected observable result before implementing the test.

| Scenario | Expected result | Must not happen |
|---|---|---|
| Source returns malformed JSON | parser rejects/quarantines one observation | process crash or partial normalized row |
| Source returns 429 with `Retry-After` | bounded delayed retry or visible source failure | tight retry loop |
| Source times out | request ends at context/HTTP deadline | goroutine remains blocked indefinitely |
| Source returns 500 repeatedly | bounded retry budget then failure | infinite retry and unbounded memory |
| PostgreSQL unavailable before transaction | no durable admission; source backpressure/retry is visible | false checkpoint/accepted success |
| Failure injected after raw statement but before normalized statement | transaction rolls back both tables | raw-only or normalized-only state |
| Crash after commit before acknowledgement | later delivery may repeat | duplicate logical record |
| API database query is cancelled | handler returns within bounded time | writes headers after cancellation unpredictably |
| Invalid cursor or limit | stable 400 | malformed values reach SQL |
| Restart with previously stored data | ingestion resumes without duplicate logical state | data reset or silent data loss |
| Sink stops while producer channel is full | producer cancellation releases blocked send and runtime joins | leaked producer goroutine |
| Two producers finish or one fails | group owner closes once after all sends stop | producer closes early or send-after-close panic |
| Retryable outage under load | one bounded retry owner with capped jittered delays | multiplicative retries or goroutine-per-attempt |
| Concurrent duplicate admission | one logical effect with visible ID/hash conflict policy | duplicate or overwritten logical record |
| API concurrency exceeds dependency budget | bounded wait/rejection and visible saturation | unbounded handler work/queue growth |

## Deliberately non-compilable pseudocode

This describes tests and failure boundaries; it is not runnable code.

```text
NOT RUNNABLE — PSEUDOCODE ONLY

for each scripted failure case:
    start a fresh test database or isolated schema
    start a fake source with the scripted response
    choose a named failpoint, if the case requires one
    run one admission attempt with a bounded context

    assert the returned category is the expected one
    assert retry count and delay stay within the configured budget
    assert raw/normalized row counts match the transaction outcome
    assert logs/metrics contain safe diagnostic identity, not secrets or full payloads

for crash-boundary simulation:
    commit the database transaction
    stop the acknowledgement step before it records success
    deliver the same source event again
    assert one logical raw event and one normalized record remain

for an API failure:
    cancel the request context while the repository is blocked
    assert the repository receives cancellation
    assert the handler returns within a bounded test deadline
```

## Implementation constraints

- Keep failures injectable and deterministic; do not use production-only sleeps or random timing to make tests pass.
- Use bounded retries with explicit error classification. Never retry malformed input or authentication/configuration failures blindly.
- Preserve provider/source identity in diagnostics, but redact credentials, authorization headers, raw secrets, and sensitive cursors.
- Ensure context cancellation reaches HTTP requests, database calls, retry waits, channel sends, and shutdown joins.
- Keep raw and normalized inserts in one transaction for each new valid event.
- Test the crash ambiguity as possible redelivery, not as proof of exactly-once.
- Keep the API read-only and bounded while reliability tests are added.
- Do not add Kafka, gRPC plugins, leases, fencing, CEL, Redis, Kubernetes, OpenTofu, API keys, quotas, or custom IaC to solve an MVP test gap.
- Do not implement a general chaos framework. A few named fault seams are enough to demonstrate the current guarantee.
- Keep queues, worker counts, database pool usage, retries, and request work bounded under every fault.
- Give every test-created goroutine explicit cancellation and a join in test cleanup.
- Prefer confinement/single-writer state; if a lock is used, never hold it during HTTP, database, channel, or writer I/O.
- Treat events and nested slices/maps as immutable after publication across a goroutine boundary.
- Keep retry ownership at one layer and test total attempts; do not let outer and inner retries multiply.

## Common failure modes

| Failure in the test design | Why it misleads | Correction |
|---|---|---|
| Test sleeps 100 ms and expects a goroutine to finish | It is flaky or slow and does not prove causality | Synchronize with a channel and use a deadline only as a guard. |
| Retry test uses real exponential waits | The suite becomes slow and hides the retry decision | Inject the clock/backoff or record requested delays. |
| Mock repository says every transaction succeeds | It cannot prove PostgreSQL rollback or uniqueness | Add real PostgreSQL integration coverage. |
| Test asserts exact log text | Small wording changes break useful tests | Assert error category, safe fields, and metrics/health state. |
| Database outage returns empty API data | Consumers cannot distinguish unavailable from empty | Require a failure status and test it. |
| Duplicate test only compares row count | A wrong record could replace the right one | Compare identity, document, source event, and timestamps. |
| “Exactly once” is written after one successful retry test | One test cannot cover distributed acknowledgement ambiguity | Use the at-least-once/idempotent statement. |
| Race detector is treated as a full correctness proof | It finds data races, not semantic loss or bad retries | Combine `-race` with stateful failure tests. |
| Failpoint remains enabled across tests | Later tests inherit hidden state | Scope failpoints to one test and restore/close them. |
| Leak test passes because process exits | The owner never joined the worker | Assert task completion and released body/timer/channel resources. |
| Race is “fixed” with a global lock around I/O | Correctness improves but liveness and throughput collapse | Confine state or reduce the locked critical section. |
| Load test creates a goroutine per request/event | The test itself removes the production bound | Drive through the real bounded admission path and cap clients. |
| Several layers retry the same failure | Attempt count multiplies during outage | Assign one retry owner and assert the total budget. |

## Tests to write

Organize tests by boundary and give every failure a named expected outcome.

### Source and parser

- malformed JSON is rejected without a normalized write;
- missing required source identity is rejected;
- HTTP timeout is cancellable;
- HTTP 429 honors a bounded retry policy and `Retry-After` when present;
- HTTP 500/connection reset retries only within the configured budget;
- deterministic parser errors are not retried indefinitely;
- response-body size limits reject oversized input before unbounded decoding;
- cancellation during an active request returns promptly.

### Persistence and redelivery

- failure between raw and normalized statements rolls back both;
- duplicate delivery creates one logical result;
- changed payload under the same event identity is visible as a conflict;
- commit-before-ack redelivery remains idempotent;
- PostgreSQL outage does not advance a durable-admission/checkpoint decision;
- restart after committed data resumes without resetting tables or duplicating state.

### API and lifecycle

- invalid limit/cursor inputs fail closed;
- database query cancellation stops handler work;
- database outage is not reported as an empty successful page;
- process shutdown stops source work and waits for owned goroutines;
- readiness reflects required PostgreSQL availability, while liveness remains a process check according to the chosen policy.

### Concurrency, load, and overload

- an unbuffered rendezvous blocks until the controlled receiver is released;
- a full bounded event channel blocks or applies the documented overload policy without growing memory unboundedly;
- cancellation releases producers blocked on full sends and HTTP/database calls;
- every runtime-created goroutine is joined on normal stop and first-error stop;
- two finite collectors use group-owned channel closure and preserve per-collector order;
- the intentional shared-state race is detected, then the corrected confined/synchronized version passes repeatedly under `-race`;
- the intentional blocked-send leak prevents join, then the corrected version completes repeatedly;
- active workers/requests never exceed reviewed limits and queued work remains bounded;
- concurrent duplicate delivery converges to one logical database effect;
- load results record queue depth, active work, latency, failures, retries, drops/rejections, environment, and duration;
- no test uses sleeps as its primary synchronization mechanism.

### Fuzz/property targets

- cursor decoding never panics and rejects non-canonical data;
- Wikimedia payload parsing never panics on arbitrary bounded bytes;
- a cursor walk over generated tied timestamps has no repeated record ID;
- duplicate admission attempts converge to the same database state.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
go test -race ./...
go test ./... -run 'Failure|Retry|Redelivery|Cancellation'
go test ./... -run 'Concurrency|Backpressure|Shutdown|Leak' -count=20 -timeout=60s
```

Run the integration suite against the project’s disposable PostgreSQL environment:

```powershell
docker compose config
docker compose up -d postgres
goose -dir migrations postgres "$env:DATABASE_URL" up
go test ./... -run Integration
```

Run a bounded fuzz target for a short local interval only after it exists:

```powershell
go test ./... -run '^$' -fuzz FuzzCursor -fuzztime=30s
```

Record the observed result for each scenario. A passing command without a stated guarantee is not a failure report.

## Acceptance criteria

This milestone is accepted when:

- every listed failure class has a deterministic test or an explicitly documented reason it is deferred;
- malformed, transient, and durable-store failures are classified differently;
- retries are bounded and cancellable;
- raw and normalized rows never partially commit in the tested transaction boundary;
- duplicate/redelivery tests show one logical effect without an exactly-once claim;
- the API distinguishes empty data, invalid input, and dependency failure;
- the race-enabled suite passes where the local toolchain supports it; and
- owned goroutines join under success, cancellation, overload, and first-error paths;
- concurrency/load tests demonstrate queue and worker/request bounds, retry budget, ordering scope, and idempotent duplicate behavior;
- the deliberately introduced race and leak are documented and protected by corrected regression tests; and
- the failure report states the measured behavior, test setup, and unresolved limitations.

## Post-MVP promotion gates

Use evidence, not anticipated scale, to unlock later components:

1. **Kafka gate:** first reproduce database outage, backlog, catch-up, replay, and duplicate tests with the synchronous path. Add Kafka only when it demonstrates replay or failure isolation that PostgreSQL mode cannot provide.
2. **Lease/failover gate:** define a real singleton/partition ownership requirement and test stale-owner rejection before adding leases or fencing.
3. **Plugin gate:** demonstrate a need for process isolation and a versioned cross-language contract before adding gRPC plugins.
4. **API-control gate:** demonstrate multiple untrusted consumers and an authorization threat model before adding API keys or quotas.
5. **Operational-scale gate:** publish resource measurements before adding Redis, Kubernetes, OpenTofu, or a custom IaC layer.

## Reflection questions

- Which failure has the largest gap between what a caller sees and what the database may have done?
- What makes a retry safe in this MVP, and where does that safety stop?
- Which test would fail if the raw uniqueness boundary were removed?
- What evidence would justify changing a retry policy rather than adding another dependency?
- How would you explain why a process can be healthy while data is stale?

## Review submission template

```text
Milestone: 11 — reliability and failure testing
Guarantee I am claiming:
Failure matrix covered:
Fault seams I added:
What I expected:
What actually happened:
Commands I ran:
Integration environment:
Retry/cancellation observations:
Duplicate/redelivery observations:
Race/fuzz results:
Concurrency/load environment and limits:
Blocking, race, and leak observations/fixes:
Goroutine ownership and join evidence:
Queue depth, active work, latency, failure, and drop/reject observations:
Decision I am unsure about:
Unresolved limitation:
What I intentionally did not build:
```
