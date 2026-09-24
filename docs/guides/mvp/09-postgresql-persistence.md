# Streamforge MVP — PostgreSQL Persistence

- Status: Draft MVP guide
- Scope: raw observations and normalized records in PostgreSQL
- Teaching rule: the learner writes the implementation

## Outcome

Build the first durable boundary in the MVP:

```text
source event -> normalization -> one PostgreSQL transaction -> raw event + normalized record
```

The application design is deliberately small. There are two application tables:

1. `raw_events` retains the source observation and its identity.
2. `normalized_records` stores one stable, provider-neutral record served by the API.

The migration tool may have its own bookkeeping table. That bookkeeping table is not a third domain table.

The guarantee is **at-least-once ingestion with idempotent effects**. A duplicate delivery may execute the transaction again, but database constraints and the write policy must prevent a duplicate logical result. This guide does not authorize an exactly-once claim.

## Prerequisites

Before beginning, complete stages 1-8 in the [MVP curriculum](README.md). This stage introduces normalization, Compose, migrations, and PostgreSQL rather than assuming they already exist. You should be able to explain:

- why source-event identity is different from normalized-record identity;
- why raw payloads are retained before interpretation;
- how `context.Context` cancellation reaches a blocking operation;
- how the Wikimedia collector turns one complete SSE message into one raw event; and
- how to ask for help using the existing [hint ladder](../mvp-learning-guide.md#hint-ladder): concept, direction, pseudocode, focused syntax on an unrelated example, then rescue explanation.

Complete the idempotency, ordering, worker-limit, and mutable-payload sections of the [Go concurrency track](go-concurrency-track.md). The production baseline uses one sink loop and one transaction at a time; database-pool capacity does not itself justify concurrent event handlers.

Do not skip the hint ladder by asking for a complete repository implementation. The learner owns the SQL, Go code, migrations, and tests.

## Conceptual explanation

### Raw observation versus normalized record

`raw_events` answers: “What did the source send, and when did we accept it?” It is an evidence record. It should preserve enough information to diagnose a parser problem without calling the provider again.

`normalized_records` answers: “What stable public record did this accepted event produce?” For the lean MVP, it is an append-only normalized dataset: one accepted source event produces at most one normalized record. It is not yet a mutable current-state projection.

Do not make the normalized table a copy of the Wikimedia payload. A source-specific normalizer should validate the fields it needs and map them to a small provider-neutral record shape before the transaction begins. Keep stable identity and timestamps in typed columns; use a JSONB document only for the bounded normalized body the API is allowed to return.

This one-to-one shape is a deliberate sequencing choice. Mutable current-state projections require a logical entity key plus a trustworthy ordering or version rule so an older redelivery cannot overwrite newer state. Wikimedia recent-change events do not require that machinery for the first MVP dataset, so defer it behind a promotion gate.

### Lean two-table shape

Use reviewed SQL migrations to create the following shape. Treat names as a design starting point, not a command to copy blindly.

| Table | Required responsibility | Candidate columns | Uniqueness boundary |
|---|---|---|---|
| `raw_events` | Retain one source observation | `source`, `event_id`, `event_type`, `observed_at`, `payload`, `payload_hash`, `received_at` | primary key `(source, event_id)` |
| `normalized_records` | Serve one immutable normalized change record | `record_id`, `source`, `source_event_id`, `kind`, `occurred_at`, immutable `published_at`, `document` | primary key `record_id`; unique `(source, source_event_id)` |

For this MVP, one accepted source event produces at most one normalized record. If a future transform emits several records, the uniqueness boundary must become an explicit `(source, event_id, output_ordinal)` or equivalent. Do not quietly add that generalization now.

The API pagination timestamp is the immutable `normalized_records.published_at`, paired with the immutable `record_id`. `occurred_at` is source time and is not the pagination key.

### Compose is the local database boundary

This stage adds one PostgreSQL service to Compose. Keep it small:

- choose and record a supported PostgreSQL major version rather than relying on an unqualified floating tag;
- provide configuration through documented environment values without committing real secrets;
- use a named volume for ordinary local persistence;
- add a PostgreSQL readiness health check; and
- keep migration execution explicit and inspectable.

Compose starting a container does not prove PostgreSQL is ready to accept connections. Health and dependency conditions can coordinate startup, but the application must still handle a database connection failure as a normal runtime error. Do not add the application container until the packaging stage unless it materially helps the database exercise.

### Constraints are part of the design

Application checks improve error messages, but only PostgreSQL constraints protect the invariant under concurrent writers. At minimum:

- raw event identity is unique on `(source, event_id)`;
- normalized identity is unique through both `record_id` and `(source, source_event_id)`;
- required timestamps and documents are not null;
- timestamps use `timestamptz` and are treated as UTC instants;
- the API ordering columns have a supporting index in the same order as the query.

Use `ON CONFLICT` only after deciding which identity the conflict represents. `DO NOTHING` for a duplicate raw event means “this source event was already durably handled,” not “silently accept a different payload with the same ID.” A changed payload with the same source identity should be detected through the hash and routed to an explicit diagnostic path.

### One transaction, one admission decision

For a new valid event, the raw insert and normalized insert belong to one transaction. If either fails, neither is committed. This prevents a raw event from claiming durable admission while its normalized effect is absent, and prevents a normalized record from appearing without retained source evidence.

The transaction should be short:

1. validate and normalize the bounded event before opening a transaction;
2. begin with the worker context;
3. insert the raw event using the raw uniqueness boundary;
4. if it is new, insert its one normalized record;
5. if it is a duplicate, compare the stored hash and perform no second logical effect;
6. commit; or
7. roll back on every error path.

Do not perform HTTP calls, sleeps, parsing of unbounded input, or logging of secrets inside the transaction.

## C#/Java comparison

| Familiar idea | Go/PostgreSQL MVP interpretation |
|---|---|
| `SqlConnection`/`JdbcConnection` | `pgxpool.Pool`; it is a concurrency-safe pool, not one permanently open application-wide connection. |
| `TransactionScope` or `Connection.begin()` | An explicit `pgx.Tx` whose commit and rollback are visible in the function. |
| `try/finally` cleanup | `defer` is useful for a rollback fallback, but commit must still be explicit and commit errors must be returned. |
| JPA/Hibernate unit of work | No ORM is required here. Reviewed SQL is the unit of persistence and makes constraints and query plans visible. |
| Java `CancellationException` / .NET `CancellationToken` | `context.Context` is passed to database calls and HTTP handlers; cancellation is not a global mutable flag. |
| Bean validation plus database constraints | Use both when useful, but PostgreSQL remains the final authority for uniqueness and relational invariants. |
| JSON DTO plus mapper | Wikimedia payload DTO → normalized record → public API DTO. Do not reuse the source DTO as a public contract. |

One important Go difference: a database handle is a managed pool, while a transaction pins a connection for its lifetime. Keep the transaction narrow so a slow caller does not consume a connection unnecessarily.

## Targeted official and primary resources

Read only the sections needed for this milestone and record the evidence in the review submission:

- [Go: Accessing relational databases](https://go.dev/doc/database/) — read the transaction and cancellation concepts, then compare them with the PostgreSQL-specific driver used here.
- [`pgxpool` package](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool) — `Pool`, `Begin`, `Ping`, and concurrency-safe pool behavior.
- [`pgx` transactions](https://pkg.go.dev/github.com/jackc/pgx/v5#Tx) — `Tx`, `Commit`, `Rollback`, and commit-error handling.
- [PostgreSQL transactions tutorial](https://www.postgresql.org/docs/current/tutorial-transactions.html) — commit and rollback as one atomic unit.
- [PostgreSQL constraints](https://www.postgresql.org/docs/current/ddl-constraints.html) — primary keys, unique constraints, and checks.
- [PostgreSQL `INSERT` and `ON CONFLICT`](https://www.postgresql.org/docs/current/sql-insert.html) — conflict targets and `DO NOTHING` behavior.
- [PostgreSQL indexes](https://www.postgresql.org/docs/current/indexes.html) — why a supporting index is a query design decision, not a decorative optimization.
- [PostgreSQL `EXPLAIN`](https://www.postgresql.org/docs/current/using-explain.html) — inspect the actual plan for the API ordering query.
- [goose primary repository](https://github.com/pressly/goose) — SQL migration annotations, status, and migration commands. Treat the repository version used by the project as an explicit dependency choice.
- [Docker Compose application model](https://docs.docker.com/compose/intro/compose-application-model/) — services, networks, volumes, and configuration.
- [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/) — why `service_healthy` depends on an actual health check.

## Design questions

Answer these in the engineering journal before writing persistence code:

1. What distinct fact is protected by the raw `(source, event_id)` key?
2. What distinct fact is protected by the normalized `record_id` key?
3. Which Wikimedia fields belong in the normalized public record, and which remain only in raw evidence?
4. Why must raw and normalized insertion commit together for a new event?
5. What should happen if the same source ID arrives with a different payload hash?
6. Why is immutable `published_at` safer than source-controlled `occurred_at` for the API cursor?
7. Which invariant belongs in a database constraint instead of only in Go?
8. What does a successful commit prove, and what does it not prove about future delivery?
9. Why is a mutable current-state projection deferred for this dataset?

## Deliberately non-compilable pseudocode

The following is control-flow notation, not Go or SQL. Do not paste it into a source file.

```text
NOT RUNNABLE — PSEUDOCODE ONLY

validate and normalize one bounded source event

begin transaction using the caller's context
defer a rollback fallback

insert raw observation keyed by (source, event_id)
if the insert reports an existing identical event:
    commit the no-op decision
    return duplicate-delivery result
if the insert reports a conflicting hash:
    roll back
    return identity-conflict diagnostic

insert the one immutable normalized record
    link it to the same (source, event_id)

commit
if commit fails:
    return the commit error; the caller must treat the outcome as uncertain
return accepted result
```

The phrase “commit outcome is uncertain” is intentional. A process can lose its connection after PostgreSQL commits but before the client receives the acknowledgement. The retry path must be duplicate-safe; it must not pretend the first attempt definitely did or did not commit.

## Implementation constraints

- Add only the two application tables described above plus migration bookkeeping.
- Use SQL migrations with an obvious forward order and a reviewed rollback story; do not edit an applied migration in place.
- Keep raw payload size bounded before it enters PostgreSQL.
- Store source identifiers as strings unless the source contract guarantees another stable type.
- Store timestamps as UTC instants and normalize them at the boundary.
- Use parameterized SQL. Never interpolate payloads, IDs, or cursor values into SQL text.
- Keep provider DTOs out of persistence and public API packages.
- Use `pgxpool` and `pgx` with explicit SQL; do not add an ORM or `sqlc` before the small query set demonstrates a need.
- Normalize the source payload before opening the transaction.
- Use one transaction for raw and normalized inserts.
- Keep one runtime sink owner and sequential admission for the MVP. Do not start a goroutine per event or hold a mutex while waiting on PostgreSQL.
- Treat the received event and `json.RawMessage` as immutable. Clone only at a boundary that cannot guarantee exclusive ownership.
- Bound database calls with the operation context and let cancellation return through the owning sink loop.
- Make duplicate behavior explicit and observable; do not swallow every conflict as success.
- Do not add Kafka, a journal queue, Redis, leases, a schema registry, a general ORM, or a general query builder in this milestone.
- Do not claim exactly-once delivery. The demonstrated claim is at-least-once with idempotent effects for this managed PostgreSQL path.
- Keep implementation examples small. If syntax blocks learning, request a focused syntax hint using an unrelated domain example.

## Common failure modes

| Failure | Why it is dangerous | Evidence to seek |
|---|---|---|
| Raw insert commits before normalized write | Source appears accepted while the API is missing state | Inject a failure between the two operations and verify both roll back. |
| Normalized write commits separately | API state has no retained source evidence | Query both tables after an induced failure. |
| No database uniqueness | Concurrent or duplicate deliveries create duplicate rows | Run the duplicate test concurrently and inspect constraints. |
| Normalizer copies the provider DTO | Source-specific names leak into storage and the API | Review a small provider-neutral record contract before migrating. |
| Same event ID with a changed payload is ignored | A source identity collision can hide corruption or provider behavior | Compare hashes and record a diagnostic outcome. |
| Transaction holds a connection during parsing or HTTP | A small pool becomes an outage under slow input | Measure pool wait time and move work outside the transaction. |
| `published_at` changes after insertion | Existing records move between cursor pages | Make normalized rows immutable in this MVP and test the constraint or repository policy. |
| Migration is edited after release | Fresh and upgraded databases diverge | Apply migrations to both an empty and an already-versioned database. |

## Tests to write

Start with unit tests around normalization, then add PostgreSQL integration tests. Tests must not depend on the public data source.

- A migration creates exactly the expected application tables, columns, constraints, and ordering index.
- The Compose model validates, PostgreSQL becomes healthy, and ordinary stop/start preserves the named volume's data.
- A source-specific normalizer maps a valid recent-change fixture to the reviewed provider-neutral shape.
- Missing or wrongly typed required source fields return a normalization error before a transaction opens.
- A new event inserts one raw row and one normalized row.
- A failure injected between raw insert and normalized insert leaves neither row committed.
- Replaying an identical event does not add a raw row or duplicate normalized state.
- Reusing an event ID with a changed payload hash produces a visible conflict result.
- A duplicate does not update `published_at` or replace the normalized document.
- Two concurrent attempts for the same raw event converge to one logical effect.
- A cancelled context stops a pending database operation within a bounded test timeout.
- A bounded concurrent duplicate test proves the database uniqueness/transaction invariant without changing production to concurrent writes.
- Cancellation while PostgreSQL is unavailable releases the owned sink operation and permits runtime join.
- Race-enabled tests exercise event handoff and duplicate attempts without mutable-payload races.
- The normalized record can be read using the API's eventual `(published_at, record_id)` ordering query.

Prefer a real PostgreSQL integration test for constraints, transactions, `ON CONFLICT`, and row ordering. A mock that only checks SQL strings cannot prove PostgreSQL behavior.

## Commands to run

Use the project’s actual Compose and migration paths once they exist; the placeholders below describe the intended checks.

```powershell
go fmt ./...
go test ./...
go vet ./...
docker compose config
docker compose up -d postgres
docker compose ps
goose -dir migrations postgres "$env:DATABASE_URL" status
goose -dir migrations postgres "$env:DATABASE_URL" up
go test ./... -run Persistence
```

Inspect the API ordering query with PostgreSQL’s plan tool after the query exists:

```text
EXPLAIN (ANALYZE, BUFFERS) SELECT ... ORDER BY published_at DESC, record_id DESC LIMIT ...;
```

Do not use a destructive volume-removal command as a routine test shortcut. If a clean database is required, create an explicitly named test database or disposable Compose project and verify its target first.

## Acceptance criteria

This milestone is accepted when:

- a fresh PostgreSQL instance applies reviewed migrations successfully;
- the Compose service has documented configuration, a named volume, and a working readiness health check;
- the two-table raw/normalized design is visible in SQL and documentation;
- one source-specific normalizer produces the reviewed provider-neutral record shape;
- raw and normalized insertion are one atomic transaction for a new valid event;
- duplicate delivery produces one logical raw observation and one normalized effect;
- changed payload under an existing source identity is not silently accepted;
- production admission remains one owned sequential sink loop, while a bounded concurrent duplicate test proves the transaction/uniqueness invariant;
- event payloads remain immutable across runtime and database boundaries, with race-enabled evidence;
- normalized rows and API pagination columns are immutable and indexed;
- context cancellation and database failure have bounded, observable behavior; and
- the guide's tests pass without network access to Wikimedia.

## Post-MVP promotion gates

Do not promote this persistence design to a more complex transport until all gates below have evidence in the repository:

1. **Kafka gate:** demonstrate that stopping the database or sink causes retained backlog and that recovery catches up without duplicate logical state. Kafka is not added merely because the architecture diagram contains it.
2. **Mutable projection gate:** identify a real logical entity key and trustworthy monotonic version or ordering rule, then prove conditional updates reject older state before adding upserts.
3. **Second-provider gate:** demonstrate provider-ID mapping, source-specific fixtures, and a deterministic conflict policy before adding reconciliation tables.
4. **History/replay gate:** define retention, replay target isolation, and a correction ordering rule before rebuilding projections.
5. **Scale gate:** show query plans, pool saturation, payload sizes, and measured write/query limits before adding Redis, typed generated tables, or a separate storage system.
6. **Persistence worker-pool gate:** demonstrate with representative load that one sequential sink is the bottleneck; choose fixed worker and queue limits; prove idempotency under concurrent duplicates; document global or per-key ordering; and show that PostgreSQL pool/lock contention remains within budget. Otherwise retain one sink worker.

## Reflection questions

- Which database constraint would you miss most if it were removed?
- What evidence distinguishes an identical duplicate from an event-ID collision?
- What would a crash at each point of the transaction look like to the caller?
- Which part of the guarantee is provided by PostgreSQL, and which part is provided by retry/idempotency design?
- What would have to change before two normalized outputs per source event were safe?

## Review submission template

```text
Milestone: 09 — PostgreSQL persistence
What I implemented:
Migration files and database shape:
Normalizer input/output contract:
Transaction boundary:
Uniqueness boundaries:
What I expected:
What actually happened:
Commands I ran:
Tests and failure-injection results:
Evidence from the official resources:
Decision I am unsure about:
My explanation of duplicate and retry behavior:
What I intentionally did not build:
```
