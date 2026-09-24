# Streamforge MVP — Read-Only HTTP API

- Status: Draft MVP guide
- Scope: one bounded, read-only canonical-record endpoint
- Teaching rule: the learner writes the implementation

## Outcome

Expose normalized PostgreSQL records through a small versioned HTTP API:

```text
GET /health/live
GET /health/ready
GET /api/v1/records?limit=50
GET /api/v1/records?limit=50&cursor=<cursor>
```

The MVP has no write endpoint, consumer API keys, quotas, filter DSL, arbitrary sorting, signed cursors, or general query builder. The endpoint is intentionally narrow so its SQL, response contract, and failure behavior can be tested deeply.

## Prerequisites

Complete [09 — PostgreSQL persistence](09-postgresql-persistence.md) and the earlier source/normalization milestones in the [MVP learning guide](../mvp-learning-guide.md). You should understand the raw/normalized table boundary, the transaction guarantee, and the existing [hint ladder](../mvp-learning-guide.md#hint-ladder).

You should also be comfortable with:

- `net/http` handlers and response writers;
- context cancellation from an incoming request;
- JSON encoding and stable DTOs; and
- parameterized SQL and PostgreSQL row ordering.

Read the structured-lifetime, concurrency-limit, timeout, and observability sections of the [Go concurrency track](go-concurrency-track.md). `net/http` may serve requests concurrently; that does not justify starting another goroutine inside each handler.

## Conceptual explanation

### The API is a product boundary

The database schema is an implementation detail. A provider’s response shape, SQL row, and public JSON response are three different shapes:

```text
provider DTO -> canonical persistence model -> public API DTO
```

The public DTO should expose only the reviewed normalized record contract, such as record ID, kind, occurred time, publication time, and the bounded provider-neutral document. It should not expose Wikimedia field names by accident, raw payloads, SQL null implementation details, source configuration, or internal cursor state.

### Stable two-column keyset pagination

The only collection pagination contract in this MVP is:

```text
ORDER BY published_at DESC, record_id DESC
```

`published_at` is the immutable database publication timestamp in `normalized_records`. `record_id` is immutable and unique. The second column is required because timestamps can tie. The pair is the total ordering key.

For a later page, the query continues strictly before the cursor:

```text
WHERE (published_at, record_id) < (cursor_published_at, cursor_record_id)
ORDER BY published_at DESC, record_id DESC
LIMIT requested_limit + 1
```

The cursor is an unsigned, untrusted encoding of exactly those two values. It is not signed, does not contain authorization, and does not contain a filter hash because filters and authorization are outside this MVP. Strict validation is still required: reject malformed base64, extra fields, non-UTC timestamps, invalid IDs, empty values, repeated cursor parameters, oversized cursors, and impossible limits.

Use `limit + 1` to determine whether another page exists. Return at most `limit` records. If the extra row exists, derive the next cursor from the last returned record; otherwise return no next cursor. Never fetch an unbounded result set to discover page boundaries.

### Why keyset instead of offset

Offset pagination asks the database to skip a growing number of rows and can shift when records are inserted or updated. Keyset pagination uses the last known position in a deterministic order. It is not a magic consistency guarantee: records whose immutable ordering key is outside the already-read boundary may still appear on a later request, and a client that changes the endpoint contract mid-walk must restart from the first page.

The MVP's narrower promise is: with immutable `published_at` and `record_id`, a cursor walk follows one deterministic order without duplicate positions caused by timestamp ties. It is not a transactionally frozen snapshot; records published after the walk begins may fall outside the pages that client sees.

### Bounded request work

The handler must bound:

- accepted `limit` values, for example default 50 and maximum 100;
- cursor byte length;
- response size through the maximum row count and payload-size policy; and
- database work through request context deadlines and the indexed keyset query.

Do not add arbitrary field filters or sorts until the project has an allow-list, type mapping, index plan, and security review. “Flexible” SQL built from request strings is a query-injection and operational-risk boundary.

## C#/Java comparison

| Familiar idea | Go HTTP MVP interpretation |
|---|---|
| ASP.NET controller or Spring MVC method | A small `net/http` handler with explicit dependency wiring. A router is optional; the handler contract is the important boundary. |
| `CancellationToken` / request-scoped cancellation | `r.Context()` passed to database calls; a client disconnect can end work that no longer has a consumer. |
| Jackson/System.Text.Json DTO | A dedicated public response struct. Do not serialize database or provider structs by accident. |
| `Pageable`/offset page | A validated cursor plus bounded limit; the SQL compares the two ordering columns. |
| Bean validation / model binding | Explicit parsing and validation of query parameters. Invalid input returns a stable 400 response. |
| MockMvc/WebTestClient/MockWebServer | `httptest.NewRecorder` and `httptest.NewServer` for handler and source-boundary tests. |
| Middleware pipeline | Plain handler composition is enough for this milestone. Add middleware only when it expresses a real cross-cutting requirement. |

Go’s standard library gives a useful default without requiring a framework lifecycle. That also means the application must deliberately choose status codes, headers, timeouts, and error behavior.

## Targeted official and primary resources

- [`net/http` package](https://pkg.go.dev/net/http) — handlers, request contexts, response writing, and server behavior.
- [`net/http/httptest` package](https://pkg.go.dev/net/http/httptest) — recorder and test server tools for deterministic HTTP tests.
- [Go: Accessing relational databases](https://go.dev/doc/database/) — context-aware database operations and query boundaries.
- [`pgxpool` package](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool) — context-aware pool queries and row handling for the PostgreSQL path chosen in stage 9.
- [PostgreSQL indexes](https://www.postgresql.org/docs/current/indexes.html) — support the exact ordering/filter shape used by the endpoint.
- [PostgreSQL `EXPLAIN`](https://www.postgresql.org/docs/current/using-explain.html) — verify the bounded query plan instead of assuming it.
- [OpenAPI Specification](https://spec.openapis.org/oas/latest.html) — read-only contract documentation once the behavior is stable.
- [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339) — timestamp representation used by the cursor and response.

## API contract to design

### Request

`GET /api/v1/records`

- `limit`: optional decimal integer; choose and document a default and maximum. Reject zero, negative, non-decimal, overflowed, repeated, or otherwise invalid values.
- `cursor`: optional strict two-column cursor. Reject repeated or malformed values. Absence means the first page.
- no other query parameters in the MVP. Reject unknown parameters rather than silently pretending they were applied.

### Success response

Use one stable JSON envelope. The exact field names are the learner’s design decision, but the response must make the page boundary explicit:

```text
{
  data: [normalized record DTOs ...],
  next_cursor: string or null
}
```

Use a consistent content type and API-version header. Choose whether an empty page returns an empty array or an error; the recommended MVP behavior is `200` with an empty array and no next cursor.

### Error response

Use one stable JSON error shape for invalid parameters and dependency failures. A minimal shape can contain an application error code, human-safe detail, and request ID. Never include SQL text, credentials, raw payloads, stack traces, or cursor internals that expose more than the contract requires.

Recommended status decisions:

| Situation | Status |
|---|---:|
| Valid page, including empty page | 200 |
| Invalid or unknown query parameter | 400 |
| Database unavailable or request deadline exceeded while reading | 503, according to the project’s health/error policy |
| Unexpected handler failure | 500 |
| Unknown route | 404 |

Do not turn a database outage into an empty successful page. An empty result and an unavailable dependency mean different things to a client.

## Design questions

1. Why must the pagination timestamp be immutable for the lifetime of a record?
2. Why is `record_id` needed when timestamps already exist?
3. What does the strict `<` comparison mean for descending order?
4. Why fetch one extra row instead of running a separate `COUNT(*)` query?
5. Which malformed cursor cases should return 400 rather than an empty page?
6. Why should unknown query parameters be rejected in this small contract?
7. What public fields are canonical, and which provider details must remain private?
8. How does request cancellation reach the database query?
9. What evidence would show that the ordering index is actually used?

## Deliberately non-compilable pseudocode

This is not Go, SQL, or a complete implementation.

```text
NOT RUNNABLE — PSEUDOCODE ONLY

handler receives GET request
reject every query parameter except limit and cursor
parse limit exactly once; require 1 <= limit <= configured maximum
if cursor exists:
    decode the strict two-field representation
    require canonical UTC timestamp and valid immutable record ID

query with request context:
    if first page:
        order by published_at descending, record_id descending
    else:
        require (published_at, record_id) strictly before the cursor pair
        use the same order
    fetch limit + 1 rows

if more than limit rows arrived:
    retain only the first limit
    derive next cursor from the last retained row
else:
    next cursor is absent

map database rows into public canonical DTOs
write headers, status, and JSON envelope
```

The cursor encoding/decoding syntax is intentionally omitted. If it is the only blocker, request a focused syntax hint using an unrelated example such as paginating library books.

## Implementation constraints

- Keep the API read-only and versioned under `/api/v1`.
- Implement one collection endpoint plus liveness/readiness checks; do not build a generic dataset gateway.
- Use the immutable `(published_at, record_id)` keyset order and document it.
- Enforce default and maximum limits before querying.
- Use `limit + 1`; never rely on a count query or unbounded query for `has_next`.
- Strictly validate and bound the unsigned cursor. Do not sign it in the MVP.
- Parameterize every value. Do not concatenate cursor values, limits, JSON paths, identifiers, or sort expressions into SQL.
- Use one reviewed SQL query shape. Do not add a general query builder or filter language.
- Pass request context through the database call and enforce a bounded server/database timeout.
- Let the HTTP server own request goroutines. Handlers perform one bounded query synchronously under the request context; do not start fire-and-forget query goroutines.
- Bound concurrency through page limits, request deadlines, and the PostgreSQL pool first. Add an application semaphore only after a load test identifies a smaller safe limit and defines reject/wait behavior.
- Do not hold an application mutex during database or response I/O.
- Keep consumer quotas and API-key rate limiting outside the MVP; operational concurrency protection is a different concern.
- Keep public DTOs separate from provider and persistence models.
- Return `[]`, not a null collection, for an empty successful page if that is the chosen contract; test the decision.
- Do not add API keys, quotas, Redis, React, SSE, WebSockets, or write endpoints in this guide.

## Common failure modes

| Failure | Result | Test or prevention |
|---|---|---|
| Offset pagination slips into the endpoint | Work grows with page number and concurrent inserts shift results | Inspect SQL and test a cursor walk. |
| Order uses timestamp only | Equal timestamps can produce unstable or repeated boundaries | Insert tied timestamps and verify ID tie-breaking. |
| Source-controlled business time is the cursor column | Late or skewed source time makes publication order surprising | Use immutable database `published_at`; keep `occurred_at` as data. |
| Cursor comparison uses `<=` | The boundary row can repeat | Use strict comparison and assert no duplicate IDs. |
| Cursor is decoded but not validated | Malformed or attacker-controlled values reach SQL or cause panics | Table-test every invalid shape and size. |
| Limit is parsed after querying | A huge limit can exhaust memory or database work | Validate before building query arguments. |
| `limit + 1` row is returned | Clients see an unexpected extra record | Trim before encoding the response. |
| DB failure returns `200 []` | Clients mistake an outage for “no records” | Test a dependency failure explicitly. |
| Provider DTO is serialized directly | Source changes leak into the public contract | Compile-time/package review plus response fixture. |
| SQL is assembled from request strings | Query injection and unreviewed plans become possible | Keep query shape fixed and values parameterized. |

## Tests to write

Use table-driven handler tests with `httptest`. Add PostgreSQL integration tests for real ordering and query behavior.

- Default limit is applied.
- Maximum limit is accepted; maximum plus one is rejected.
- Zero, negative, overflowed, repeated, and non-decimal limits are rejected.
- Unknown query parameters are rejected.
- First page is ordered by `published_at DESC, record_id DESC`.
- Equal timestamps are ordered by descending immutable ID.
- A page with `limit + 1` rows returns exactly `limit` rows and a next cursor.
- A final page returns no next cursor.
- Following the cursor produces no duplicate record ID.
- A cursor with invalid base64, extra fields, non-UTC timestamp, invalid ID, oversized value, or trailing data returns 400.
- Normalized records and their `published_at` values remain immutable in this MVP.
- Database cancellation or outage does not return a successful empty page.
- Response JSON contains canonical fields only and always uses the documented envelope.
- Liveness and readiness differ according to the chosen dependency policy.
- A cancelled request context stops a blocked repository query and leaves no handler-owned goroutine.
- A bounded concurrent-request test cannot exceed the reviewed database/application limit.
- Overload behavior is explicit—bounded wait or a stable rejection—not an unbounded in-memory request queue.

Do not use only a mocked repository for the pagination test. A real PostgreSQL query should prove row comparison, index-compatible ordering, and `limit + 1` behavior.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
go test ./... -run 'API|Pagination|Handler'
docker compose config
docker compose up -d postgres
```

Run a local smoke test after starting the API. Replace the address with the project’s configured one:

```powershell
Invoke-WebRequest http://localhost:8080/health/live
Invoke-WebRequest 'http://localhost:8080/api/v1/records?limit=2'
```

If the response includes a cursor, repeat the request with the URL-encoded cursor and compare the record IDs across pages.

## Acceptance criteria

This milestone is accepted when:

- the read API returns canonical normalized records without provider DTO leakage;
- limits, cursor syntax, status codes, and response envelopes are documented and tested;
- pagination is exactly the immutable timestamp-plus-ID keyset contract;
- `limit + 1` determines next-page existence;
- malformed cursor and parameter inputs fail closed with 400;
- database failures are distinguishable from empty data;
- request cancellation reaches the database, no handler starts unowned work, and concurrent request work remains within reviewed bounds;
- overload produces bounded waiting or stable rejection rather than an unbounded application queue;
- the SQL uses bound parameters and an explainable index-supported plan; and
- no API keys, quotas, signed cursors, filter DSL, or general query builder has been introduced.

## Post-MVP promotion gates

Promote the endpoint only when the following evidence exists:

1. **Filter gate:** an allow-list of fields, operators, type mappings, indexes, and query-plan tests exists before any filter is added.
2. **Sort gate:** every new sort has an immutable deterministic tie-breaker and a bounded index/query plan.
3. **Consumer-control gate:** API keys or quotas are added only after a real multi-consumer requirement and an authorization/threat-model test plan exist.
4. **Live-delivery gate:** SSE or another stream is added only after the read contract is stable and a live freshness requirement is demonstrated.
5. **Scale gate:** Redis or another cache is justified by measured database pressure and cache correctness tests, not by a default architecture diagram.
6. **API concurrency/rate-limit gate:** add a handler semaphore or consumer rate limiting only after representative load shows the database pool and request deadlines are insufficient, with explicit wait/reject behavior, low-cardinality saturation metrics, and no API-key/quota scope leakage into the lean MVP.

## Reflection questions

- What does a cursor promise to a client, and what does it intentionally not promise?
- Why is “empty page” different from “database unavailable”?
- Which API field would be hardest to change after clients depend on it?
- How would you explain the two-column order to someone who only knows offset pagination?
- What is the smallest additional feature that would force a new query-safety design?

## Review submission template

```text
Milestone: 10 — read-only HTTP API
Public route and response contract:
Pagination columns and immutability decision:
Cursor validation rules:
Limit and bounded-work policy:
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Tests and page-walk results:
Database plan evidence:
Decision I am unsure about:
What I intentionally did not build:
```
