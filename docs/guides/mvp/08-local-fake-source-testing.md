# Streamforge MVP - Local Fake-Source Testing

- Milestone: offline reliability and connector conformance
- Scope: test HTTP/SSE behavior with a local deterministic server
- Teaching rule: the public source is for bounded manual smoke tests, never a CI dependency

## Prerequisites

Complete the runtime, backpressure, and Wikimedia SSE guides. Have a synchronous collector whose HTTP client, endpoint, retry/backoff policy, limits, and request context can be supplied or controlled by a test. The runtime—not the collector—owns the event channel. Read the existing source-policy documents before adding any real-source test.

You should be able to explain:

- why fetching and parsing are separate seams;
- how a request context cancels a read;
- why a fake source should control timing through synchronization rather than arbitrary sleeps;
- the existing hint ladder: concept, direction, pseudocode, focused unrelated syntax, then rescue explanation.

## Conceptual explanation

A local fake source is a small HTTP server under test control. It lets one test choose the exact response status, headers, SSE frames, disconnect point, delay, malformed payload, and reconnect sequence. The collector then exercises the same net/http path it uses in production, but the test does not depend on DNS, Wikimedia availability, rate limits, source history, or network timing.

Use two layers of tests:

1. **Pure parser tests**: feed stored SSE text or individual frames to the framing/decoding seam. These are fastest and explain protocol rules.
2. **HTTP integration tests**: use `httptest.Server` to verify request headers, status/content-type checks, body cancellation, disconnects, reconnect budgets, and synchronous callback behavior at the real HTTP boundary.
3. **Runtime integration tests**: connect the synchronous collector to the Stage 5 runtime to prove that a full channel stops the runtime from requesting another event and cancellation reaches both the blocked send and active HTTP request.

The fake server is not a second production implementation. It is a fault-injection instrument with deliberately obvious behavior. Keep it inside tests or a test-support package; do not turn it into a general mock framework.

### C#/Java comparison

| Streamforge idea | C# analogy | Java analogy | Go lesson |
|---|---|---|---|
| httptest.NewServer | ASP.NET TestServer or WebApplicationFactory | embedded server, MockWebServer, or WireMock | The standard library supplies a loopback server for end-to-end HTTP tests. |
| Handler closure | test endpoint delegate | request handler/controller stub | Configure behavior directly and record only facts needed by the test. |
| httptest.ResponseRecorder | in-memory response result | mock response object | Useful for a handler; not a replacement for a streaming server when flush and cancellation matter. |
| request context | CancellationToken | request cancellation/interruption | A client test can cancel the request and assert the server observes it. |
| injected http.Client/transport | injected HttpMessageHandler | injected client/transport | Dependency injection can be one field or function; no container is needed. |

Go’s httptest package is intentionally small. For this MVP, prefer one local server and explicit test fixtures over adding WireMock, MockWebServer, a fake DNS layer, or a large HTTP abstraction.

## Targeted official resources

- [net/http/httptest](https://pkg.go.dev/net/http/httptest) - NewServer, NewRequestWithContext, server closure, and client connections.
- [net/http](https://pkg.go.dev/net/http) - request contexts, response-body ownership, client timeout behavior, ResponseHeaderTimeout, and ResponseController.Flush.
- [context](https://pkg.go.dev/context) - cancellation and deadlines.
- [bufio](https://pkg.go.dev/bufio) - bounded line scanning and scanner failure behavior.
- [testing](https://pkg.go.dev/testing) - test deadlines, cleanup, and parallel-test considerations.
- [Go race detector](https://go.dev/doc/articles/race_detector) - execute concurrent paths instead of assuming they are safe.
- [`testing/synctest`](https://pkg.go.dev/testing/synctest) - deterministic timing/deadlock checks where compatible with the HTTP test boundary.
- [Concurrency track testing rules](go-concurrency-track.md#15-deterministic-concurrency-testing) - explicit signals, lifetime ownership, and no sleep-based synchronization.
- [WHATWG Server-sent events](https://html.spec.whatwg.org/dev/server-sent-events.html) - the framing cases the fake stream must cover.

Reading task:

1. Locate the httptest.Server documentation that says it listens on a system-chosen loopback port.
2. Locate the statement that Server.Close waits for outstanding requests and decide when CloseClientConnections is useful.
3. Compare the response recorder with a real streaming server. Explain why an SSE cancellation test needs the latter.

## Fake-source scenarios

Build a small table of scenarios rather than one giant test with many flags:

| Scenario | Server behavior | Expected collector behavior |
|---|---|---|
| Valid stream | 200, correct media type, two complete events | One synchronous collection run invokes two callbacks in order while retaining one stream. |
| Header rejection | 200 with JSON media type | Stops with protocol error before JSON parsing as SSE. |
| Status rejection | 500 or a configured non-success status | Classifies response and applies bounded policy. |
| Canary | Valid event whose payload carries the documented canary marker | Skips it before invoking the callback for the next real event. |
| Multiline data | One message with multiple data fields | Applies SSE assembly rules and decodes one JSON payload. |
| Malformed JSON | Complete SSE frame with invalid data | Reports a parse failure; does not invoke the callback with a valid event. |
| Oversized frame | One line or assembled event over the configured limit | Rejects within bounded work. |
| Remote disconnect | Send a prefix, flush, then close the connection | Attempts only the configured number of reconnects. |
| Slow body | Send headers, then wait for a test-controlled release | Context cancellation releases the client and server. |
| Request contract | Record headers and path | Sees the descriptive User-Agent, expected path, and no credential header. |
| Cursor policy | Record reconnect request headers | MVP does not send a durable Last-Event-ID. |
| Identity separation | Valid payload `meta.id` plus a different SSE `id` | Envelope identity comes from payload metadata; SSE `id` remains cursor provenance. |
| Identity fallback | Omit `meta.id`, then vary wiki/recent-change ID and the raw payload | Uses the documented deterministic fallback order and never a local counter. |

For disconnect tests, make each reconnect response distinct so the test can prove which attempt ran. For timing tests, use a channel that the test controls; do not rely on a particular scheduler delay.

## Design questions

1. Which behaviors belong in pure parser tests, and which require a real HTTP server?
2. Why is a loopback server more valuable here than a fake function that returns a response object?
3. How can the fake server prove the collector closes the response body and observes cancellation?
4. How will you guarantee that a reconnect test cannot run forever?
5. Which request headers are part of the source contract, and which should never be present in the test?
6. How do you test a long-lived response without making the suite wait for a real long-lived connection?
7. Why should a test assert the documented absence of durable Last-Event-ID recovery in the MVP?
8. What fixture would expose a parser that emits on data: lines instead of blank-line dispatch?

## Deliberately non-compilable pseudocode

This is a test plan, not complete Go code.

~~~text
create local loopback server with handler
    record request path and User-Agent
    choose response by attempt number
    write status and text/event-stream media type
    write a complete SSE frame
    flush the frame
    if scenario says disconnect:
        close the response connection
    if scenario says wait:
        wait for test-controlled release or request cancellation

create collector configured with the server URL,
bounded event limit, reconnect budget, and test cancellation
run synchronous collection with a test callback under test control

for expected outcome:
    wait for callback event, classified error, reconnect observation,
    cancellation observation, or test deadline

cancel collection
close the test server
assert request facts, callback events, dropped canaries,
error classification, attempt count, and bounded completion
~~~

## Implementation constraints

- Use httptest.NewServer or the equivalent standard-library test helper.
- Keep all automated tests offline; no test may call stream.wikimedia.org.
- Inject a client, transport, or endpoint URL rather than hard-coding a live URL in tests.
- Keep the fake handler’s state minimal and synchronized; record only the request facts needed for assertions.
- Use explicit test cleanup so server and client connections close even when an assertion fails.
- Use request contexts and test-controlled channels for cancellation and release.
- Set test deadlines so a broken collector fails quickly instead of hanging the suite.
- For stream responses, send headers before body data and flush where the test needs the client to observe an incremental frame.
- Do not use time.Sleep to “wait until” a handler has run; use a channel or another synchronization point.
- Do not start the collector in an unowned goroutine. If the test starts a caller goroutine because the operation blocks, the test owns its cancellation and join.
- Test the runtime's full-channel send separately from the collector's synchronous parser behavior.
- Keep pure parser fixtures small and readable; include comments, blank lines, multiline data, unknown fields, canary payloads, malformed JSON, and boundary-sized frames.
- Do not add third-party mock servers, Kafka, Redis, or a generic test DSL for this milestone.
- Do not test durable cursor recovery before the storage stage and its promotion gates exist.

Focused syntax hint policy: request a focused syntax example using an unrelated health-check endpoint or library-catalog response. Do not receive a complete Streamforge fake-server test.

## Common failure modes

| Failure | Why it happens | Correction |
|---|---|---|
| CI reaches the public stream | A live URL leaked into a test default | Require an explicit injected test endpoint and assert it is loopback. |
| Test hangs on a blocked body | No cancellation or test deadline | Cancel through the request context and apply a bounded test deadline. |
| Test passes without proving streaming | Entire response is written before the client reads | Flush after a frame and gate the next write on a test channel. |
| Reconnect test is flaky | Wall-clock sleeps race the scheduler | Synchronize on request count and channels; use an injected backoff seam. |
| Server closes before assertions | Cleanup or handler state is unsynchronized | Coordinate handler observations and call cleanup after collection stops. |
| Wrong media type is accepted | Test only checks status | Assert status and parsed media type independently. |
| Malformed frame is hidden | Fake server sends a complete valid frame by accident | Store malformed fixtures as exact bytes and assert no callback event. |
| Canary filter is not exercised | Test payload omits the actual marker path | Keep a fixture representing the documented marker and assert no callback for the canary. |
| Request secrets leak | Test client inherits production headers | Build the test client/config explicitly and assert forbidden headers are absent. |
| Test asserts implementation details | It checks an exact goroutine count or private names instead of behavior | Assert callback events, errors, owned-task completion, timing bounds, and request facts. |
| httptest.ResponseRecorder masks a stream bug | Recorder buffers instead of behaving like a live connection | Use httptest.Server for incremental body and cancellation tests. |

## Tests to write

### Pure parser tests

- one complete message with event, id, and data;
- multiple data lines and a blank-line dispatch;
- comments and empty messages;
- unknown fields and alternate line endings;
- malformed JSON and missing required source fields;
- canary marker accepted for filtering;
- line-size and event-size boundaries;
- no callback before the blank-line delimiter.

### HTTP integration tests

- correct request path and descriptive User-Agent;
- every identity fallback branch remains distinct from SSE cursor provenance;
- 200 plus correct text/event-stream media type;
- wrong status and wrong content type;
- response body cancellation while the handler is blocked;
- disconnect followed by exactly the configured number of bounded reconnect attempts;
- cancellation during reconnect backoff;
- runtime integration: bounded output-channel send when the sink is intentionally slow, without moving channel ownership into the collector;
- no Last-Event-ID header for the non-resumable MVP policy;
- server and client cleanup after success, error, and cancellation;
- repeated execution under go test -race where supported.
- a test-owned blocking call is cancelled and joined; no background collector goroutine survives cleanup.

Use table-driven tests for the scenario matrix, but keep each row’s expected guarantee obvious. A test that needs more than one independent fault to explain its failure should probably be split.

## Commands to run

~~~powershell
go fmt ./...
go test ./...
go test -run 'Test.*SSE|Test.*Fake|Test.*Stream' ./...
go vet ./...
go test -race ./...
~~~

Run one manual smoke test against the real source only after the offline suite passes. Record the observation window and stop the client deliberately; do not make a live connection part of acceptance.

## Acceptance criteria

The offline testing milestone is complete when:

- every collector behavior needed for the MVP has a deterministic local test;
- parser tests cover framing independently from network behavior;
- HTTP tests verify status, media type, User-Agent, cancellation, body closure, reconnect budget, and size boundaries;
- runtime integration separately verifies channel backpressure, cancellation-aware send, and lifetime join;
- the fake source can simulate a disconnect, slow body, malformed event, canary, and wrong response;
- no test requires public Internet access or a credential;
- no test waits on arbitrary sleeps or can hang beyond its deadline;
- tests assert the MVP’s deliberate absence of durable Last-Event-ID recovery;
- cleanup runs on success and failure;
- go fmt ./..., go test ./..., and go vet ./... pass.

## Reflection questions

1. Which failure could you reproduce locally only after adding a synchronization seam?
2. What does the fake server prove that a pure parser fixture cannot prove?
3. Which assertion protects the Wikimedia source from accidental misuse?
4. How would you evolve the tests if durable cursor recovery were promoted later?
5. What is the smallest fake-source API that would still support the current scenarios?
6. Which test would give the clearest evidence that the collector is cancellation-safe?

## Review submission template

Use the existing hint ladder and submit this at review:

~~~text
Milestone: Local fake-source testing
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of the fake server, parser seam, cancellation test, reconnect budget, and offline guarantee:
~~~

The mentor reviews determinism, source isolation, HTTP contract coverage, cleanup, cancellation, boundary testing, and whether the fake source remains a focused test instrument.
