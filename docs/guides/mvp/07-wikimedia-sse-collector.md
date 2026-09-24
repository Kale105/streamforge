# Streamforge MVP - Wikimedia EventStreams Collector

- Milestone: first permitted real streaming source
- Source: Wikimedia EventStreams `recentchange` SSE
- Scope: one public stream, one bounded collector, best-effort reconnect, no durable cursor recovery

## Prerequisites

Complete the event contract, collector/sink boundaries, runtime orchestration, and backpressure experiment. Use the minimal local `httptest` seam required for this collector's tests; stage 8 then expands it into the full fake-source scenario matrix. Never make the public stream an automated-test dependency.

You should already understand:

- `context.Context` cancellation;
- `http.Client`, request contexts, and response-body ownership;
- the synchronous collector contract and runtime-owned bounded channel;
- line-oriented parsing and JSON validation;
- the existing hint ladder: concept, direction, pseudocode, focused unrelated syntax, then rescue explanation.

No API key, OAuth token, Kafka topic, Redis cache, lease, or external plugin is part of this collector.

## Why this source

The real source is Wikimedia EventStreams, already preferred as the first streaming collector by `docs/architecture/platform-design-v2.md`. The MVP uses one endpoint:

```text
https://stream.wikimedia.org/v2/stream/recentchange
```

It is a public, credential-free HTTP service that exposes a realistic long-lived stream. It uses the Server-Sent Events protocol, so one response remains open and delivers framed events over time. This makes it useful for learning connection lifetime, incremental parsing, cancellation, backpressure, and reconnect behavior without inventing a fake wire protocol.

Credential-free access is not a promise of unlimited traffic or blanket permission to redistribute every field. Send a descriptive User-Agent with project and contact information, follow Wikimedia usage guidance and throttling signals, preserve source-policy decisions, and keep the collector configurable and disableable.

The MVP intentionally does not promise zero loss, exact resume, or exactly-once delivery. EventStreams documents an `id` field and `Last-Event-ID` resume mechanism, but reliable recovery requires a durable checkpoint that is committed with the accepted event and a tested crash boundary. Until the storage stage makes that safe, the collector may observe a disconnect, reconnect from the live endpoint without a durable cursor, and produce a gap or duplicate. That limitation must be visible in the source capability and review notes.

## Conceptual explanation

### HTTP response versus stream

An ordinary HTTP client often reads a finite response to completion. An SSE collector instead:

1. creates a request with a cancellation context;
2. sends a descriptive User-Agent and requests `text/event-stream`;
3. validates the status and media type before reading the body;
4. keeps the response body open and parses incrementally;
5. invokes the synchronous event callback only after a complete SSE message has been framed and validated;
6. keeps the body open across callbacks, then closes it on disconnect, reconnect, cancellation, or collection return;
7. classifies cancellation, remote disconnect, protocol errors, and source errors separately.

The response has no useful whole-body size bound because it is intentionally long-lived. Apply boundaries that are meaningful for a stream instead:

- require the expected successful status for this connector;
- require media type `text/event-stream` (a UTF-8 charset parameter is acceptable);
- configure a response-header/connect wait bound;
- cap each SSE line and assembled event/data payload;
- cap the number of data lines or fields retained for one event;
- never call an unbounded whole-body read on the stream.

The exact limits are configuration and experiment decisions. Choose conservative development defaults, expose the rejection reason, and test the boundary rather than treating a number as a source guarantee.

### SSE framing

The SSE stream is text. A message is a sequence of fields terminated by a blank line. Relevant fields for this lesson are:

- `event`: the event type; ordinary data messages are commonly `message`;
- `id`: the SSE resume cursor representation; it is not the same thing as the source event's identity;
- `data`: one or more lines whose payload is assembled according to the SSE rules;
- a blank line: dispatches the accumulated message;
- a line beginning with `:`: a comment/keep-alive and not a data message.

Unknown fields should not crash the parser. A message with no data should not reach the event callback. Data must be valid JSON before it crosses the collector boundary. The collector owns its response/parser state for one synchronous collection run; it does not start a hidden reader goroutine. The callback must return before the collector reads the next frame, which preserves backpressure and one clear body lifetime.

### Event identity versus SSE cursor

The recent-change JSON schema describes `meta.id` as a unique event ID when present, but it is not listed as required. Use a reviewed deterministic identity policy for the Streamforge envelope together with a stable Streamforge source value:

1. prefer non-empty payload `meta.id`;
2. otherwise use the documented wiki plus non-null recent-change `id` tuple; and
3. only when neither exists, use a deterministic hash of the bounded raw payload and document that equivalent reserialization can produce a different identity.

Treat the SSE `id` field as cursor/provenance only.

Do not use the SSE cursor as the event ID, and do not derive logical record identity from it. The cursor can describe multiple topic partitions and exists to resume stream position. The payload identity policy identifies the observation. Never fall back to a random value or restart-unsafe local counter.

### Canary filtering

Wikimedia’s EventStreams documentation demonstrates discarding canary events. Apply this filter before invoking the callback so canaries do not enter the runtime channel or become misleading “real” observations. Treat the field path and comparison as source-specific parsing logic, not a general platform filter language.

The filter must be explicit and testable. If the expected canary marker is missing or the payload shape is incompatible, classify the event according to the parser policy; do not silently treat every malformed payload as a canary.

### C#/Java comparison

| Streamforge idea | C# analogy | Java analogy | Go lesson |
|---|---|---|---|
| `http.Client` plus request context | `HttpClient.SendAsync` with `CancellationToken` | `HttpClient.sendAsync` or a streaming body subscriber | The request context covers connection, headers, and body lifetime; cancellation must reach the read. |
| Response body | `HttpContent.ReadAsStreamAsync` | response input stream | A long-lived body is not a string; consume it incrementally and close it. |
| `bufio.Scanner`/reader | `StreamReader.ReadLineAsync` | `BufferedReader.readLine` | Line parsing still needs explicit per-line and per-event limits. |
| Scoped body cleanup | `using`/`await using` | try-with-resources | Close each connection on terminal/reconnect paths; a defer placed in a long reconnect loop can postpone cleanup too long. |
| SSE `id` | `Last-Event-ID` header in a library | `Last-Event-ID` request header | The protocol supports resume; the MVP does not claim durable resume until storage makes it safe. |

## Targeted official/primary resources

- [Wikimedia Event Platform/EventStreams HTTP Service](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service) - read **Streams**, **API**, **Stream selection**, and **Response Format**. Verify the endpoint, public stream behavior, media type, event fields, and documented cursor mechanism.
- [Wikimedia EventStreams API documentation](https://stream.wikimedia.org/?doc) - inspect the current generated stream listing when implementing; the available list can change.
- [Wikimedia recent-change schema](https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml) - inspect the required event metadata and payload fields used by the normalizer.
- [Wikimedia User-Agent Policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy/en) - descriptive agent and contact expectations.
- [Wikimedia API Usage Guidelines](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_API_Usage_Guidelines/en) - rate/throttling, identity, and responsible use.
- [WHATWG Server-sent events](https://html.spec.whatwg.org/dev/server-sent-events.html) - media type, UTF-8, event framing, reconnect behavior, and `Last-Event-ID`.
- [`net/http` package](https://pkg.go.dev/net/http) - request context, response-body closure, client timeout behavior, and `ResponseHeaderTimeout`.
- [`bufio` package](https://pkg.go.dev/bufio) - scanner line limits and when a reader is preferable for larger or more controlled input.

Reading task:

1. Compare Wikimedia’s example raw frame with the WHATWG field and blank-line rules.
2. Explain why a fixed `http.Client.Timeout` that includes the response body is usually wrong for an intentionally long-lived stream.
3. Find the exact place where the source documentation discusses `Last-Event-ID`, then write why technical support for it is not the same as a durable application guarantee.

## Design questions

1. What makes EventStreams a realistic learning source while keeping the first connector credential-free?
2. Which response checks happen before the body is read, and which size checks happen while the body is read?
3. How do blank lines, comments, multiple `data` lines, and unknown fields affect the parser state machine?
4. What is the event-identity fallback order, what does the SSE `id` represent, and why must they remain separate?
5. Why must canary filtering happen before invoking the runtime callback?
6. Which errors should reconnect, which should stop immediately, and which should be counted as malformed source data?
7. What does “best-effort reconnect without a durable cursor” permit you to claim, and what must you explicitly refuse to claim?
8. What promotion gates would make `Last-Event-ID` safe enough to enable?

## Deliberately non-compilable pseudocode

This describes parser and lifecycle behavior without providing a complete implementation.

```text
repeat until root context is cancelled or reconnect budget is exhausted:
    create GET request for the recentchange endpoint
    attach descriptive User-Agent and cancellation context
    do request

    if status is not the accepted success status:
        close body if present
        classify source response
        apply bounded reconnect policy or stop

    if media type is not text/event-stream:
        close body
        classify protocol failure
        stop or consume one bounded diagnostic body

    initialize empty SSE message state
    read one bounded line at a time:
        if cancellation is observed:
            close body and return expected shutdown
        if line is a comment:
            continue
        if line is a field:
            update event type, SSE cursor id, or data-line accumulator
        if line is blank:
            if data accumulator is empty:
                reset message state
                continue
            decode assembled data as JSON
            if payload is marked as Wikimedia canary:
                discard and reset message state
                continue
            choose identity through the reviewed deterministic fallback order
            detach any reusable payload buffer
            invoke synchronous callback with one bounded immutable event and cursor provenance
            if callback fails: close body and return that failure
            reset message state

    classify EOF or read error as disconnect
    close body
    wait for bounded backoff with cancellation
    reconnect without a durable Last-Event-ID in this MVP
```

## Reconnect and cursor policy

Use a small explicit policy, for example:

- retry only a bounded number of times after a remote disconnect or transient transport failure;
- use a capped backoff with jitter and make the sleep cancellation-aware;
- stop immediately for invalid configuration, wrong content type, unsupported status, repeated malformed framing, or an exhausted retry budget;
- do not reconnect in a tight loop;
- do not send a cursor recovered only from process memory after restart;
- may retain the most recent SSE `id` in an in-memory diagnostic value during one connection, but must label it non-durable and never use it as event identity or claim recovery;
- report reconnect attempts, last successful event time, stop reason, and whether the stream may have a gap.

Treat connection concurrency, request timeout, rate limiting, and retry as different controls. The MVP opens one conservative stream connection. A request context owns cancellation of connection setup and body reading; do not apply a short client-wide timeout that kills a healthy long-lived stream. Reconnect attempts have one total owner and bounded elapsed/attempt budgets. Capped jittered delay prevents synchronized retry bursts, but delay must be injectable in tests and cancellation-aware. Do not stack retries in `http.Client`, collector, and runtime, or multiply attempts by launching reconnect goroutines.

The number of attempts and delay cap are implementation parameters to be reviewed, not universal Wikimedia requirements. Pick a small development budget and test it deterministically.

### Promotion gates for durable resume

Do not promote cursor recovery until all gates below are met:

1. PostgreSQL or another approved durable store can atomically commit the accepted observation and the source checkpoint candidate.
2. The checkpoint is advanced only after durable admission, matching the platform architecture’s durability-before-acknowledgement rule.
3. Crash tests cover before admission, after admission, and after checkpoint commit.
4. The connector handles expired, malformed, rejected, and too-old cursors without a hot loop.
5. Duplicate delivery is safe through the Streamforge `(source, event ID)` identity contract, where event ID comes from the reviewed deterministic payload identity policy rather than the SSE cursor.
6. Metrics and operator documentation distinguish resumed, replayed, skipped, and unknown-gap outcomes.
7. A review approves the source’s retention window and the product wording; “resume supported” must not mean “zero loss under every failure.”

Until then, the capability declaration should say `resumable: false` for the MVP even though the source protocol exposes an ID mechanism.

## Implementation constraints

- Implement only the `recentchange` stream.
- Use a descriptive User-Agent containing the project name, version, repository/contact URL, and a reachable contact where appropriate.
- Do not add API keys, OAuth, quotas, Redis, Kafka, leases, or an SSE client framework.
- Separate transport checks, SSE framing, JSON decoding, canary filtering, and event-envelope construction so each can be tested without the live source.
- Require the expected successful status and `text/event-stream` media type before parsing.
- Apply per-line and per-event/data size limits; never read the infinite response to memory.
- Propagate cancellation into the request, body read, and reconnect delay. The runtime separately owns cancellation-aware channel sends.
- Keep one collection run confined to one runtime goroutine; do not add a background parser or concurrent access to the same response state.
- Clone payload bytes if the parser or scanner may reuse their backing storage before the runtime handoff.
- Close the response body on every path.
- Use the reviewed deterministic payload identity policy for event identity. Preserve the SSE `id` only as non-durable cursor provenance; do not write a checkpoint in this lesson.
- Keep reconnect attempts and delays bounded and observable.
- Keep one active stream connection and one reconnect owner. Record attempts and total retry time; never launch a goroutine per reconnect.
- Place timeouts at named phases such as connection/header setup and total reconnect budget rather than imposing a short whole-stream lifetime.
- Do not silently skip malformed data; classify it and enforce the chosen policy.
- Do not claim zero loss, exact resume, exactly-once processing, or historical completeness.

Focused syntax hint policy: request a focused syntax hint only with an unrelated example such as parsing a bounded weather-alert stream. Never receive complete collector code.

## Common failure modes

| Failure | Why it happens | Correction |
|---|---|---|
| Whole-body read never returns | SSE response is intentionally long-lived | Parse incrementally from the body. |
| Stream dies after a fixed duration | A client-wide timeout includes body reading | Bound connection/header setup separately from the long-lived read; use context for shutdown. |
| Parser emits partial JSON | It emits on every line instead of on the blank-line boundary | Accumulate one complete SSE message first. |
| Multiline data is corrupted | Only the first `data` line is retained or joined incorrectly | Follow the SSE data-field assembly rule and test multiple lines. |
| `Scanner: token too long` is ignored | Default token limit is smaller than the chosen event bound | Set and test an explicit scanner limit or use a controlled reader. |
| Canary events reach the sink | Filtering occurs after callback or uses an untested field path | Filter before callback invocation and add a fixture test. |
| Parser starts a hidden goroutine | Collector lifetime escapes the runtime owner | Keep response parsing synchronous inside the collector call. |
| Callback payload aliases scanner storage | A later read mutates an earlier event | Clone or otherwise detach bytes before callback. |
| Collector closes and reconnects after every event | Per-event calls destroyed the SSE connection lifetime | Keep one synchronous collection run and body across callbacks. |
| Reconnect storm | No attempt budget, no cap, or no jitter | Bound retries and make backoff cancellation-aware. |
| Wrong content type is parsed as JSON | Status is checked but media type is not | Validate both status and parsed media type before body processing. |
| Response body leaks | Early returns omit `Close` | Make body ownership and cleanup visible in tests and review. |
| Reconnect loop accumulates deferred closes | Defers run only when the outer collection method returns | Scope one connection attempt or close explicitly before retry. |
| Cursor overclaim | In-memory `id` is described as durable resume | Mark MVP resumability false and use promotion gates. |
| Source policy violation | Generic or missing User-Agent, excess concurrency, or ignoring throttling | Follow current Wikimedia policy and keep one conservative connection. |

## Tests to write

All automated tests use a local fake HTTP server described in `08-local-fake-source-testing.md`.

- A valid `message` event with one data line becomes one valid event.
- Multiple data lines are assembled according to SSE rules.
- A comment/heartbeat produces no domain event.
- An event with no data is ignored.
- Unknown fields do not crash parsing.
- A canary payload is discarded before the collector invokes the event callback.
- Malformed JSON is classified and does not produce a valid event.
- An oversized line or assembled event is rejected within a bounded amount of work.
- Wrong status and wrong content type stop or reconnect according to the declared policy.
- The request contains the descriptive User-Agent and no credentials.
- Cancellation while waiting for headers, reading a body, or waiting on reconnect backoff returns within a bound; Stage 5 separately proves cancellation while sending to a full channel.
- Consecutive callback events remain byte-stable even if the parser reads more input.
- One collection run owns response/parser state, creates no unowned goroutine, and closes the body on every terminal/reconnect path.
- A callback failure stops reading, closes the body, and returns to the runtime.
- A remote disconnect consumes only the configured reconnect budget and does not loop forever.
- The MVP does not send `Last-Event-ID` or claim durable cursor recovery; test that request behavior explicitly.
- The identity fallback order is tested, while the SSE `id` remains non-durable cursor provenance.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
go test -race ./...
```

For an intentional manual smoke test, run the application with the source enabled and stop it after a short observation window. Do not make the public stream a required CI dependency.

## Acceptance criteria

The collector milestone is complete when:

- it connects to the public, credential-free `recentchange` SSE endpoint with a descriptive User-Agent;
- it validates status and content type before parsing;
- it parses bounded SSE frames and valid JSON without whole-body buffering;
- it drops canary events before invoking the runtime callback;
- it propagates cancellation through the request, parser, and backoff while runtime tests cover output sends;
- it transfers immutable payload ownership to the synchronous callback, retains one connection across events, and starts no hidden goroutine;
- reconnect behavior has a small explicit attempt/delay budget;
- tests run entirely offline against the fake source;
- deterministic event identity and SSE cursor provenance are separate and tested;
- the docs state no zero-loss or exact-resume guarantee;
- promotion gates for durable cursor recovery are written and testable;
- formatting, tests, and static checks pass.

## Reflection questions

1. Which part of the collector is source-specific and which part belongs in a reusable transport boundary?
2. Why is a public credential-free stream a better MVP learning source than a provider requiring an API key at this stage?
3. What information would an operator need to decide whether a reconnect ended with a gap?
4. If the source sends duplicates after reconnect, where should idempotency be enforced later?
5. Which promotion gate is most likely to fail when PostgreSQL is introduced, and why?
6. What source-policy claim did you verify rather than assume?

## Review submission template

Use the existing hint ladder and submit this at review:

```text
Milestone: Wikimedia EventStreams SSE collector
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of SSE framing, source policy, cancellation, reconnect limits, canary filtering, and cursor non-guarantees:
```

The mentor reviews source-policy compliance, response validation, framing correctness, size bounds, cancellation, reconnect budget, canary behavior, and the honesty of the cursor guarantee.
