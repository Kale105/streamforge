# 04 - Stdout sink and synchronous baseline

- Status: MVP laboratory guide
- Sequence: after the synchronous generator collector
- Boundary: one destination operation and a finite synchronous composition; no goroutine or channel yet

## Objective

Implement a sink that writes one complete event as one JSON line, then connect one generator collector to one stdout sink in a finite synchronous loop. This is the performance and correctness baseline that later concurrency experiments must beat without weakening ownership or error behavior.

## Prerequisites

- Complete Stages [01](01-event-json-round-trip.md), [02](02-collector-sink-interfaces.md), and [03](03-generator-collector.md).
- Read Gate 1 of the [Go concurrency track](go-concurrency-track.md#staged-learning-gates).
- Understand `io.Writer` ownership and the difference between production stdout and an injected test writer.
- Keep the scope narrow: no logging framework, broker, PostgreSQL, worker pool, or background output goroutine.

## Conceptual explanation

The sink handles exactly one event per synchronous call. It has two boundaries:

1. the event value supplied by its caller; and
2. the writer receiving encoded bytes.

Injecting `io.Writer` keeps the destination testable. Production wiring may provide standard output, while tests use a buffer or a predictably failing writer. The sink does not close a writer it did not create.

One `json.Encoder` associated with the sink/writer can encode each event. `Encoder.Encode` writes one JSON encoding followed by a newline, matching the desired line-oriented format. A write error returns to the caller immediately.

The finite baseline runs the collector with a synchronous callback that calls the sink in the same goroutine:

```text
collector wait/build -> callback -> sink handle -> callback returns -> collector continues
```

This establishes:

- strict call order;
- zero in-process queue;
- implicit backpressure because collection waits for the sink;
- direct error propagation; and
- no lifecycle or channel-close problem.

Record its event count and elapsed time under a controlled workload. Later stages add overlap and buffering one property at a time.

### Comparison for a C# or Java developer

| Concern | C#/Java instinct | Go interpretation here |
|---|---|---|
| Output dependency | `TextWriter`, `Stream`, `OutputStream` | `io.Writer` with a very small contract |
| JSON streaming | Serializer writing to a stream | One encoder writes one event and newline per call |
| Async output | Background logger or task | Not added; the caller decides whether to overlap calls later |
| Failure | Exception or faulted task | Return the writer/encoding error directly |
| Backpressure | Awaiting a write or bounded queue | The synchronous callback blocks further collection until writing completes |
| Closing | Dispose only what is owned | Do not close injected writer resources |

The synchronous version is not “less Go.” Direct calls are the correct default until independently blocking stages justify a concurrent pipeline.

## Targeted official resources

- [`encoding/json.NewEncoder`](https://pkg.go.dev/encoding/json#NewEncoder) and [`Encoder.Encode`](https://pkg.go.dev/encoding/json#Encoder.Encode) - writer and newline behavior.
- [`io.Writer`](https://pkg.go.dev/io#Writer) - short writes and returned errors.
- [`testing` package](https://pkg.go.dev/testing) - table tests, helpers, and cleanup.
- [Concurrency is not parallelism](https://go.dev/blog/waza-talk) - prepare to distinguish structural overlap from CPU parallel speedup.
- [Concurrency track: channel as a deliberate choice](go-concurrency-track.md#3-pipeline-stages-and-typed-channel-ownership).

## Design questions

1. Why inject `io.Writer` instead of hard-coding `os.Stdout`?
2. Who owns and closes the writer?
3. What ordering guarantee follows naturally from direct synchronous calls?
4. Where does pressure appear when writing is slower than collection?
5. Which elapsed-time and error observations will be the baseline for Stage 5?
6. What new failure paths will appear when collector and sink are separated by a goroutine/channel boundary?
7. Why would a hidden asynchronous writer weaken the experiment?

## Deliberately non-compilable pseudocode

```text
NOT RUNNABLE - PSEUDOCODE ONLY

sink handles one Event:
    encode exactly one complete Event through the owned encoder
    if writer fails: return the failure
    otherwise: return success

finite synchronous baseline:
    choose a small event count
    run finite collector with callback that:
        calls sink.handle(context, event)
        increments observed count only after success
        returns sink error to stop collection
    distinguish normal completion, cancellation, source failure, and sink callback failure
    record count, order, and elapsed time
```

Do not retain a general “pipeline framework” for this finite loop.

## Implementation constraints

- Implement the synchronous sink contract from Stage 2.
- Handle one `event.Event` per call; do not receive from a channel or start a goroutine.
- Accept an injected `io.Writer` and use one encoder associated with that writer.
- Emit exactly one complete JSON object and newline per successful call.
- Return encoding/writer errors; do not only log them.
- Do not close an injected writer.
- Keep the baseline finite and deterministic.
- Record baseline behavior without adding buffering, retries, batching, or metrics infrastructure.
- Use focused syntax hints only on an unrelated domain.

## Common failure modes

| Failure | Why it matters | Evidence to seek |
|---|---|---|
| Hard-coded stdout | Tests cannot inject failure or inspect bytes reliably | Constructor/dependency review. |
| Hidden background writer | Errors and lifetime escape the caller | Search for unowned goroutines. |
| Manual JSON/newline assembly | Framing and escaping become fragile | Parse every output line. |
| Writer error is swallowed | Caller continues collecting undeliverable events | Failing-writer test. |
| Writer is closed by sink | Borrowed resource ownership is violated | Close-tracking fake. |
| Baseline uses a channel | The learner cannot separate channel cost/behavior later | Direct-call composition review. |
| Timing test uses long sleeps | Results are slow and unstable | Controlled source/sink signals. |
| Baseline is removed after concurrency | Regression loses its simplest correctness oracle | Keep a finite synchronous integration test. |

## Tests to write

- One event produces exactly one valid JSON line.
- Several direct calls preserve call order and line count.
- Payload JSON remains valid inside each encoded event.
- A failing writer produces an immediate returned error.
- The sink does not close its injected writer.
- A finite synchronous collector-to-sink loop produces the expected count and strict sequence.
- Collector failure prevents a callback for a missing event.
- Sink callback failure stops any later collection event.
- The baseline contains no goroutine or channel.

Capture elapsed time only as local experiment evidence; avoid brittle absolute performance assertions in unit tests.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
```

Run the finite synchronous example/test before starting Stage 5 and record the result in the concurrency review submission.

## Acceptance criteria

- The stdout sink handles one event synchronously through an injected writer.
- Output contains one valid JSON value per line and no mixed diagnostics.
- Errors return directly and borrowed resources remain open.
- A finite one-collector/one-sink loop works entirely in one goroutine.
- The baseline documents event order, blocking behavior, and error order.
- No channel, `WaitGroup`, worker, or background goroutine is introduced.
- Formatting, tests, and vet pass.
- The learner can explain what concurrency must improve and which guarantees it must preserve.

## Reflection questions

- What does the direct-call baseline guarantee that fan-out may not?
- Which wait dominates elapsed time?
- If stdout blocks forever, who is blocked in this version?
- Which errors are simpler because no goroutine boundary exists?
- Why should this baseline remain after the concurrent runtime ships?

## Hint ladder and review submission

Use the shared [hint ladder](../mvp-learning-guide.md#hint-ladder). Review behavior and ownership before considering concurrency.

```text
Milestone: Stdout sink and synchronous baseline
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
Finite event count and observed order:
Baseline blocking and elapsed-time observations:
Why no goroutine/channel was needed yet:
Evidence read and links:
```
