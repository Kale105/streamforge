# 03 - Generator collector

- Status: MVP laboratory guide
- Sequence: after synchronous collector/sink contracts
- Boundary: one synchronous synthetic source loop; no goroutine, channel, network, or persistence yet

## Objective

Implement a deterministic collector that synchronously yields valid events at a configurable interval until finite completion, cancellation, or callback/source failure. Keep this stage synchronous: the caller invokes collection and remains blocked for its lifetime. The runtime will place that same operation in an owned goroutine later.

The generator separates event construction and source waiting from concurrency policy. This lets the learner prove timer cleanup, cancellation, payload ownership, and deterministic sequencing before scheduling and channels complicate the evidence.

## Prerequisites

- Complete [01 - Event JSON round-trip](01-event-json-round-trip.md) and [02 - Collector and sink interfaces](02-collector-sink-interfaces.md).
- Complete Gates 0 and 1 preparation in the [Go concurrency track](go-concurrency-track.md#staged-learning-gates).
- Understand the existing `encoding/json` and `json.RawMessage` contract.
- Keep the MVP boundary: no API keys, quotas, Kafka, Redis, leases, gRPC plugins, Kubernetes, OpenTofu, or custom infrastructure.

## Conceptual explanation

One collection run has a small synchronous state machine:

```text
validate configuration
wait for the next interval or cancellation
construct one immutable event
invoke the event callback synchronously
repeat until terminal, cancellation, callback error, or source error
```

The caller is blocked while the collector waits and while its callback handles each event. That is expected and gives direct backpressure. Blocking does not require a goroutine inside the collector; the later runtime owns the decision to overlap source and sink work.

The collector's sequence is useful for laboratory ordering assertions, not durable identity. A process restart can repeat an in-memory sequence. The real collector later derives stable identity from source data.

Timer lifecycle matters even in synchronous code. Waiting must observe context cancellation promptly, and resources created for the collection lifetime must have one cleanup path. Avoid hiding a background ticker goroutine or general clock framework. Use the smallest time seam that makes tests deterministic; Go 1.26's `testing/synctest` is available when the whole test fits its model.

After constructing `Event.Data`, treat its bytes as immutable. If construction used a reusable buffer, clone before invoking the callback. A later channel send will not deep-copy `json.RawMessage`.

### Comparison for a C# or Java developer

| Concern | C#/Java instinct | Go interpretation here |
|---|---|---|
| Blocking operation | Async method, scheduled task, or interruptible wait | An ordinary method can block while observing `context.Context` |
| Periodic timing | `PeriodicTimer` or scheduled executor | A timer/ticker drives the synchronous collector loop; its caller owns the run lifetime |
| Cancellation | `CancellationToken` or interrupt | Wait on the context signal at the blocking boundary |
| State | Mutable object field guarded by task ownership | One collector owner mutates the laboratory sequence synchronously |
| Async execution | Component starts its own task | The later runtime starts the goroutine and therefore owns its lifetime |

The important shift is that “can block” and “starts concurrency” are different properties.

## Targeted official resources

- [`time` package](https://pkg.go.dev/time) - timer/ticker lifecycle and positive durations.
- [`context` package](https://pkg.go.dev/context) - cancellation and deadlines at blocking boundaries.
- [`testing/synctest`](https://pkg.go.dev/testing/synctest) - fake time and completion/deadlock behavior in Go 1.26 tests.
- [`encoding/json`](https://pkg.go.dev/encoding/json) - build valid payload bytes without string concatenation.
- [Go memory model](https://go.dev/ref/mem) - prepare for later ownership transfer; returning or sending a slice does not make mutation safe.
- [Concurrency track: mutable payload transfer](go-concurrency-track.md#13-mutable-slices-maps-and-payload-transfer).

## Design questions

1. Why should collection run synchronously and invoke a callback rather than start a hidden goroutine?
2. Who owns the collection call and decides when it stops?
3. Which wait must observe cancellation?
4. How does the test avoid human-scale sleeps?
5. Which state is confined to the collector's single owner?
6. Why is the sequence useful for a lab but unsafe as durable identity?
7. Could callback payload bytes still alias a reusable buffer?
8. What must happen when the callback returns an error?
9. What will change—and what will not—when the runtime calls this operation in a goroutine?

## Deliberately non-compilable pseudocode

```text
NOT RUNNABLE - PSEUDOCODE ONLY

on synchronous collection run:
    reject invalid interval configuration
    create and own one repeating timer
    repeat:
        wait for either configured interval or caller cancellation
        advance the owner-confined sequence
        encode a small deterministic payload
        detach payload bytes if their backing storage may be reused
        construct one Event with synthetic source and type
        invoke event callback synchronously
        if callback fails: return that failure
```

The learner chooses the exact timer seam and error semantics. Do not paste this into a Go file.

## Implementation constraints

- Implement the synchronous collector contract from Stage 2.
- Yield events through the synchronous callback; do not start a goroutine or expose a channel.
- Use a configurable positive interval and a narrow test seam.
- Observe cancellation during the wait.
- Keep sequence mutation confined to the one collector owner; document that a collector instance is not concurrently called.
- Emit valid JSON using the existing v1 `encoding/json` contract.
- Clone or transfer exclusive ownership of payload bytes before callback invocation; never mutate a published event.
- Return callback errors immediately so synchronous sink failure stops collection.
- Do not retry, persist, make HTTP calls, add metrics, or create a generic scheduler in this lesson.
- Use the hint ladder; focused syntax examples must use an unrelated domain.

## Common failure modes

| Failure | Why it matters | Evidence to seek |
|---|---|---|
| Collector starts its own goroutine | Caller cannot join it or observe its error | Search for unowned `go` statements. |
| Uninterruptible sleep | Cancellation waits for the entire interval | Controlled cancellation test. |
| One ticker is created per loop iteration | Lifecycle becomes hard to own | Resource construction and cleanup review. |
| Collector is called concurrently | Sequence or timer state races | Ownership contract plus race test after runtime work. |
| Payload uses a reused backing buffer | Later event bytes change unexpectedly | Capture two callbacks and verify first bytes remain unchanged. |
| Callback error is ignored | Source continues after destination failure | Failing-callback test. |
| Test waits on production timing | Suite becomes slow and flaky | Fake time or small controlled interval. |
| Sequence is treated as identity | Restart repeats IDs | Reflection and real-source identity design. |
| Goroutine is added “because Go” | No independently blocking peer exists yet | Keep the synchronous baseline. |

## Tests to write

- A finite/test-controlled run invokes the callback within a bounded test guard.
- Consecutive callback events have increasing laboratory sequence values.
- Every `Event.Data` is valid JSON.
- Envelope fields use the reviewed synthetic source and type.
- Cancellation releases a waiting call promptly.
- A non-positive interval returns the documented configuration error rather than a late panic.
- The first captured event remains unchanged after the next event is generated, proving payload ownership.
- A callback failure stops collection and is returned unchanged or wrapped according to the documented policy.
- Running the finite collector synchronously requires no goroutine or channel in the test.

Use `testing/synctest` where it genuinely simplifies timer behavior; otherwise use an explicit narrow signal. Do not synchronize by sleeping and hoping the scheduler ran.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
```

The race detector becomes required once Stage 5 runs collector operations concurrently across distinct instances. This stage's ownership contract should already forbid concurrent collection calls on one instance.

## Acceptance criteria

- The generator implements the synchronous collector contract.
- One synchronous run yields complete, valid, immutable events through the callback.
- No application goroutine or channel is introduced.
- Cancellation releases the blocking wait.
- Sequence state has one owner and is not claimed as durable identity.
- Tests control time without relying on long sleeps.
- Formatting, tests, and vet pass.
- The learner can explain why a blocking method is not the same thing as an asynchronous component.

## Reflection questions

- Which behavior became easier to test because the collector stayed synchronous?
- Which state would race if two goroutines called the same instance?
- How will a runtime goroutine change throughput without changing this source contract?
- What exact ownership rule protects `json.RawMessage` during and after the callback?
- Which source waits would benefit from overlap with sink work?

## Hint ladder and review submission

Use the [MVP hint ladder](../mvp-learning-guide.md#hint-ladder). The reviewer should ask where every wait and mutable value is owned.

```text
Milestone: Generator collector
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of synchronous blocking and cancellation:
My sequence-state and payload-byte ownership rules:
Evidence read and links:
```
