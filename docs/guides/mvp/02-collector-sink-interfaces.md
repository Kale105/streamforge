# 02 - Collector and sink interfaces

- Status: MVP laboratory guide
- Sequence: after the synchronous event contract
- Boundary: synchronous behavior contracts; concurrency policy belongs to the runtime

## Objective

Define the smallest behavioral boundary between a source and a destination without choosing goroutines or channels yet. A collector runs synchronously and invokes a supplied synchronous event callback for each complete `event.Event`. A sink handles one complete event at a time. The caller decides whether collection and handling run in one goroutine or are connected by a concurrent pipeline.

This separation is intentional. A channel is useful when independently blocking stages need a bounded handoff, but putting a channel into every interface makes concurrency an implementation requirement before the learner has proved it is needed. Streamforge will add a typed channel in the runtime guide after the synchronous baseline works.

## Prerequisites

- Complete [01 - Event JSON round-trip](01-event-json-round-trip.md) and have its contract reviewed.
- Read the opening and Gate 0 sections of the [Go concurrency track](go-concurrency-track.md).
- Read sections 2 and 6 of [the MVP learning guide](../mvp-learning-guide.md).
- Be comfortable reading a Go function signature and returning an error.
- Keep the MVP exclusions in view: no Kafka, gRPC plugins, leases, CEL, Redis, React, Kubernetes, OpenTofu, API keys, quotas, or custom IaC.

## Conceptual explanation

The collector owns source-side behavior. It knows how to wait for or construct the next observation, but it does not know whether the caller will write it to stdout, place it on a channel, or persist it. The sink owns one destination-side effect and does not know which collector produced the event.

The contracts should express these synchronous operations:

- run collection under a context and synchronously yield complete events;
- give a sink one complete event under a context;
- return errors rather than selecting process policy; and
- document how a finite collector reports normal exhaustion.

The runtime introduced later owns looping, goroutines, channels, buffering, fan-in, cancellation of siblings, channel closure, and waiting. This establishes an important production pattern: concurrency is orchestration policy around simple operations, not an infectious property of every package interface.

The first learning composition is deliberately synchronous:

```text
caller runs one finite collector
collector waits for/builds one Event
collector calls the sink-shaped callback synchronously
sink handles that Event and returns
collector continues until finite completion, cancellation, or callback/source error
```

When the sink is slow, the collector callback remains blocked and the next source event is not requested/read. That baseline has implicit backpressure, deterministic ordering, and simple error propagation. Later measurements will show which independent waits justify overlap.

### Comparison for a C# or Java developer

| Concern | C#/Java instinct | Go interpretation here |
|---|---|---|
| Interface implementation | A class declares `implements` or inherits an interface | A type satisfies a small interface implicitly through its method set |
| Source stream | Async iterator, observer callback, or blocking read loop | One context-aware method runs synchronously and yields each complete event through a callback |
| One destination effect | `WriteAsync`, consumer method, or handler | One context-aware method handles one event and returns an error |
| Cancellation | `CancellationToken` or interrupt/future policy | `context.Context` is passed explicitly to the blocking operation |
| Concurrency | Often embedded in `Task`-returning interfaces | The Go runtime caller may keep the same operation synchronous or run loops in goroutines |
| Failure | Exception propagation or failed future | Return an error; the owner chooses retry, cancellation, drain, or exit |

An operation can block without being asynchronous. Go makes it inexpensive to place that operation in a goroutine later, but doing so creates lifetime obligations that belong to the caller.

## Targeted official resources

- [Go specification: interface types](https://go.dev/ref/spec#Interface_types) - interface method sets and implicit implementation.
- [`context` package](https://pkg.go.dev/context) - explicit context propagation and the rule against storing context in a struct.
- [Go FAQ: goroutines instead of threads](https://go.dev/doc/faq#goroutines) - why the runtime can later overlap blocking operations efficiently.
- [Go concurrency track: pipeline stages](go-concurrency-track.md#3-pipeline-stages-and-typed-channel-ownership) - why the runtime, not these contracts, owns the channel.
- [MVP hint ladder](../mvp-learning-guide.md#hint-ladder) - review protocol rather than a source of completed code.

## Design questions

1. Why should the collector not write directly to PostgreSQL?
2. Why should the sink not import the collector package?
3. What becomes easier to test when each method describes one synchronous operation?
4. Who owns the collector's source loop, and who owns the goroutine that may run it later?
5. How will a finite fake collector distinguish normal exhaustion from failure?
6. Which source, callback, and sink operations can block, and why must cancellation reach each boundary?
7. What measured or structural reason will justify placing the two loops in separate goroutines?
8. Why is a channel appropriate for the later pipeline but not required in either component contract?

## Deliberately non-compilable pseudocode

This is contract notation, not Go syntax. The learner chooses exact names and terminal semantics.

```text
interface Collector:
    collect(context, synchronous Event callback) returns terminal/error result

interface Sink:
    handle(context, one complete Event) returns error

synchronous caller:
    run finite collector with callback that:
        calls sink.handle(context, event)
        returns sink error to collector/caller
    distinguish normal source completion, cancellation, source error, and callback error
```

Do not add a channel, worker count, retry policy, start/stop pair, or hidden lifecycle method to these interfaces. The synchronous event callback is the collector's narrow delivery boundary; it must not launch work or retain the event after returning.

## Implementation constraints

- Create one small interface in the collector package and one in the sink package.
- Each interface has one method and no framework-specific type. Keep any named event-callback type narrow and project-owned.
- Use the repository's `event.Event`, not `any`, a provider DTO, or a database row.
- Put `context.Context` first; do not store it on a long-lived component.
- Keep channel, goroutine, `WaitGroup`, worker, retry, and shutdown coordination out of the interfaces.
- Do not add `Start`, `Stop`, `Close`, `Health`, `Metrics`, `Manager`, `Service`, `BaseCollector`, or a generic pipeline abstraction.
- Document the normal terminal result for a finite collector and callback failure propagation; do not confuse either with cancellation or source failure.
- Assume a collector instance has one runtime owner unless the concrete implementation explicitly proves concurrent safety.
- The learner writes the code. Focused syntax hints must use an unrelated domain.

## Common failure modes

| Failure | Why it is a problem | Correction to investigate |
|---|---|---|
| Channel appears in every interface by default | Execution policy leaks into packages and complicates synchronous tests | Keep per-event contracts synchronous; let runtime own the pipeline. |
| Method starts a hidden goroutine | Caller cannot join it or observe its error | Keep lifetime creation in the owner. |
| Collector returns a provider DTO | Source schema leaks into downstream contracts | Return the canonical event envelope. |
| Sink accepts a concrete collector | Source and destination become coupled | Depend only on the event and sink behavior. |
| Context is stored on the component | One run's lifetime can leak into another | Pass context per operation. |
| Finite exhaustion, callback failure, and source failure are ambiguous | Runtime may retry completed work or hide sink failure | Define testable terminal and error propagation. |
| Callback starts a goroutine or retains mutable event bytes | Collector loses backpressure and payload ownership | Callback completes synchronously and treats the event as immutable. |
| Interface grows speculative methods | Every fake pays for unused future behavior | Keep one current operation per role. |
| Interface promises concurrent safety accidentally | Multiple goroutines may mutate parser or writer state | Document single-owner use and add parallelism only around proven-safe instances. |

## Tests to write

- Compile-time checks show the generator satisfies the collector contract once Stage 3 exists.
- Compile-time checks show the stdout implementation satisfies the sink contract once Stage 4 exists.
- A tiny finite fake collector can be called synchronously and yields events in known order.
- A tiny recording sink receives synchronous calls in exactly that order.
- Collector failure stops the finite synchronous loop before invoking the callback with a missing event.
- Sink failure stops the loop before requesting more source work.
- Cancellation reaches a blocking fake operation and produces the documented result.
- A normal finite terminal result is distinguishable from failure.

Do not create a mock framework, concurrent coordinator, or channel for this milestone.

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
```

The race detector begins after goroutines are introduced. Running it early is harmless, but it does not replace the synchronous contract tests.

## Acceptance criteria

- Collector and sink each expose exactly one synchronous, context-aware operation.
- The collector's event callback is synchronous, propagates handler failure, and does not own a goroutine.
- Neither interface starts work, owns a goroutine, or exposes a channel.
- The caller owns iteration and failure policy.
- Normal finite exhaustion is documented separately from cancellation and failure.
- Neither interface mentions Kafka, HTTP, SQL, stdout, or provider-specific types.
- A finite one-collector/one-sink synchronous test plan is ready for Stages 3 and 4.
- Formatting, tests, and vet pass.
- The learner can explain why Streamforge will use a channel in its runtime without making channels the default interface shape.

## Reflection questions

- What does the synchronous baseline make easier to reason about?
- Which independently blocking operations will benefit when the runtime adds goroutines?
- Would a mutex, direct call, or channel best protect a shared configuration snapshot? Why?
- What extra obligations appear the instant a caller starts a goroutine?
- How could a channel-oriented interface make multiple-producer closure harder to own?

## Hint ladder and review submission

Use the shared [hint ladder](../mvp-learning-guide.md#hint-ladder). Review the plain-English contract and ownership before method naming preferences.

```text
Milestone: Collector and sink interfaces
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My plain-English reading of each method and callback:
How finite completion differs from failure:
Why concurrency stays outside these interfaces:
Evidence read and links:
```
