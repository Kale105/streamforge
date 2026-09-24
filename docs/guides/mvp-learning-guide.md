# Streamforge MVP - Learning-First Build Guide

- Status: Active learning plan
- Updated: 2026-08-06
- Target: A small but reliable source-to-API vertical slice
- Teaching rule: The learner writes the implementation

## Detailed curriculum

Use the [modular lean-MVP curriculum](mvp/README.md) for the complete learning order, stage gates, and detailed briefs from event testing through the release checklist. This file remains the orientation, learning agreement, and high-level milestone map. The modular guides are authoritative for stage acceptance criteria and deliberately keep post-MVP architecture out of the implementation path.

## 1. Purpose

This guide is designed to teach Go, backend development, and system-design reasoning while producing a working MVP.

The MVP is:

```text
Real data source
    -> Go collector
    -> normalization
    -> PostgreSQL
    -> read-only HTTP API
```

The first laboratory begins synchronously, then adds one concurrent boundary:

```text
Event JSON tests in one goroutine
    -> one collector call
    -> one sink call

then, after the baseline passes:

collector task goroutine
    -> runtime-owned bounded Go channel
    -> sink loop goroutine
```

The laboratory milestone is not throwaway work. It establishes synchronous component contracts first, then the ownership, cancellation, channel, backpressure, and lifecycle rules that later components will reuse. Follow the required [Go concurrency track](mvp/go-concurrency-track.md) alongside the modular stages.

## 2. Learning agreement

### What the learner does

- Writes every implementation file.
- Chooses names and explains important choices.
- Runs formatting, tests, static analysis, and the application.
- Shares code or errors at each review checkpoint.
- Answers the explanation questions before moving forward.
- Keeps a short engineering journal containing surprises, failures, and decisions.

### What the mentor does

- Explains the purpose of each component before implementation.
- Defines behavior and acceptance criteria.
- Reviews submitted code and identifies correctness or design problems.
- Uses a hint ladder rather than immediately supplying a solution.
- Explains compiler messages, runtime failures, and test results.
- Supplies a small syntax example only when the missing syntax is blocking the lesson.
- Does not implement the project unless explicitly asked to switch out of learning mode.

### Hint ladder

When blocked, ask for the smallest useful level:

1. **Concept hint** - explains the idea without naming the Go construct.
2. **Direction hint** - names the relevant package, function, or language feature.
3. **Pseudocode hint** - shows control flow without compilable Go.
4. **Focused syntax hint** - demonstrates only the unfamiliar syntax on an unrelated example.
5. **Rescue explanation** - walks through the exact problem after the learner has attempted it.

Do not jump directly to level 5. Struggling briefly, forming a hypothesis, and testing it are part of learning.

## 3. Scope control

### Included in the MVP

- One synthetic collector
- One permitted real HTTP or SSE source
- A small CloudEvents-inspired event envelope
- One source-specific normalization function
- PostgreSQL raw-event storage
- PostgreSQL normalized-record storage
- Duplicate-safe writes
- One read-only records endpoint
- Bounded query results
- Graceful shutdown
- Docker Compose for PostgreSQL and the application
- Unit and integration tests
- Structured logs and basic health reporting

### Excluded until after the MVP

- Kafka
- Multiple services
- External plugins or gRPC
- Collector leases and fencing
- CEL or a custom expression language
- A general schema engine
- Redis
- React
- API keys, quotas, and billing
- ClickHouse or object storage
- Kubernetes, Helm, or OpenTofu
- A custom IaC engine

These exclusions are sequencing decisions, not rejections of the long-term design.

## 4. Development setup

### Step 4.1 - Verify Go

Open a new PowerShell terminal and run:

```powershell
go version
where.exe go
```

Expected facts:

- Go reports version 1.26.5 or a newer compatible version.
- Windows locates `go.exe` under `C:\Program Files\Go\bin`.

If the current terminal has a stale `PATH`, refresh it temporarily:

```powershell
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
$env:Path = "$machinePath;$userPath"
```

Why this matters: Windows processes receive a copy of their environment when they start. A terminal opened before Go was installed does not automatically receive the updated system `PATH`.

### Step 4.2 - Create the initial directories

From the repository root:

```powershell
New-Item -ItemType Directory -Force -Path `
  cmd\app, `
  internal\event, `
  internal\collector, `
  internal\sink
```

Purpose of each directory:

| Directory | Responsibility |
|---|---|
| `cmd/app` | Constructs dependencies, starts the process, and coordinates shutdown |
| `internal/event` | Defines the transport-independent event contract |
| `internal/collector` | Produces events from sources |
| `internal/sink` | Consumes events and delivers them somewhere |

`internal` has special meaning in Go: code outside this project cannot import these packages. That prevents an unfinished implementation from accidentally becoming a public SDK.

### Step 4.3 - Initialize the module

```powershell
go mod init github.com/YOUR_USERNAME/streamforge
```

Replace `YOUR_USERNAME` with the GitHub account that will own the repository.

The module path becomes the root of internal imports. It does not require the GitHub repository to exist yet.

Review checkpoint:

- Show the output of `go version`.
- Show `go.mod`.
- Show the directory tree.
- Explain the difference between `cmd` and `internal`.

Do not continue until the module and directory layout are correct.

### Setup research resources

Use these official resources to verify the setup concepts rather than relying only on this guide:

- [Organizing a Go module](https://go.dev/doc/modules/layout) - read **Basic command**, **Package or command with supporting packages**, and **Packages and commands in the same repository**. Look for the distinction between the module root, `cmd`, `package main`, and `internal`.
- [Go modules documentation](https://go.dev/doc/modules/) - use this as the index when a `go.mod`, module-path, dependency, or package-import question appears.

Reading task:

1. Locate the sentence explaining why supporting packages may be placed under `internal`.
2. Locate the example that places multiple executable programs under `cmd`.
3. Compare that example with this repository and write down what is convention versus what the Go toolchain enforces.

## 5. Milestone 1 - Define the event contract

### Objective

Create an `Event` type in the event package. Do not create collectors or sinks yet.

### Required fields

| Field | Go representation | Meaning |
|---|---|---|
| Specification version | String | Version of the outer envelope; initially `1.0` |
| ID | String | Identity of one source event |
| Source | String | Stable identifier for the producing source |
| Type | String | Describes what kind of event occurred |
| Time | Time value | When the event occurred or was produced |
| Data | Raw JSON bytes | Source-specific payload |

Use JSON names compatible with the CloudEvents core names: `specversion`, `id`, `source`, `type`, `time`, and `data`.

### Concepts to understand first

- Exported Go fields begin with an uppercase letter.
- Struct tags control JSON field names.
- `time.Time` has standard JSON timestamp behavior.
- Raw JSON allows the outer pipeline to carry a payload without interpreting it.
- Source-event identity and normalized-record identity are not the same concept.

### Research resources for the design questions

You do not need to read every line of every specification. Follow the targeted reading path below and take notes before answering the questions.

#### Resource A - CloudEvents motivation and roles

- [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
- Read **Overview** and the definitions of **Event**, **Producer**, **Source**, and **Consumer**.

Look for:

- Why a vendor-neutral event format exists
- The distinction between event context and event data
- Whether an event identifies a particular destination
- How a generic intermediary can carry an event without understanding the domain payload

Notebook exercise:

```text
Draw: provider -> collector/adapter -> event transport -> sink

Under each arrow, write which data shape crosses the boundary.
Then imagine that the provider renames one field.
Mark which components should need to change.
```

Use this exercise to investigate design question 1. Do not begin by searching for a memorized definition of “decoupling”; reason from which components would change.

#### Resource B - Go JSON representations

- [`encoding/json` package documentation](https://pkg.go.dev/encoding/json)
- [JSON and Go](https://go.dev/blog/json)
- [Standard-library `RawMessage` example](https://go.dev/src/encoding/json/example_test.go)

Within the package documentation, search for:

```text
RawMessage
delay JSON decoding
Unmarshal
interface
Valid
```

Within **JSON and Go**, read:

- **Decoding**
- **Generic JSON with interface**
- **Decoding arbitrary data**

Create a comparison table in the journal:

| Question | Raw JSON value | `map[string]any` |
|---|---|---|
| Has the payload already been interpreted into Go values? | Research | Research |
| Can it be forwarded without inspecting individual fields? | Research | Research |
| What Go type is normally used for a JSON number after generic decoding? | Research | Research |
| When is each representation more convenient? | Research | Research |

Use the evidence in that table to answer design question 2. The goal is not to prove that one representation is universally better; it is to choose the better representation for a transport layer that should not interpret every source payload.

#### Resource C - Event identity

- In the [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md), read the required `id` and `source` attribute sections.
- In the [CloudEvents Primer](https://github.com/cloudevents/spec/blob/main/cloudevents/primer.md), search for **CloudEvents Core Attributes** and then read the `id` discussion.
- Optional deeper reference: [RFC 9562: Universally Unique IDentifiers](https://www.rfc-editor.org/info/rfc9562).

Restart thought experiment:

```text
Run A starts its in-memory counter and emits IDs 1, 2, 3.
The process stops.
Run B starts a fresh in-memory counter.

Write the first three IDs produced by Run B.
Now reason about what a consumer sees if the source identifier did not change.
```

Use the specification and thought experiment to answer questions 3 and 4. UUIDs are one possible tool, but the important lesson is the identity contract, not memorizing one ID library.

#### Resource D - Envelope and payload versioning

- In the [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md), compare the `specversion` and `dataschema` attribute descriptions.
- In the [CloudEvents Primer](https://github.com/cloudevents/spec/blob/main/cloudevents/primer.md), read the versioning discussion and **The role of the `dataschema` attribute within versioning**.

Two-change exercise:

```text
Change A: the rules for interpreting the outer event metadata change.
Change B: a sports payload changes score from a number to a string.

For each change, decide which version identifier would need attention and why.
```

Use this exercise to answer design question 5.

### Research method

For each design question, record:

```text
My current answer:
Evidence or heading I found:
What the source actually claims:
My reasoning from that evidence:
What remains uncertain:
```

Paraphrase rather than copying specification sentences. Being able to reconstruct the reasoning is more useful than remembering the wording.

### Design questions

Answer these in the engineering journal before writing the type:

1. Why should a collector not send a provider-specific structure directly to every sink?
2. Why is a raw JSON payload preferable to a generic `map[string]any` in the transport layer?
3. Is the sequence `1` a safe production event ID after a restart? Why or why not?
4. Which pair of fields should identify a source event uniquely?
5. What is the difference between an event-envelope version and a payload-schema version?

### Implementation task

Create `internal/event/event.go` containing:

- The event package declaration
- Imports required by the chosen field types
- The event structure
- JSON tags for every field

Do not add validation, constructors, UUID libraries, or schema registries yet.

### Tests to write

Create a table-driven JSON round-trip test that verifies:

- All required JSON field names appear.
- A payload remains valid JSON after encoding and decoding.
- The timestamp survives a round trip.
- No unexpected exported field appears.

### Acceptance commands

```powershell
go fmt ./...
go test ./...
go vet ./...
```

Review checkpoint:

- Share the event file and its test.
- Explain every field and import.
- Explain why the payload is not strongly typed at this layer.

## 6. Milestone 2 - Define behavioral boundaries

### Objective

Describe what a collector and sink do without implementing either one.

### Collector behavior

A collector:

- Accepts application cancellation.
- Synchronously invokes the supplied callback for each complete event.
- Returns an error if it cannot continue.
- Knows about its source.
- Does not know about stdout, PostgreSQL, Kafka, or the public API.

### Sink behavior

A sink:

- Accepts application cancellation.
- Handles one complete event per synchronous call.
- Returns an error if it cannot continue.
- Knows about its destination.
- Does not know which collector produced an event.

### Concepts to understand first

- An interface describes behavior rather than storage.
- A blocking method does not need to start its own goroutine.
- The caller owns loops and decides when concurrency is justified.
- Context cancellation must be observed at blocking boundaries.
- Returning an error is different from logging an error.

### Design questions

1. Why should the collector not write directly to PostgreSQL?
2. Why should the stdout sink not import the collector package?
3. Who should decide whether a returned error stops the entire application?
4. Why does a context belong in a long-running method?
5. Why should neither component interface own a runtime channel?
6. How will a finite collector report normal exhaustion separately from failure?

### Implementation task

Create one file in the collector package and one in the sink package. Each file defines the smallest interface that expresses the behavior above.

Constraints:

- Each interface has one method.
- Each method describes one synchronous operation; do not expose a channel or start hidden work.
- Neither interface contains Kafka-, HTTP-, or database-specific types.
- Do not create a generic `Manager`, `Service`, or `BaseCollector` abstraction.

Review checkpoint:

- Share both interfaces.
- Read each method signature aloud in plain English.
- Explain why runtime concurrency policy stays outside the interfaces.

## 7. Milestone 3 - Implement a generator collector

### Objective

Synchronously yield synthetic events at a configurable interval. Keep the collection run synchronous; the runtime owns its eventual goroutine.

### Required behavior

- Wait on a controlled timer or time seam.
- Release timer resources when collection ends.
- Maintain an incrementing sequence for the payload.
- Construct valid JSON for the payload.
- Yield complete immutable events through the synchronous callback.
- Stop promptly when the context is cancelled.
- Do not start a hidden goroutine or expose a channel.

### Pseudocode

```text
validate interval
create and own repeating timer
repeat:
    wait for either timer or cancellation
    on timer:
        increment owner-confined sequence
        encode payload
        detach reusable bytes if necessary
        invoke synchronous event callback
        return callback failure if present
    on cancellation:
        return expected shutdown result
```

### Design questions

1. Why use a ticker instead of sleeping at the bottom of an infinite loop?
2. Why can a blocking operation remain synchronous?
3. Who owns the collection call and eventual goroutine lifetime?
4. What resource does stopping the timer release?
5. Why is an incrementing ID acceptable for this test but not for durable collection?
6. How do you prevent callback `json.RawMessage` bytes from aliasing a reused buffer?

### Tests to write

- The collector invokes the callback with at least one event.
- The sequence increases.
- The payload is valid JSON.
- Cancelling the context causes the collector to return within a bounded time.
- A captured payload remains unchanged after the next callback.
- A callback failure stops collection and propagates to the caller.
- The collector starts no goroutine and uses no channel.

Do not make a unit test wait for real one-second intervals. Choose an injectable interval or another small test seam rather than making the suite slow.

Review checkpoint:

- Share the generator and its tests.
- Identify every operation that could block.
- Explain how each blocked operation can be released.

## 8. Milestone 4 - Implement the stdout sink

### Objective

Handle one complete event and write one JSON object per terminal line, then run a finite collector-to-sink loop synchronously.

### Required behavior

- Create one JSON encoder for standard output.
- Handle one event per synchronous call.
- Encode exactly one complete event per line.
- Return encoding errors to the caller.
- Keep a finite direct-call baseline with no goroutine or channel.

### Design questions

1. Why create one encoder instead of repeatedly converting JSON bytes to strings?
2. Why is newline-delimited JSON useful for streams?
3. Where does backpressure appear in the direct-call version?
4. Which order does the synchronous loop guarantee?
5. What new obligations will appear when the runtime adds a channel?

### Tests to write

To make the sink testable, consider whether its output destination should be an injected writer rather than permanently fixed to the process terminal.

Verify:

- One event produces one valid JSON line.
- Several direct calls preserve call order.
- A writer failure is returned rather than silently logged.
- A finite one-collector/one-sink loop runs in one goroutine.

Review checkpoint:

- Share the sink and its tests.
- Explain dependency injection using the writer as the example.

## 9. Milestone 5 - Wire the application

### Objective

Wrap the synchronous collector and sink operations in owned loops, connect them with a typed bounded channel, and shut them down predictably.

### Required behavior

- Create a root context that responds to `Ctrl+C`.
- First observe an unbuffered rendezvous, then create a small measured bounded event channel.
- Construct the generator and stdout sink.
- Run one synchronous collector task and one sink loop concurrently.
- Run a finite two-collector fan-in exercise, then return production to one active collector.
- Capture errors from both components.
- Cancel the other component when one fails unexpectedly.
- Track and wait for every application-created goroutine with `sync.WaitGroup`.
- Treat an expected cancellation differently from an unexpected failure.

### Ownership model

For the first single-collector version:

```text
main owns application lifecycle
runtime collector task owns the collection call and its channel-sending callback
runtime sink loop owns receives and destination calls
producer-group owner closes after every producer finishes
main/runtime joins every owned goroutine
```

No individual collector closes the channel. This rule works for one producer and remains correct for the finite two-producer fan-in exercise.

### Shutdown sequence

```text
Ctrl+C
    -> cancel collection
    -> collector task stops producing
    -> producer-group owner waits and closes channel
    -> sink drains buffered events
    -> sink sees channel closure
    -> coordinator observes both completions
    -> process exits
```

### Design questions

1. Why does starting a goroutine not automatically make it supervised?
2. What happens if `main` returns while goroutines are still running?
3. Why should the error channel be buffered?
4. What happens if the sink fails while the collector is blocked on a send?
5. Why might immediate context cancellation conflict with draining buffered events?
6. What does `WaitGroup` prove, and why does it not replace error propagation?
7. Which order survives the two-collector fan-in experiment?
8. Why does sending an `Event` not deep-copy its `json.RawMessage` bytes?

### Manual acceptance test

```powershell
go run ./cmd/app
```

Verify:

- One valid event appears at each interval.
- IDs and sequences increase.
- Every line is valid JSON.
- `Ctrl+C` stops production.
- Buffered accepted events are drained.
- The program exits without a panic or forced terminal close.

Review checkpoint:

- Share `main.go`.
- Draw the goroutines and channel.
- Name the owner, stop condition, result path, and join path for every goroutine.
- Narrate the shutdown sequence from the signal to process exit.

## 10. Milestone 6 - Backpressure experiment

### Objective

Observe the difference between buffering and sustainable throughput.

### Experiment

Add a configurable delay to the test sink and run the same producer with channel capacities:

```text
0
1
10
100
10,000
```

For each run, record:

- Produced events
- Consumed events
- Approximate runtime
- Maximum observed queue depth if instrumented
- Memory behavior
- Time required to shut down

Then complete the required concurrency fault laboratories from the [Go concurrency track](mvp/go-concurrency-track.md#required-intentional-failure-experiments):

- deliberately block an unbuffered and full bounded send, then release/cancel it;
- deliberately introduce a shared counter/map race, observe it with `go test -race`, then fix it through confinement or explicit synchronization;
- deliberately create a blocked-send goroutine leak, prove the owner cannot join it, then add the stop/join path;
- fan in two finite collectors with producer-group-owned channel closure; and
- compare one sink worker with a small bounded worker pool, observe reordering, and keep the production sink sequential unless promotion evidence exists.

Broken variants do not remain in the reviewed tree. Preserve the observation and corrected invariant in the experiment note.

### Questions

1. Does a larger buffer increase the sink's long-term processing rate?
2. What does it change instead?
3. What happens after the buffer fills?
4. Why would an unbounded queue be dangerous?
5. Which behavior would be preferable for a replayable source versus a non-replayable live stream?
6. Which goroutine owns each task, and how is each joined?
7. Which ordering guarantee is lost by fan-out?
8. Why must both worker count and queued work be bounded?

### Deliverable

Write a short experiment note under `docs/experiments/` containing the setup, results, and conclusion. Do not claim scalability from a single measurement.

## 11. Two-week MVP sequence

Move forward only after the first six milestones are understood.

### Days 1-2 - Runtime laboratory

- Complete Milestones 1-5.
- Complete the backpressure, blocking, race, leak, fan-in, and bounded-fan-out experiments before the real source.
- Keep the production concurrency budget to one collector task, one sink loop, and one measured bounded channel.

### Days 3-5 - Real source

- Use Wikimedia EventStreams `recentchange` SSE as the one permitted real source for this curriculum.
- Its public, credential-free, long-lived HTTP stream teaches framing, cancellation, reconnect classification, and backpressure without making the MVP depend on an API key or unresolved sports-feed redistribution permission.
- Do not also implement HTTP polling in the MVP.
- Test against a local fake HTTP server rather than the live internet.
- Add timeouts, response-size limits, cancellation, and clear error classification.
- Preserve a source identifier and cursor when the source provides them.

Learning topics:

- HTTP request lifecycle
- Timeouts versus retries
- Rate limiting
- Source identity
- Test doubles

### Days 6-8 - PostgreSQL

- Add PostgreSQL through Docker Compose.
- Create reviewed SQL migrations.
- Store raw events.
- Normalize one source payload into one record shape.
- Store the normalized record in the same transaction.
- Add unique constraints that make reprocessing safe.

Learning topics:

- Transactions
- Primary and unique keys
- Idempotency
- Connection pools
- Migrations

### Days 9-10 - Read API

- Create a health endpoint.
- Create one endpoint that lists normalized records.
- Enforce a maximum result count.
- Add simple stable pagination only if the base endpoint is correct.
- Keep provider-specific structures out of the public response contract.

Learning topics:

- HTTP handlers
- Status codes
- JSON response contracts
- Query parameter validation
- Pagination tradeoffs

### Days 11-12 - Reliability

Test:

- Duplicate source events
- Malformed JSON
- HTTP timeout
- HTTP 429 and 500
- Database unavailable
- Restart after previously stored data
- Cancellation during an active request

Write down the observed guarantee. Do not claim exactly-once behavior.

### Days 13-14 - Packaging

- Make a fresh-clone startup path.
- Document environment variables without committing secrets.
- Add Compose health checks.
- Run all tests from a clean environment.
- Write known limitations.
- Record a short demonstration.

## 12. MVP definition of done

The MVP is complete when all statements below are demonstrated:

- A permitted real source produces data continuously or on a schedule.
- Raw source events are retained in PostgreSQL.
- One normalizer produces a stable public record shape.
- Reprocessing the same source event does not duplicate logical state.
- A read-only HTTP endpoint returns normalized records.
- Results are bounded.
- Source and database failures are visible and do not cause silent corruption.
- Restart behavior is documented and tested.
- Docker Compose starts the supported local stack.
- A fresh user can follow the README without undocumented manual steps.
- Tests run without depending on the public source.
- Limitations are stated honestly.

## 13. Commands used throughout

Format the repository:

```powershell
go fmt ./...
```

Run all tests:

```powershell
go test ./...
```

Run static checks:

```powershell
go vet ./...
```

Run the application:

```powershell
go run ./cmd/app
```

Inspect module information:

```powershell
go env GOMOD
go list ./...
```

Later, after the basic suite is stable, use the race detector on a supported Windows C toolchain:

```powershell
go test -race ./...
```

## 14. Review template

At every checkpoint, submit:

```text
Milestone:
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of how it works:
```

The mentor reviews correctness, clarity, ownership, blocking behavior, error handling, test quality, and unnecessary abstraction before approving the next milestone.

## 15. First assignment

Complete only Development Setup and Milestone 1.

Deliver:

1. Output from `go version`.
2. Contents of `go.mod`.
3. The directory tree.
4. The event type written by you.
5. Its JSON round-trip test.
6. Your answers to the five Milestone 1 design questions.

Do not implement the generator, sink, database, or API yet. The next component begins only after the event contract is reviewed.
