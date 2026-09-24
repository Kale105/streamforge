# Streamforge MVP - Backpressure Experiment

- Milestone: runtime laboratory experiment
- Scope: measure bounded-channel behavior and intentionally diagnose one block, race, leak, and bounded fan-out design
- Teaching rule: measurements must support claims; a single run does not prove scalability

## Prerequisites

Complete [05 - Runtime orchestration](05-runtime-orchestration.md) and the earlier generator/sink milestones in the [MVP learning guide](../mvp-learning-guide.md). You should have:

- a supervised producer and consumer;
- a bounded channel whose capacity can be configured;
- cancellation-aware sends and receives;
- a test sink whose processing delay can be controlled;
- counters for produced and consumed events.

Also complete Gates 2-7 in the [Go concurrency track](go-concurrency-track.md#staged-learning-gates). Keep the synchronous Stage 4 result as the baseline and use the track's intentional-failure rules: broken variants belong only in a temporary patch/branch or journal, never in the reviewed tree.

Keep the experiment in memory. Kafka, Redis, PostgreSQL, batching frameworks, unbounded queues, and load-test products are outside this lesson. A small bounded worker-pool comparison is permitted only as a lab; it does not enter production without the promotion gate.

## Conceptual explanation

Backpressure is the signal that downstream work cannot keep up with upstream production. A bounded channel makes that condition visible: the producer eventually waits, slows down, disconnects, or applies a deliberately chosen drop policy. An unbounded queue hides the condition until memory or another resource is exhausted.

Use these terms precisely:

- **Arrival rate**: how quickly the producer is ready to emit events.
- **Service rate**: how quickly the sink can process them.
- **Burst absorption**: temporary work a finite buffer can hold while the sink catches up.
- **Sustainable throughput**: the long-run rate the sink can complete; a larger buffer does not make the sink faster.
- **Queue depth**: currently buffered events, not total events ever produced.
- **Backlog age**: how long the oldest accepted event waits; this is often more useful than depth alone.

When arrival is faster than service for long enough, every finite buffer fills. The buffer changes when the producer feels pressure and how much memory is temporarily occupied. It does not change the sink’s underlying processing rate.

The source capability matters. A replayable source can often pause and resume later. A live non-replayable source may have to block, disconnect, or explicitly accept loss. The MVP Wikimedia stream is treated as a best-effort live stream until storage and cursor recovery are promoted; this experiment must not turn that into a zero-loss promise.

### C#/Java comparison

| Streamforge idea | C# analogy | Java analogy | Go lesson |
|---|---|---|---|
| `make(channel, capacity)` | bounded `Channel<T>` options | `ArrayBlockingQueue` capacity | Capacity is part of behavior, not just a performance knob. |
| Blocking send | `WriteAsync` waiting for space | `put` waiting for space | The send must have a cancellation alternative. |
| Non-blocking/drop experiment | `TryWrite` | `offer` | Dropping is a policy that must be counted and documented, never an accidental fallback. |
| Slow sink | delayed consumer task | slow queue consumer | It determines sustainable throughput regardless of buffer size. |
| Queue metrics | channel count/diagnostic counters | queue size and wait time | Instrument the boundary; do not infer health from process liveness alone. |

Go channels are built into the language and have a small surface area. That simplicity makes it easy to forget that a send is a synchronization point and a potential wait. In Java or .NET, the queue type often advertises boundedness in its name; in Go, the capacity is visible where the channel is created and should be recorded as configuration.

## Targeted official resources

- [Go Concurrency Patterns: Pipelines and cancellation](https://go.dev/blog/pipelines) - read the discussion of blocked senders, channel closing, and explicit cancellation.
- [`context` package](https://pkg.go.dev/context) - cancellation as the release path for blocked work.
- [`runtime` package](https://pkg.go.dev/runtime) - read `MemStats` documentation only for coarse experiment observations; it is not a throughput proof.
- [`testing` package](https://pkg.go.dev/testing) - table-driven tests, deadlines, and benchmarks if you choose to add a small benchmark after the behavioral test.
- [Go data race detector](https://go.dev/doc/articles/race_detector) - why shared counters and shutdown state need synchronization.
- [Go memory model](https://go.dev/ref/mem) - channel happens-before rules and why unsynchronized shared state is incorrect.
- [`testing/synctest`](https://pkg.go.dev/testing/synctest) - deterministic fake time and deadlock detection where the whole test fits a bubble.
- [`sync` package](https://pkg.go.dev/sync) - lifetime joining and safe mutex use; synchronization values must not be copied after first use.
- [Concurrency track intentional-failure experiments](go-concurrency-track.md#required-intentional-failure-experiments) - required block, race, leak, fan-in, and fan-out labs.

Reading task:

1. Find the pipeline guidance that stages either provide enough buffer for known values or explicitly signal senders.
2. Explain why a finite buffer postpones pressure but cannot eliminate it when arrival remains higher than service.
3. Decide which measurements can be made with counters and timestamps, and which would require profiling or a longer benchmark.
4. Explain why the race detector only examines executed paths and why a passing run does not prove semantic ordering or leak freedom.

## Experiment design

Run the same producer and sink with capacities:

```text
0, 1, 10, 100, 10,000
```

Keep these variables explicit:

- producer interval or target rate;
- sink delay per event;
- total experiment duration or event budget;
- channel capacity;
- shutdown mode: cancel immediately or allow the producer to finish and the sink to drain;
- whether events carry an enqueue timestamp.

For each capacity, record:

- produced events;
- consumed events;
- dropped events, if a drop policy is intentionally tested;
- elapsed wall-clock time;
- maximum observed queue depth;
- approximate producer blocked time;
- drain time after production stops;
- peak memory observation if instrumented;
- whether shutdown completed within the agreed bound.

Do not compare runs made with different producer rates or sink delays. Record the machine, Go version, operating system, commit, and experiment settings in the experiment note.

## Concurrency fault laboratories

Run these after the core capacity matrix. Preserve the failing observation in `docs/experiments/backpressure.md`, then keep only the corrected code and regression test.

### Blocking laboratory

- Hold the consumer behind an explicit test gate.
- Prove an unbuffered send cannot complete.
- Fill a bounded channel and prove the next send waits.
- Cancel the owner and prove the blocked sender joins.

### Race laboratory

- Temporarily let two goroutines update one plain counter or map.
- Run `go test -race` and capture the report.
- Fix the design using confinement/single-writer aggregation first; compare a small mutex/atomic alternative only if useful.
- Verify the corrected path under repeated race-enabled execution.

### Leak laboratory

- Temporarily remove the cancellation alternative from a full-channel send or leave a worker waiting on an input that no owner closes.
- Prove the runtime cannot join within the test guard.
- Restore an owned stop, close, and wait path.
- Prove repeated completion; use goroutine counts only as supporting diagnostics.

### Fan-in and bounded fan-out laboratory

- Run two finite generator collectors into the runtime-owned channel and verify group-owned close plus per-collector ordering.
- Compare sequential handling with a small fixed worker count under one deterministic slow operation.
- Track active workers and assert the configured limit is never exceeded.
- Observe output reordering and state which Streamforge guarantee it would break.
- Keep the production sink sequential unless measured throughput, idempotency, and ordering evidence pass the promotion gate.

## Design questions

Answer before implementing instrumentation:

1. Does increasing capacity change the sink’s long-term service rate? What observation would support your answer?
2. What does a larger buffer change: burst tolerance, producer blocking time, memory, drain time, or all of them?
3. What happens when the channel fills and the producer uses a blocking send?
4. What makes an unbounded queue dangerous even if it avoids immediate producer blocking?
5. Which source behaviors justify blocking, disconnecting, or best-effort dropping?
6. Why should queue depth and queue bytes be separate future metrics?
7. Why is “the run completed” weaker evidence than “the run completed with produced, consumed, backlog, and shutdown measurements”?
8. Which race fix best reduces shared state rather than merely protecting it?
9. What exact join condition proves the leak is fixed?
10. Which ordering promise changes when more than one sink worker handles events?
11. Why is a fixed worker count insufficient if its input queue is unbounded?

## Deliberately non-compilable pseudocode

This is a measurement recipe, not complete Go. Do not paste it into a source file.

```text
for each capacity in [0, 1, 10, 100, 10000]:
    create a bounded event queue with that capacity
    create producer and delayed sink
    start a monotonic experiment clock

    while the event budget is not exhausted and run is not cancelled:
        producer attempts to enqueue an event with sequence and enqueue time
        record whether the attempt waited, succeeded, or was deliberately dropped

    stop new production
    choose the declared shutdown mode
    let the sink drain accepted events, subject to the shutdown bound

    report produced, consumed, dropped, maximum depth,
           blocked time, drain time, memory observation, and errors
```

If you add a non-blocking policy, label it as a separate experiment. It is not an optimization of the blocking policy; it changes the delivery guarantee.

## Implementation constraints

- Keep collector and sink operations synchronous; the runtime owns producer/sink loops and the channel.
- Change one experimental variable at a time.
- Use a fixed event budget or fixed duration; do not let a slow run continue indefinitely.
- Use a monotonic elapsed-time source. Wall-clock timestamps can jump because of clock adjustment.
- Keep counters owned by one goroutine where possible; otherwise synchronize them and run with the race detector.
- Do not use `time.Sleep` as the only synchronization mechanism in tests.
- Do not allocate an unbounded slice of every event merely to calculate results; keep bounded summaries and counters.
- Do not report `runtime.MemStats` as an exact allocation or leak measurement.
- Do not add batching, retry queues, Kafka, Redis, or persistence to make the graph look more realistic. A fixed worker pool may exist only in the isolated comparison described above.
- Do not silently drop events. A drop policy must be explicit, counted, and included in the conclusion.
- Stop the timer and release all goroutines at the end of every run.
- Give every laboratory goroutine an owner, stop condition, and join path.
- Never hold a mutex while performing a delayed sink write or other I/O simulation.
- Do not send mutable maps/slices or reuse `json.RawMessage` backing bytes after handoff.
- Write the learner's measured setup, results table, limitations, and conclusion under `docs/experiments/backpressure.md`; the curriculum itself does not invent results.

Focused syntax hint policy: ask for the smallest existing hint level. Any focused syntax example must use an unrelated domain such as a bounded image-thumbnail queue, not Streamforge events or channels.

## Common failure modes

| Failure | Why it happens | Correction |
|---|---|---|
| Larger capacity appears “faster” | The run measures only early burst absorption | Separate startup burst, steady-state throughput, and drain time. |
| Producer and sink counts disagree | Shutdown cancels before accepted items drain, or a drop is not counted | Define the shutdown mode and count accepted, consumed, and dropped separately. |
| Experiment hangs at capacity zero | The sink or producer lacks a cancellation-aware operation | Test cancellation while the producer is blocked on send. |
| Memory grows with every run | A result slice or goroutine is retained | Keep bounded summaries and verify each run completes. |
| Queue depth is sampled too slowly | A short burst is missed | Sample around sends/receives or maintain a synchronized high-water mark. |
| Results vary wildly | Scheduling, CPU load, timers, or background work differ | Record environment, repeat runs, and describe variance rather than hiding it. |
| Non-blocking send silently loses data | try-send behavior was added without a policy | Make drops visible and test their count. |
| Shared counters race | Multiple goroutines write ordinary integers | Single-owner aggregation, synchronization, or atomics; verify with `go test -race`. |
| Race fix adds one large lock around slow work | Lock is held during simulated I/O | Copy/update bounded state under lock, release it, then perform the slow operation. |
| Leak test only compares goroutine counts | Runtime goroutines and test framework add noise | Assert the owned task completes after cancellation and resources close. |
| Pool has fixed workers but an unbounded input slice | Queue growth remains uncontrolled | Bound both active workers and queued admission. |
| Fan-out corrupts order silently | Completion order replaces source order | State ordering scope or keep one worker. |
| The conclusion claims scalability | One small local run is treated as a capacity guarantee | State the tested range and limitations; propose the next measurement. |

## Tests to write

Behavioral tests should be fast and deterministic; the experiment itself may be a separate manual or benchmark run.

- Capacity zero requires rendezvous between producer and consumer.
- A full bounded queue blocks the producer rather than growing silently.
- Cancelling while a producer is blocked releases it within a bounded time.
- Accepted events are either consumed or explicitly reported as remaining at cancellation.
- An intentional drop policy reports every drop.
- Queue depth never exceeds configured capacity.
- Maximum observed depth is zero for an idle queue and reaches capacity in a controlled saturation test.
- Producer and sink finish on every capacity, including 10,000.
- Repeated runs do not leak goroutines.
- Race-enabled tests detect no counter or shutdown races where the environment supports the detector.
- A temporary shared-state race is observed by the detector, fixed, and documented.
- A temporary blocked-send leak prevents join, then the corrected version joins repeatedly.
- Two collectors complete through group-owned closure without a send-after-close panic.
- The bounded fan-out comparison never exceeds its active-worker or queue limits.
- Cancellation releases the pool while its queue is full.

Optional benchmark questions:

- Is the sink’s steady-state consumption rate statistically distinguishable across capacities?
- How does drain time scale with backlog?
- What is the memory cost of the selected event representation?

## Commands to run

```powershell
go fmt ./...
go test ./...
go vet ./...
go test -race ./...
go test -run Backpressure -count=10 ./...
go test -run Concurrency -count=50 ./...
go test -run Leak -count=20 -timeout=30s ./...
```

If you add a benchmark, run it separately and preserve the exact command and parameters in the note. Do not use benchmark output as acceptance proof for correctness.

## Acceptance criteria

The experiment is complete when:

- all five capacities have been run under the same stated producer and sink conditions;
- produced, consumed, dropped, queue depth, blocked time, drain time, and shutdown result are recorded;
- the results show whether buffering changed burst behavior and memory, without claiming it increased sink service rate;
- cancellation works while the producer is blocked;
- the channel remains bounded and no silent unbounded queue exists;
- tests cover saturation, cancellation, accounting, capacity, and repeated cleanup;
- the learner has intentionally observed and fixed a race and goroutine leak;
- fan-in closure and per-collector ordering are demonstrated;
- bounded fan-out is measured but remains outside production unless promotion evidence is approved;
- `go fmt ./...`, `go test ./...`, and `go vet ./...` pass;
- the written conclusion names the guarantee and the uncertainty.

## Reflection questions

1. Which chart or table would make the buffer tradeoff easiest to explain to an operator?
2. What is the first metric you would add to distinguish a healthy slow source from a stalled sink?
3. If the source is replayable, why might blocking be preferable to dropping? If it is not replayable, what explicit policy is honest?
4. At what observed backlog or memory behavior would you stop the experiment and change the design?
5. What evidence would be required before replacing the channel with a durable journal?
6. Which shared state disappeared when you applied confinement?
7. Why did the leaked goroutine remain blocked, and which owner now releases it?
8. What throughput result and ordering plan would justify a production worker pool?

## Review submission template

Use the existing hint ladder and submit this at review:

```text
Milestone: Backpressure experiment
What I implemented:
What I expected:
What actually happened:
Commands I ran:
Errors or failing tests:
Decision I am unsure about:
My explanation of buffering, sustainable throughput, shutdown, and delivery policy:
Race observed and ownership-based fix:
Leak observed and join-path fix:
Fan-in closure and ordering evidence:
Bounded fan-out result and production decision:
```

The mentor reviews measurement validity, boundedness, accounting, cancellation, test determinism, and whether the conclusion stays within the evidence.
