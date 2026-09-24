# Streamforge MVP - Concurrent runtime orchestration

- Status: Required MVP guide
- Sequence: after the finite synchronous collector-to-sink baseline
- Boundary: runtime-owned goroutines, typed channel, cancellation, errors, and joins

## Objective

Move the already-tested synchronous collector and sink operations into an intentionally owned concurrent pipeline. Add one concurrency property at a time: two goroutines, typed channel direction, bounded buffering, multi-producer fan-in, cancellation, `sync.WaitGroup` lifetime management, error propagation, and graceful shutdown.

The production MVP ends with one active collector task and one sink loop. The multi-collector version is a finite learning experiment that proves closure and ordering rules; it is not an instruction to enable several real sources at release.

## Prerequisites

- Complete Stages [01](01-event-json-round-trip.md) through [04](04-stdout-sink.md).
- Keep the finite synchronous integration test as a correctness baseline.
- Read Gates 2-7 and the structured-lifetime sections of the [Go concurrency track](go-concurrency-track.md#staged-learning-gates).
- Be able to identify every blocking operation in collector and sink calls.

## Conceptual explanation

The runtime—not the collector or sink—owns execution policy. It starts loops around the synchronous operations:

```text
collector task: synchronous collect -> callback sends each Event
sink loop: receive Event -> handle Event -> repeat
```

Separating those loops allows source waiting and destination work to overlap. That is concurrency. Whether the runtime executes them simultaneously on different processors is parallelism and is not required for the pipeline to be useful.

Every runtime-created goroutine must have:

- a named owner;
- a reason to exist;
- a stop condition;
- a path that releases every block;
- exactly one completion/error report; and
- a join path before the owner returns.

### Phase A: two goroutines and an unbuffered channel

Start with one goroutine running the synchronous collection operation, one sink-loop goroutine, and an unbuffered `Event` channel. The collector callback performs the send and does not return until the rendezvous completes. Use a test-controlled sink gate to observe that collection cannot continue until the consumer receives. Do not add buffering until the learner can narrate this block.

### Phase B: typed direction and bounded capacity

The runtime constructs the bidirectional channel, then passes a send-only view to producer-loop code and a receive-only view to consumer-loop code. Direction prevents accidental operations, but it does not decide closure ownership.

Use a small bounded capacity selected for the experiment. Capacity absorbs a finite burst; it does not increase sustainable sink throughput. A full send must also observe cancellation.

### Phase C: finite two-collector fan-in

Run two synthetic collector instances, each with one owner, into the same output. No individual producer closes the shared channel. The producer-group owner waits until every producer has stopped, then closes exactly once. Each collector's local order can be preserved while cross-collector merge order remains nondeterministic.

Return production configuration to one active real collector after the exercise.

### Phase D: cancellation and graceful drain

Use distinct roles rather than one ambiguous cancel signal:

- operator stop or first fatal error stops intake/production;
- channel closure tells the sink that all accepted sends are finished;
- the sink drains accepted buffered events during ordinary shutdown; and
- a separate bounded hard-abort deadline can terminate a stuck drain.

The collection operation, callback channel sends, timer/backoff waits, and later HTTP requests must observe the production context. The sink's synchronous handler receives a context appropriate to its work; ordinary producer cancellation must not accidentally discard the channel's buffered events.

### Phase E: joins and errors

`sync.WaitGroup` is a lifetime counter, not an error policy. In current Go, `WaitGroup.Go` starts and tracks tasks; manual `Add`/`Done` is still worth understanding because it exposes the ordering invariant and appears in existing code. A `WaitGroup` must not be copied after first use.

Use an explicit result/error mailbox sized so each component can report once without blocking shutdown. The coordinator records the first unexpected error, initiates the documented stop policy, and still waits for every owned goroutine. Expected cancellation is classified separately.

Conceptually compare this with `golang.org/x/sync/errgroup`: it combines grouped waits, first-error return, derived cancellation, and optional limits. It is useful for subtasks that share a simple first-error lifetime. The MVP can remain on explicit standard-library coordination because its stop-intake/drain/hard-abort policy is more specific than “cancel every sibling immediately.” Do not add a dependency merely to reduce a few lines.

### Happens-before and payload ownership

The event-channel send is synchronized before the matching receive completes, so work sequenced before the send can be observed after receive in a race-free design. The send does not deep-copy `Event.Data`. Producers must transfer immutable payload ownership or clone reusable bytes before sending. The sink never mutates a received event.

## C#/Java comparison

| Streamforge concern | C# analogy | Java analogy | Go lesson |
|---|---|---|---|
| Goroutine loop | Owned `Task` | Executor/virtual-thread task | Starting work is not supervision; the runtime must join it. |
| Typed channel | Bounded `Channel<T>` | Bounded `BlockingQueue<T>` | Direction and closure are explicit parts of the pipeline design. |
| Context | `CancellationToken` | Structured cancellation convention/deadline | Pass it through every cancellable blocking call. |
| WaitGroup | `Task.WhenAll` without result policy | `CountDownLatch` | Counts lifetime; it does not collect errors or decide cancellation. |
| Result mailbox | Task results/channel | Completion service/queue | Goroutine errors must be transported to their owner. |
| Fan-in | Several producers writing one channel | Several producers writing one queue | A group owner—not a producer—closes after all sends end. |
| Graceful drain | Stop producers then complete reader | Shutdown producer executor then drain queue | Intake cancellation and sink abort need distinct policies. |

## Targeted official resources

- [Concurrency is not parallelism](https://go.dev/blog/waza-talk).
- [Go FAQ: goroutines instead of threads](https://go.dev/doc/faq#goroutines).
- [Go memory model](https://go.dev/ref/mem) - goroutine creation, channel synchronization, locks, and `WaitGroup` ordering.
- [Go specification: channel types](https://go.dev/ref/spec#Channel_types), [send](https://go.dev/ref/spec#Send_statements), [receive](https://go.dev/ref/spec#Receive_operator), and [`select`](https://go.dev/ref/spec#Select_statements).
- [Pipelines and cancellation](https://go.dev/blog/pipelines) - fan-out/fan-in, closure, and unblocking senders.
- [`context`](https://pkg.go.dev/context) - propagation and cancel-function cleanup.
- [`sync.WaitGroup`](https://pkg.go.dev/sync#WaitGroup) - current `Go`, `Add`, `Done`, and `Wait` rules.
- [`errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) - conceptual comparison only.
- [Concurrency track](go-concurrency-track.md) - production invariants and intentional-failure labs.

## Design questions

1. Which two independently blocking operations justify the first two goroutines?
2. What blocks in the unbuffered version?
3. Who owns the event channel, and who is the only closer?
4. Why can no individual collector close a fan-in channel?
5. Which order is guaranteed with two collectors, and which is not?
6. If the sink fails while a producer is blocked on a full channel, what releases the producer?
7. Why would canceling the sink immediately on an operator stop break the drain guarantee?
8. What does `WaitGroup` prove, and what error information does it not carry?
9. When would `errgroup` simplify the design, and when would it hide a required drain policy?
10. Which event fields require an immutability/clone rule across the channel?
11. Why is a goroutine-per-event not needed here?
12. How does production return to one active collector after the fan-in lesson?

## Deliberately non-compilable pseudocode

```text
NOT RUNNABLE - CONTROL-FLOW PSEUDOCODE

create operator context
create stop-intake context
create hard-abort context for bounded drain
create bounded typed Event channel
create producer lifetime group and component result mailbox

for each approved collector instance:
    start one producer task owned by runtime
    run collector synchronously with stop-intake context and callback that:
        waits for either sending immutable event or stop-intake cancellation
        returns cancellation/send failure to collector
    classify normal terminal, expected cancellation, source error, or callback error
    report completion exactly once

start one producer-group closer task or coordinator step:
    wait until every producer task finishes
    close Event channel exactly once

start one sink task owned by runtime:
    repeat receive from Event channel
    on closed channel: report successful drain
    handle one event synchronously
    on handler failure: report failure

wait for operator stop or component result
on operator stop: cancel intake only
on sink failure: cancel intake and record failure
on collector failure: record failure, cancel intake for sibling producers, then let producer-group closure permit drain

wait for all producers and bounded sink drain
if drain deadline expires: hard-abort sink and report possible loss
join every owned task
return selected unexpected error, if any
```

## Implementation constraints

- Keep collector and sink interfaces synchronous; concurrency lives in runtime loops.
- Start with an unbuffered channel, then use one small bounded channel. The runtime supplies a synchronous callback that sends to it.
- Use `event.Event` as the typed element and directional views at loop boundaries.
- Runtime/producer-group ownership closes the channel after every collector task finishes. Receivers and individual collectors never close it.
- Give every goroutine one owner, stop condition, completion report, and join path.
- Use `sync.WaitGroup` for lifetime and an explicit result path for errors.
- Make every potentially blocking channel send cancellation-aware.
- Do not mutate an event or its `json.RawMessage` after handoff.
- Wait for every application-started goroutine before the runtime returns.
- Keep first-error cancellation as the MVP policy; partial source operation is post-MVP.
- Bound graceful drain and report hard-abort loss honestly.
- Do not add a generic supervisor framework, `errgroup` dependency, worker pool, goroutine-per-event, unbounded queue, or concurrent map.

## Common failure modes

| Failure | Broken invariant | Evidence to seek |
|---|---|---|
| Fire-and-forget loop | No owner or join | Runtime returns before worker completion. |
| `main` returns after starting tasks | Goroutine exit is not joined | Truncated output or missing error. |
| Collector closes shared fan-in channel | Closer does not know all sends ended | Send-after-close panic under two producers. |
| Sink closes its input | Receiver owns a producer fact it cannot know | Panic or hidden coupling. |
| Full send ignores cancellation | Stop cannot release producer | Bounded cancellation test hangs. |
| One root cancellation stops sink immediately | Ordinary shutdown cannot drain | Produced/handled mismatch. |
| Error is only logged | Owner cannot coordinate siblings | Process reports success after component failure. |
| Unbuffered result mailbox | Reporter can block while coordinator waits elsewhere | Shutdown deadlock. |
| WaitGroup is copied or added after an unsafe wait point | Lifetime counter splits or races | Vet/runtime failure and design review. |
| Mutable payload buffer reused | Channel transferred only slice header | Race/corrupted JSON under load. |
| Goroutine per event | Arrival rate controls goroutine count | Unbounded active work. |
| Lock held during writer/network I/O | Slow dependency blocks unrelated state | Contention/deadlock test. |

## Tests to write

Use controlled signals rather than sleeps:

- The synchronous Stage 4 baseline still passes.
- With an unbuffered channel, a held sink prevents producer send completion; releasing it completes both.
- One collector and one sink process a finite event sequence in order.
- The bounded channel never exceeds configured capacity.
- Two finite collectors fan in; each local sequence remains ordered and the group owner closes after both finish.
- No individual collector closes the shared channel.
- Operator stop cancels collection while allowing accepted events to drain.
- Sink failure cancels a producer blocked on a full channel.
- Collector failure cancels sibling producers, then closes through the group owner and permits bounded sink drain.
- Expected cancellation is not returned as an unexpected production failure.
- Hard-abort deadline releases a deliberately stuck sink and reports possible loss.
- Every started task reports once and is joined before return.
- A payload sent from a reusable buffer remains immutable at the sink.
- Repeated start/stop runs do not leak.

The deliberate race and leak variants belong to Stage 6 and the concurrency journal; do not keep broken code in the reviewed tree.

## Commands to run

```powershell
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
go test -run Runtime -count=20 ./...
go test -run Shutdown -count=20 -timeout=30s ./...
```

If the Windows environment cannot run `-race`, record the limitation and require a supported CI run before release.

## Acceptance criteria

- The direct synchronous baseline remains tested.
- The learner has observed rendezvous blocking before adding capacity.
- Runtime owns two production loops, one typed bounded channel, closure, errors, cancellation, and joins.
- A finite two-collector fan-in exercise demonstrates group-owned closure and honest ordering.
- Production configuration returns to one active collector.
- Every application goroutine has an owner, stop condition, and join path.
- Every full send and blocking component operation has a cancellation path.
- Ordinary stop drains; hard abort is bounded and reports possible loss.
- Payload bytes are immutable or cloned across handoff.
- Race-enabled and repeated lifecycle tests pass where supported.
- No worker pool, goroutine-per-event, generic supervisor, or unbounded queue enters the MVP.

## Reflection questions

1. What useful work overlaps even if only one CPU executes Go code?
2. Which channel operation created the blocking you observed?
3. What exactly tells the closer that no future send can occur?
4. Which shutdown signal stops intake, and which one aborts drain?
5. What does the `WaitGroup` synchronize that an error channel alone does not?
6. Which policy would need to change for partial collector failure?
7. Why does a channel send not make `json.RawMessage` safe to mutate?
8. What measurement would justify another sink worker?

## Review submission template

Use the existing hint ladder and attach the [concurrency review template](go-concurrency-track.md#concurrency-review-submission-template).

```text
Milestone: Concurrent runtime orchestration
Synchronous baseline evidence:
Unbuffered blocking observed:
Production goroutines and owners:
Channel type, capacity, sender, receiver, and closer:
Two-collector fan-in and ordering result:
Cancellation-aware blocking points:
Wait/join and error policy:
Drain and hard-abort policy:
RawMessage ownership rule:
Commands and repeated-test results:
Decision I am unsure about:
```
