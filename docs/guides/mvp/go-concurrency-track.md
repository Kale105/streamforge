# Go concurrency track for the Streamforge MVP

- Status: Required cross-cutting learning track
- Audience: C# or Java developer who is new to production Go concurrency
- Starts: After the synchronous event and JSON milestone
- Production rule: Add concurrency only where a measured blocking or throughput boundary justifies it

This track runs alongside the numbered MVP guides. It teaches goroutines and channels by changing one execution property at a time, then requires the learner to explain the invariant that makes each version safe. It is not permission to make every operation concurrent.

The learner writes every experiment and every production change. The pseudocode here is deliberately non-compilable. Use the [five-level hint ladder](../mvp-learning-guide.md#hint-ladder); focused syntax examples must use an unrelated domain.

## Learning outcomes

By the MVP release, the learner should be able to:

- distinguish concurrency from parallelism;
- explain why goroutines are inexpensive relative to OS threads without calling them free;
- name the owner, stop condition, and join path of every application-created goroutine;
- choose between a direct call, channel, mutex, atomic, or confined owner instead of defaulting to a channel;
- draw the producer, channel, consumer, cancellation, error, and channel-close relationships;
- predict where an unbuffered or full bounded channel blocks;
- explain what a channel send establishes in the Go memory model and what it does not copy;
- preserve or deliberately relax ordering;
- bound queues, workers, retries, request lifetimes, and shutdown;
- create and then fix a blocked pipeline, a data race, and a goroutine leak;
- design deterministic concurrency tests and interpret race-detector evidence; and
- defend the small concurrency budget used by the production MVP.

## Prerequisites and official resources

Complete [01 - Event JSON round-trip](01-event-json-round-trip.md) before starting this track. That milestone stays entirely synchronous.

Read the resources only when their gate is reached:

- [Concurrency is not parallelism](https://go.dev/blog/waza-talk) - structure versus simultaneous execution.
- [Go FAQ: goroutines instead of threads](https://go.dev/doc/faq#goroutines) - runtime multiplexing and growing goroutine stacks.
- [Go memory model](https://go.dev/ref/mem) - happens-before, channel synchronization, locks, and data-race-free execution.
- [Go specification: channel types](https://go.dev/ref/spec#Channel_types) - direction, capacity, and close behavior.
- [Pipelines and cancellation](https://go.dev/blog/pipelines) - stages, fan-out, fan-in, cancellation, and closure ownership.
- [`context` package](https://pkg.go.dev/context) - explicit propagation, deadlines, cancellation, and cleanup.
- [`sync` package](https://pkg.go.dev/sync) - `WaitGroup`, mutex rules, and the prohibition on copying synchronization values after use.
- [`testing/synctest`](https://pkg.go.dev/testing/synctest) - deterministic fake time and deadlock-aware test bubbles in the repository's Go 1.26 toolchain.
- [Go race detector](https://go.dev/doc/articles/race_detector) - dynamic race detection and report interpretation.
- [`net/http`](https://pkg.go.dev/net/http) - request contexts and client/server timeout boundaries.
- [`errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) - conceptual comparison for grouped lifetime, first-error cancellation, and concurrency limits. It is not required as an MVP dependency.
- [`golang.org/x/time/rate`](https://pkg.go.dev/golang.org/x/time/rate) - conceptual token-bucket reference for rate versus burst; it is not required for the one-connection MVP.
- [Go concurrency code-review guidance](https://go.dev/wiki/CodeReviewConcurrency) - common review checks and mistakes.

## Mental model: concurrency, parallelism, and scheduling

Concurrency is the structure that lets independent activities make progress during overlapping lifetimes. Parallelism is simultaneous execution on multiple processors. A concurrent Streamforge pipeline remains useful on one processor because a collector can wait for network input while a sink or HTTP handler makes progress. More processors may run runnable goroutines in parallel, but parallel speedup is neither automatic nor the primary correctness goal.

At a useful conceptual level, the Go runtime schedules many goroutines over a smaller set of operating-system threads. A goroutine that blocks in a channel operation, timer, or supported network operation lets other runnable goroutines execute. Goroutine stacks start small and grow. These properties make goroutines practical for independently blocking activities; they do not make goroutine count, memory, downstream capacity, file descriptors, or remote requests unbounded.

Do not base correctness on which goroutine is likely to run first. The scheduler may choose a different valid interleaving on another run. Correctness must come from explicit synchronization, ownership, and cancellation.

Hands-on scheduler observation: run two finite tasks with `GOMAXPROCS=1`; hold one at a channel or controlled I/O-like wait and prove the other can progress. Then compare a CPU-only workload with one versus more available processors. Record that concurrency enabled progress around blocking in the first case, while only the intrinsically parallel CPU workload could benefit from simultaneous processors. Do not turn scheduler timing into an assertion about which task runs first.

### Happens-before without mythology

A send on a channel is synchronized before the corresponding receive completes. Closing a channel is synchronized before a receive that observes the closed state. `WaitGroup` completion and mutex operations also establish documented ordering. These relationships let one goroutine safely observe work completed by another when all relevant accesses follow the synchronization design.

Channel send does not deep-copy the value graph. An `Event` value contains `json.RawMessage`, which is a byte slice. Sending the struct copies the slice header, not necessarily its backing bytes. After handoff, either:

- the producer transfers exclusive ownership and never mutates the bytes again;
- the producer clones the bytes before handoff; or
- access is synchronized under a deliberately reviewed shared-ownership design.

Use transfer or cloning for Streamforge events. Shared mutable payloads are not justified in the lean MVP. The same rule applies to maps, slices, and pointers nested inside messages.

## Staged learning gates

Do not skip directly to the final pipeline. Preserve each small version long enough to explain what changed.

| Gate | Curriculum point | Hands-on change | Evidence required |
|---:|---|---|---|
| 0 | Stage 1 | Model and round-trip one Event synchronously | No goroutine or channel appears in event tests. |
| 1 | Stages 2-4 | Run one finite collector whose synchronous callback calls one sink in the same goroutine | A finite synchronous test establishes behavior, error order, and baseline latency. |
| 2 | Stage 5A | Put the synchronous collector operation and sink loop in two owned goroutines with an unbuffered typed channel | The learner predicts and observes rendezvous blocking. |
| 3 | Stage 5B | Give the channel one small bounded capacity and document sender, receiver, and closer | Directional types compile; the ownership diagram identifies exactly one close path. |
| 4 | Stage 6A | Make the sink slower and compare capacities | Measurements show buffering changes burst tolerance, not sustainable throughput. |
| 5 | Stage 5C/6B | Run two finite synthetic collectors into one fan-in path | A producer-group owner closes only after both producers finish; per-collector order and merged-order limits are stated. |
| 6 | Stage 5D | Propagate context through waits, sends, receives, timers, and requests | Cancellation releases every blocked operation within a test deadline. |
| 7 | Stage 5E | Join all owned goroutines with `sync.WaitGroup` and return component errors | Shutdown cannot return while an application goroutine remains owned. |
| 8 | Stage 6C | Introduce a shared-state race intentionally, observe it with `-race`, then replace it with confinement or synchronization | The failing report and corrected design are recorded without keeping broken code. |
| 9 | Stage 6D | Create a blocked-send leak intentionally, observe non-completion, then add a stop path and join | The corrected test proves completion instead of relying only on a goroutine count. |
| 10 | Stage 11 | Run concurrency, failure, and bounded-load tests repeatedly | Race, leak, deadlock, overload, ordering, and shutdown evidence pass. |
| 11 | Post-measurement only | Compare sequential processing with a bounded worker pool | A pool is promoted only if measurements justify it and ordering/idempotency policy is complete. |

The two-collector exercise is a learning experiment. The production lean MVP still runs one selected source collector at a time unless a second real source is approved later.

## Pattern reviews

For each pattern, the learner must answer six questions: what problem it solves, what invariant it establishes, its tradeoffs, when to use it, when not to use it, and how it applies to Streamforge.

### 1. Structured lifetime ownership

- **Problem:** fire-and-forget work can outlive its caller, hide failures, retain resources, or disappear when `main` returns.
- **Invariant:** every started goroutine has one identifiable owner, a stop condition, and a join/wait path.
- **Tradeoffs:** explicit lifetime wiring adds a small amount of coordination code and forces a failure policy.
- **Use when:** work may overlap its caller, block, or own a resource.
- **Do not use when:** a direct synchronous call is simpler and meets the latency requirement.
- **Streamforge:** the runtime owns collector and sink loops, their cancellation scopes, error results, and final wait. `net/http` owns per-request goroutines; Streamforge still owns server shutdown and dependency limits.

### 2. Confinement and single-writer ownership

- **Problem:** shared mutable counters, maps, buffers, and lifecycle flags create races and complex lock protocols.
- **Invariant:** one goroutine owns each mutable value, or every access follows one explicit synchronization rule.
- **Tradeoffs:** confinement may require messages or snapshots and can centralize a hot path.
- **Use when:** one stage can naturally aggregate state, such as metrics totals or channel closure.
- **Do not use when:** immutable values can simply be copied, or a short well-scoped mutex is clearer.
- **Streamforge:** one metrics aggregator owns mutable experiment counters; one producer-group owner closes the event channel; accepted Event payloads become immutable after publication.

### 3. Pipeline stages and typed channel ownership

- **Problem:** independently blocking source and destination work need a visible handoff and pressure boundary.
- **Invariant:** producers only send, consumers only receive, and exactly one documented owner closes after all sends finish.
- **Tradeoffs:** channels add scheduling, lifecycle, ordering, and cancellation obligations; a direct call is easier to reason about.
- **Use when:** stages have independently blocking lifetimes or measured burst decoupling needs.
- **Do not use when:** a synchronous call expresses the operation without unacceptable blocking. A channel is not dependency injection or an automatic replacement for a mutex.
- **Streamforge:** component interfaces stay synchronous; the runtime supplies collectors a synchronous channel-sending callback and deliberately inserts one typed event channel before the sink loop.

### 4. Fan-in

- **Problem:** several producers need one downstream admission path.
- **Invariant:** no producer closes the shared output; a group owner waits for all producers and closes once.
- **Tradeoffs:** events from different producers interleave nondeterministically, and a shared channel can create head-of-line effects.
- **Use when:** independently owned collectors feed the same sink contract.
- **Do not use when:** one production source is enough or the sources require isolated capacity/failure domains.
- **Streamforge:** two synthetic collectors demonstrate fan-in; the release configuration retains one real collector.

### 5. Bounded fan-out, worker pools, and concurrency limits

- **Problem:** expensive independent work may need parallel service, but goroutine-per-event can exhaust memory, connections, or the downstream system.
- **Invariant:** active work never exceeds a reviewed limit and queued work is also bounded.
- **Tradeoffs:** workers add reordering, shutdown coordination, tuning, and possible head-of-line blocking.
- **Use when:** load evidence shows one worker is insufficient, operations are independent, and a downstream capacity limit is known.
- **Do not use when:** the bottleneck is the source/database, ordering is required, or the workload is already fast enough.
- **Streamforge:** keep the MVP persistence sink sequential. A worker-pool experiment is allowed only after idempotency tests and an ordering/partition plan; the database pool is not proof that more application workers help.

### 6. Backpressure and overload policy

- **Problem:** arrival rate can exceed sustainable processing rate.
- **Invariant:** queue capacity is finite, and full capacity leads to a documented outcome: wait, reject/drop, disconnect, or shed at a named boundary.
- **Tradeoffs:** waiting raises latency and propagates pressure; dropping loses work; large buffers delay failure and consume memory.
- **Use when:** a stage boundary needs burst absorption and visible flow control.
- **Do not use when:** an unbounded slice is being disguised as a queue or capacity was chosen without measurement.
- **Streamforge:** the ingestion channel blocks the collector by default. No silent drop policy is allowed for accepted events. The backpressure report selects capacity from measured burst and memory behavior.

### 7. Cancellation propagation

- **Problem:** a blocked timer, channel operation, HTTP request, or database call can prevent shutdown and leak resources.
- **Invariant:** the operation-scoped context reaches every cancellable blocking boundary, and every derived cancel function is eventually called.
- **Tradeoffs:** cancellation makes partial progress possible and requires callers to classify expected cancellation versus failure.
- **Use when:** work is request-scoped, has a deadline, or must stop when its owner stops.
- **Do not use when:** passing optional configuration or hiding a context in a struct.
- **Streamforge:** collector waits and channel sends select on cancellation; Wikimedia requests are created with the collector context; database and API calls inherit bounded contexts.

### 8. Graceful shutdown

- **Problem:** immediate process exit loses accepted work, while unlimited drain can hang forever.
- **Invariant:** stop intake, let the producer group finish and close its channel, drain or discard under a documented policy, then join every owned goroutine within a deadline.
- **Tradeoffs:** draining improves completion but increases shutdown time; hard abort bounds time but may lose accepted in-memory work.
- **Use when:** the process owns long-lived components or buffered work.
- **Do not use when:** “graceful” means merely sleeping before exit.
- **Streamforge:** ordinary stop drains the bounded event channel; sink failure stops intake immediately; a separate hard-abort deadline ends a stuck drain and reports possible loss.

### 9. Error propagation and group policy

- **Problem:** goroutine return values do not automatically reach their owner, and logging alone cannot coordinate siblings.
- **Invariant:** every component reports completion once; the coordinator applies an explicit first-error or partial-failure policy and still waits for all owned work.
- **Tradeoffs:** first-error cancellation is simple but may discard independent useful work; partial failure needs isolation, aggregation, and a definition of degraded health.
- **Use when:** sibling tasks share a lifetime or an error affects process correctness.
- **Do not use when:** a library goroutine logs and decides process policy for its caller.
- **Streamforge:** the lean ingestion pipeline uses first unexpected error to stop production, followed by bounded drain/join. Compare explicit error channels plus `WaitGroup` with `errgroup` conceptually; do not add `errgroup` unless it removes real lifecycle mistakes without hiding drain policy.

### 10. Rate limits, timeouts, and bounded retries

- **Problem:** unconstrained external calls and synchronized retries can overload a provider or amplify an outage.
- **Invariant:** each operation has one timeout owner, attempt count and total retry time are bounded, delays honor cancellation, and retry occurs only for classified transient failures.
- **Tradeoffs:** aggressive limits fail fast but may reject recoverable work; retries increase latency and duplicate-delivery exposure; jitter reduces coordinated retry bursts but reduces deterministic timing.
- **Use when:** a source policy permits repeated calls and the failure class is known to be transient.
- **Do not use when:** validation/authentication failures are permanent, an outer layer already retries, or an SSE event-processing failure would be hidden by reconnecting.
- **Streamforge:** Wikimedia reconnects use one bounded loop with cancellable jittered delay. No nested HTTP-client/runtime/sink retry stacks. Fake-source tests inject delay decisions instead of sleeping.

Keep the controls distinct: a concurrency limit caps work active at once; a rate limit caps starts over time; a timeout bounds one operation; and a retry budget bounds repeated operations. One control cannot substitute for all the others. The lean SSE collector needs one connection plus bounded reconnect cadence. The read-only API does not gain consumer quotas or API keys in the MVP.

### 11. Idempotency at concurrency boundaries

- **Problem:** concurrent attempts, retries, and reconnects can deliver the same logical event more than once.
- **Invariant:** a stable source identity and database uniqueness/transaction boundary make duplicate attempts converge to one logical effect.
- **Tradeoffs:** idempotency requires identity rules, conflict diagnostics, storage work, and careful handling of same-ID/different-payload cases.
- **Use when:** processing is at-least-once or multiple workers can race on the same identity.
- **Do not use when:** claiming “exactly once” from a channel or mutex without durable atomic state.
- **Streamforge:** raw and normalized admission commit in one PostgreSQL transaction; concurrent duplicate tests must converge, while changed payload under the same ID remains visible as a conflict.

### 12. Ordering and partitioning by key

- **Problem:** fan-out improves throughput but can reorder observations that must be applied sequentially.
- **Invariant:** the system states its ordering scope—global, per source, per entity/key, or none—and all routing/worker behavior preserves only that promised scope.
- **Tradeoffs:** stronger ordering reduces parallelism and can create hot keys; weaker ordering requires versions, commutative effects, or rejection of stale updates.
- **Use when:** source corrections or entity state depend on sequence.
- **Do not use when:** global ordering is added merely because a test expected one scheduler interleaving.
- **Streamforge:** the append-only Wikimedia MVP may preserve per-collector emission order with one sink. A future mutable sports projection needs a trusted source version or key-partitioned worker design before parallel writes are promoted.

### 13. Mutable slices, maps, and payload transfer

- **Problem:** copying a struct containing a slice, map, or pointer can leave shared mutable backing state across goroutines.
- **Invariant:** ownership transfers once, the data is cloned before send, or all access is synchronized; mutation after publication is forbidden by default.
- **Tradeoffs:** cloning costs allocations and bytes; transfer constrains reuse; locking adds contention and protocol complexity.
- **Use when:** parsers reuse buffers, producers construct `json.RawMessage`, or maps appear inside messages.
- **Do not use when:** assuming a channel send deep-copies the payload.
- **Streamforge:** clone provider bytes when their source buffer may be reused, then treat `Event.Data` as immutable through channel, storage, and API boundaries.

### 14. Concurrency observability

- **Problem:** a pipeline can be correct at low load but silently saturate, queue, retry, or drop under production conditions.
- **Invariant:** bounded, low-cardinality measurements expose queue depth/capacity, active workers/limit, processing latency, failures by stable class, retry attempts, and deliberately dropped/rejected events.
- **Tradeoffs:** instrumentation has cost and can itself contend or create high-cardinality data.
- **Use when:** a concurrency limit or overload policy needs operational evidence.
- **Do not use when:** recording event IDs, URLs, or unbounded error strings as metric labels.
- **Streamforge:** the MVP may use simple counters/snapshots and structured logs; it does not add a metrics platform solely for the lesson. Queue depth is a signal, not a correctness proof.

### 15. Deterministic concurrency testing

- **Problem:** sleeps and scheduler luck create slow, flaky tests that fail to prove blocking, ordering, or release behavior.
- **Invariant:** tests coordinate transitions with channels, fakes, barriers, or `testing/synctest`; deadlines are final guards, not the primary synchronization method.
- **Tradeoffs:** test seams require design effort and can overfit implementation details if too invasive.
- **Use when:** proving blocked sends, cancellation, shutdown, retry timing, fan-in completion, or leaks.
- **Do not use when:** asserting exact goroutine scheduling or treating a single race-free run as proof.
- **Streamforge:** fake collectors/sinks expose controlled start, block, release, and fail points. Run race and repeated tests in addition to behavior assertions.

## Required intentional-failure experiments

Keep deliberately broken variants only in a temporary branch, patch, or engineering journal. The reviewed tree must contain the fixed version and regression test.

### Experiment A: rendezvous blocking

1. Use an unbuffered channel between one finite producer and one consumer.
2. Hold the consumer at a test-controlled gate.
3. Prove the producer cannot complete its send.
4. Release the consumer and prove both complete.
5. Explain the happens-before edge created by the matching send/receive.

Do not use a sleep to guess that the producer reached the send.

### Experiment B: bounded backpressure

1. Run the same finite workload with capacities 0, 1, a measured small value, and an intentionally oversized value.
2. Block or slow the sink deterministically.
3. Record production completion time, end-to-end time, maximum observed depth, and memory caveats.
4. State the overload policy when capacity is exhausted.
5. Explain why the buffer did not change steady-state sink throughput.

### Experiment C: data race

1. Let two goroutines update one ordinary counter or map without synchronization in a temporary broken test.
2. Run the race detector and preserve the report in the engineering journal.
3. Fix it first through confinement/single-writer aggregation if that fits.
4. Compare the confined version with a small mutex or atomic design.
5. Keep a race-enabled regression test that exercises the corrected path.

The lesson is not “add a mutex wherever the detector points.” Identify the ownership invariant that was absent.

### Experiment D: goroutine leak

1. Start a producer that can become stuck sending after its consumer stops.
2. Prove the owner cannot join it within a bounded test guard.
3. Add cancellation-aware send behavior and an owned stop/join path.
4. Prove the corrected version completes repeatedly.

Goroutine counts may support diagnosis, but completion and released resources are the primary assertions.

### Experiment E: fan-in and closure

1. Run two finite synthetic collectors with distinguishable source IDs.
2. Preserve each collector's local sequence while allowing cross-source interleaving.
3. Wait for both producers before the group owner closes the output.
4. Intentionally model the panic risk of one producer closing the shared channel; do not keep panic-producing code.
5. State whether one collector failure cancels all producers or allows partial operation. The lean runtime uses first-error cancellation.

### Experiment F: bounded fan-out

1. Use a deterministic slow handler and compare one worker with a small fixed worker count.
2. Assert the active count never exceeds the limit.
3. Observe result reordering and identify which ordering promise would be broken.
4. Cancel while the work queue is full and prove producers and workers all join.
5. Decide whether measurements justify any pool in production. The expected MVP answer is usually no.

### Experiment G: timeout, rate, and retry ownership

1. Script a fake source to return transient failures before one success.
2. Record requested backoff/jitter delays without sleeping and assert one owner stays within the attempt and elapsed-time budgets.
3. Assert no more than one SSE connection/reconnect attempt is active at once.
4. Cancel during the delay and during an HTTP request; prove both return and join.
5. Add a temporary outer retry around the already-retrying collector only on paper or in a throwaway test to calculate how attempts multiply; do not keep nested retries.
6. State which control limits starts over time, which limits active work, which bounds one attempt, and which bounds the entire recovery episode.

## Deliberately non-compilable orchestration sketch

```text
NOT RUNNABLE - CONTROL-FLOW PSEUDOCODE

create operation context and bounded Event channel
create producer lifetime group, sink lifetime, and result mailbox

for each approved collector:
    start one owned producer task
    run synchronous collection with callback that:
        waits for either immutable Event channel send or cancellation
        returns send/cancellation failure to collection
    report completion exactly once

start one owner task that:
    waits for every producer
    closes the event channel exactly once

start one sink task that:
    receives events until channel close
    handles each event synchronously
    reports completion exactly once

on operator stop:
    stop intake
    permit bounded drain

on first unexpected component error:
    stop intake
    apply documented drain or abort policy

wait for every owned task
return the selected error and shutdown evidence
```

## Production MVP concurrency budget

The release candidate should be intentionally boring:

- one configured source collector active at a time;
- one goroutine running the selected synchronous collector task;
- one producer-group channel closer, which may be the coordinator rather than a permanent extra goroutine;
- one sink loop goroutine;
- one bounded event channel with capacity justified by Stage 6 measurements;
- one retry/reconnect loop owned by the collector, never nested retry layers;
- sequential normalization and PostgreSQL admission unless a measured gate approves bounded workers;
- `net/http` request concurrency managed by the server, protected by request deadlines, bounded page sizes, and the database pool;
- no goroutine-per-event, unbounded task queue, background fire-and-forget write, or hidden asynchronous logging path.

Multiple collectors, worker pools, keyed partitions, and partial-failure operation are learning experiments or post-MVP promotions unless the release checklist records a measured need and updated invariants.

## Anti-pattern review checklist

| Anti-pattern | Why it fails | Review response |
|---|---|---|
| Fire-and-forget goroutine | No owner, error path, stop, or join | Keep the call synchronous or attach it to a lifetime owner. |
| Goroutine per event | Arrival rate controls resource creation | Use sequential handling or a bounded queue plus fixed limit after measurement. |
| Unowned channel closure | Send-after-close panic or receivers never finish | Name one closer that knows all sends are finished. |
| Unbounded queue | Overload becomes memory growth and stale work | Bound capacity and choose wait/reject/drop policy. |
| Shared map without synchronization | Concurrent mutation is a race and can crash | Confine ownership, snapshot, or use a reviewed lock. |
| Copying a mutex/WaitGroup | Copies split synchronization state | Keep synchronization values behind stable pointers/owners; heed `go vet`. |
| Channel as the default answer | Adds lifecycle and blocking without solving a real boundary | Start with a direct call; justify each channel. |
| Holding a lock during I/O | One slow dependency blocks unrelated access and can deadlock | Copy required state under lock, release it, then perform I/O. |
| Closing from the receiver | Receiver cannot know whether all producers stopped | Producer-group owner closes after join. |
| Sleeping for synchronization | Scheduler timing makes tests flaky | Coordinate with explicit test signals or `testing/synctest`. |
| Retrying at multiple layers | Attempts multiply and overload the dependency | Assign one retry owner and one total budget. |
| Sending mutable `RawMessage` then reusing its buffer | Consumer sees concurrent mutation or corrupted JSON | Clone or transfer exclusive ownership before send. |

## Commands and test evidence

Run from the repository root as the relevant packages appear:

```powershell
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
go test -count=20 ./...
go test -run Concurrency -count=50 ./...
go test -run Backpressure -count=10 ./...
go test -run Shutdown -count=20 ./...
go test -run Leak -count=20 -timeout=30s ./...
```

The race detector only finds races on executed paths. A passing run is evidence, not a proof of all concurrency correctness. If `-race` is unavailable in the Windows toolchain, record the limitation and run it in a supported CI environment before release.

For load experiments, record hardware, Go version, workload size, payload distribution, channel capacity, worker count, database pool settings, duration, and whether the race detector was enabled. Do not compare throughput numbers from unlike environments.

## Acceptance criteria

- Stage 1 remains synchronous and contains no concurrency requirement.
- A finite one-collector/one-sink synchronous baseline passes before any goroutine pipeline is accepted.
- The learner demonstrates and explains unbuffered blocking and bounded backpressure.
- A two-collector fan-in experiment has correct group-owned closure and documented ordering.
- Every production goroutine has a named owner, stop condition, and join path.
- Every blocking send, receive, timer, HTTP request, and database call has the appropriate cancellation/deadline path.
- The learner intentionally observes and fixes one race and one goroutine leak.
- Error, shutdown, overload, retry, idempotency, and ordering policies are explicit.
- A fake-source test proves one rate/retry owner, bounded attempts and elapsed time, injected jitter decisions, and cancellation during request/delay.
- Mutable event payload ownership is safe across goroutine boundaries.
- Repeated, race-enabled, and bounded-load tests support the claims.
- The production MVP stays within the stated concurrency budget; worker pools remain absent unless promotion evidence is approved.

## Reflection questions

1. Which Streamforge boundaries benefit from concurrency because they block independently?
2. Where would a direct function call be clearer than a channel?
3. Which values are confined, immutable, cloned, or synchronized?
4. What exact event does each goroutine wait for before it can exit?
5. Which ordering guarantee survives the two-collector experiment?
6. Where can backpressure propagate outside the process?
7. What is the total retry budget across all layers?
8. Which metric would reveal saturation before memory exhaustion?
9. What measured evidence would justify a persistence worker pool?
10. Why is “goroutines are cheap” not a capacity plan?

## Concurrency review submission template

```text
Concurrency gate:
Synchronous baseline retained:
Goroutines and their owners:
Stop condition for each goroutine:
Join/wait path for each goroutine:
Channel element type, capacity, sender, receiver, and closer:
Shared mutable values and their ownership/synchronization:
Cancellation and timeout boundaries:
First-error or partial-failure policy:
Shutdown drain/discard policy:
Overload and retry policy:
Ordering and idempotency guarantees:
Race deliberately observed and fixed:
Leak/block deliberately observed and fixed:
Commands and repeated-test evidence:
Queue/worker/latency/failure observations:
Production concurrency added or deliberately rejected:
Decision I am unsure about:
```
