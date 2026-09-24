# Streamforge lean MVP curriculum

- Status: Active learning path
- Audience: C# or Java developer learning Go and backend reliability
- Teaching rule: The learner writes all application code
- Scope: One Go process, one real source, PostgreSQL, and one read-only API

This directory is the detailed continuation of the [MVP learning guide](../mvp-learning-guide.md). The original guide remains the orientation and learning agreement; the files here are the stage-by-stage implementation briefs. When a long-range architecture document describes a larger system, the lean constraints in this curriculum control until the MVP release gate is passed.

## How to use the curriculum

Work in order. At each stage:

1. Read only the targeted resources and answer the design questions.
2. Attempt the implementation without copying a complete solution.
3. Run the required tests and commands.
4. Submit the review template from that guide.
5. Fix review findings before moving to the next stage.

When blocked, use the [five-level hint ladder](../mvp-learning-guide.md#hint-ladder). A focused syntax example must use an unrelated domain so that the learner still designs and writes the Streamforge implementation.

The required [Go concurrency track](go-concurrency-track.md) runs across the numbered stages. It keeps Stage 1 and the first collector-to-sink composition synchronous, then introduces owned goroutines, typed channels, bounded backpressure, fan-in, cancellation, joins, error policy, race/leak laboratories, load evidence, and worker-pool promotion gates in that order.

## Learning order and gates

| Stage | Guide | Evidence required to advance |
|---:|---|---|
| 1 | [Event JSON round trip](01-event-json-round-trip.md) | The envelope round-trips without changing its contract or raw payload meaning. |
| 2 | [Collector and sink interfaces](02-collector-sink-interfaces.md) | Narrow synchronous operations express behavior without infrastructure or concurrency policy. |
| 3 | [Generator collector](03-generator-collector.md) | One synchronous call returns a valid immutable event and is promptly cancellable. |
| 4 | [Stdout sink](04-stdout-sink.md) | A finite one-collector/one-sink synchronous baseline proves order, errors, and blocking. |
| 5 | [Runtime orchestration](05-runtime-orchestration.md) | Owned loops, typed fan-in channel, cancellation, errors, closure, drain, and joins are demonstrated. |
| 6 | [Backpressure experiment](06-backpressure-experiment.md) | Measurements plus intentional block/race/leak labs demonstrate safe bounded concurrency. |
| 7 | [Wikimedia SSE collector](07-wikimedia-sse-collector.md) | One permitted real stream works with bounded resources and honest recovery semantics. |
| 8 | [Local fake-source testing](08-local-fake-source-testing.md) | Normal and failure paths run deterministically without the public internet. |
| 9 | [PostgreSQL persistence](09-postgresql-persistence.md) | Raw and normalized writes are transactional and duplicate-safe. |
| 10 | [Read-only HTTP API](10-read-only-http-api.md) | Validation, bounded results, and stable keyset pagination are tested. |
| 11 | [Reliability and failure testing](11-reliability-failure-testing.md) | The observed guarantees and non-guarantees are backed by repeatable tests. |
| 12 | [Packaging and MVP release](12-packaging-release.md) | A clean clone starts, migrates, ingests, queries, shuts down, and documents limitations. |

The stage gates are cumulative. A later feature does not excuse a broken earlier contract.

### Concurrency production gate

The teaching exercises intentionally try more configurations than production. The release MVP keeps one selected collector task, one sink loop, one bounded event channel, and one reconnect owner. Multiple collectors and bounded workers remain experiments unless measurements plus ordering, idempotency, shutdown, and observability evidence pass their promotion gates. “Goroutines are cheap” is not promotion evidence.

## Lean MVP contract

The MVP contains only what is needed to prove this path:

```text
Wikimedia EventStreams SSE
    -> collector
    -> bounded in-process channel
    -> normalization
    -> PostgreSQL raw + normalized records
    -> read-only Go HTTP API
```

The real collector is Wikimedia EventStreams SSE rather than a credentialed sports API. It is a public, read-only, long-lived HTTP source that exercises cancellation, framing, reconnect classification, and backpressure without making the learning path depend on an API key or unresolved redistribution permission. The local fake source remains the authoritative test dependency. Sports-specific sources can be promoted after the collector contract is proven and their source policy is approved.

The MVP does not include Kafka, gRPC plugins, leases, CEL, React, Redis, Kubernetes, OpenTofu, custom infrastructure-as-code, API keys, or quotas. It also avoids a general schema engine, generalized retry framework, query-builder framework, and multi-service split.

## Promotion gates after the MVP

An excluded technology may be proposed only after the release checklist passes and the proposal identifies a measured limitation that the change solves.

| Candidate | Minimum promotion evidence |
|---|---|
| Kafka | A retained-log experiment must demonstrate replay, outage isolation, or independently scaled consumers that PostgreSQL personal mode cannot meet acceptably. |
| External or gRPC connectors | A second-language connector or crash-isolation need must exist, and the in-process contract must already be stable. |
| Leases and fencing | More than one collector worker must compete for the same source and a measured failover requirement must exist. |
| CEL or another expression system | At least two real normalization rules must show that reviewed Go functions are the bottleneck, with evaluation limits and versioning specified first. |
| Redis | Query or distributed rate-limit measurements must show PostgreSQL or process-local state is insufficient. |
| API keys and quotas | A real multi-consumer authorization or abuse-control requirement must be approved. |
| React | The HTTP contract and operator workflow must be stable enough that a UI will not drive premature backend churn. |
| Kubernetes or custom IaC | Compose must be insufficient for an explicitly supported deployment target with documented operational needs. |

"The architecture mentions it" is not promotion evidence.

## Cross-cutting rules

- Keep provider payload, event envelope, normalized storage record, and public API response as separate concepts.
- Pass `context.Context` through blocking boundaries; do not store it as optional configuration.
- Bound channels, HTTP bodies, database queries, page sizes, retry counts, and shutdown time.
- Give every application-started goroutine one owner, stop condition, completion/error path, and join before owner return.
- Prefer immutable handoff or single-writer confinement. A channel send does not deep-copy `json.RawMessage`, slices, maps, or pointed-to state.
- Start with a direct call. Add a channel, worker, mutex, or atomic only when its invariant and current need are explicit.
- Do not hold locks during HTTP, database, channel, or writer I/O.
- Return errors to the owner that can decide policy; logging is not a substitute for propagation.
- Preserve raw source data before or atomically with normalized state.
- Assume duplicate delivery and make effects idempotent with database constraints and transactions.
- Claim at-least-once processing with idempotent effects only where tests demonstrate it. Do not claim exactly-once delivery.
- Add a package or interface only when the current stage has at least two concrete reasons for the boundary.
- Keep tests deterministic. Live-source checks are manual smoke tests, not required unit or integration tests.

## Review responsibilities

The learner submits the template in each guide. The reviewer checks:

- correctness and test evidence;
- ownership of channels, goroutines, transactions, and shutdown;
- the problem, invariant, tradeoffs, use/non-use case, and Streamforge application for each concurrency pattern added;
- bounded resource behavior;
- separation of transport, provider, storage, and API models;
- error classification without speculative frameworks;
- unnecessary abstraction or post-MVP leakage; and
- whether the learner can explain the design in plain language.

Approval means the acceptance criteria were demonstrated, not merely that the code compiles.
