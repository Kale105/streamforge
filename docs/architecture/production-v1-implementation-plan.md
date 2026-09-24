# StreamForge production v1 implementation plan

- Status: In progress
- Baseline: personal-mode commit `9d5930f`
- Target: self-hosted provider deployment with independently scalable workers
- Compatibility: the same dataset package runs in personal and provider modes

## Release contract

Production v1 is a deployable reference platform, not a claim of universal enterprise readiness. It must demonstrate, with reproducible tests, that multiple collector, processor, sink, and query replicas can survive duplicate delivery, restarts, poison events, and a temporary downstream outage without silently losing admitted data.

The release promises at-least-once delivery and idempotent materialization. It does not promise exactly-once effects at arbitrary external sinks.

## Decisions that keep the design focused

1. Keep one repository and one image with role-specific subcommands. Split packages and processes, not repositories.
2. Use PostgreSQL for desired state, API-key hashes, audit records, and normalized data.
3. Use Kafka only in provider mode, where retained partitioned transport and consumer groups enable independent horizontal scaling. Personal mode remains PostgreSQL-only.
4. Run trusted collectors as separate processes. Executing hostile tenant code is out of scope.
5. Provide API keys, scopes, quotas, and usage metrics, but integrate with an external gateway or identity provider for advanced identity, billing, and bot protection.
6. Supply Kubernetes manifests or a Helm chart after the containers and role boundaries work under Compose. Do not build an operator in v1.
7. Keep transformations deterministic and deliberately small. Arbitrary user code, joins, and a general workflow engine are out of scope.

## Architectural invariants

- An event is acknowledged to its source only after durable raw admission.
- Every queue, request, response, retry series, batch, lease, and shutdown wait is bounded.
- Kafka records are keyed by the declared ordering key; ordering is guaranteed only inside a partition.
- Consumers may receive duplicates and must use stable event and materialization identities.
- Poison events enter a durable DLQ and require an explicit, audited replay request.
- Dataset revisions are immutable. A replay names the exact source revision and target revision.
- Query replicas never consume from Kafka and ingestion workers never serve public queries.
- Credentials are referenced through environment/secret providers and never embedded in dataset packages.
- Labels and log fields have bounded cardinality; raw payloads and API keys are never logged.

## Target topology

```text
                         PostgreSQL control metadata
                          /         |            \
                 desired state   API keys      audit/replay
                      |                              |
source -> collector replicas -> Kafka raw -> processor replicas -> Kafka normalized
                                      |                    |               |
                                      +-> raw retention    +-> DLQ         v
                                                                    sink replicas
                                                                          |
                                                                 PostgreSQL datasets
                                                                          |
                                                                  query API replicas
                                                                          |
                                                                   API consumers
```

Kafka is transport, not the source of configuration truth. PostgreSQL is the managed query store, not a substitute for a distributed log.

## Parallel task graph

```text
E0 freeze production contracts and evidence plan
 |
 |-- E1 explicit DLQ and replay state machine
 |-- E2 API-key authentication, scopes, and quotas
 |-- E3 bounded metrics and operational endpoints
 `-- E4 schema validation and revision compatibility

E1 + E2 + E3 + E4
 `-- E5 compose the secure single-host provider surface
      |
      |-- E6 Kafka transport and worker role split
      |-- E7 PostgreSQL leases/checkpoints and collector ownership
      `-- E8 OpenAPI, filters/sorts, usage, and audit persistence

E6 + E7 + E8
 `-- E9 container and Compose failure tests
      `-- E10 Helm deployment and Kubernetes smoke test
           `-- E11 load/fault benchmark and runbooks
                `-- E12 independent senior release review
```

Tasks at the same indentation level own separate packages and can run concurrently. Each merge waits for its local unit tests and an integration barrier.

## Work packages and acceptance criteria

### E0 - Contracts and evidence

- Record service-level indicators: admitted events, end-to-end materialization latency, freshness, error rate, consumer lag, DLQ depth, and API latency.
- Add a reproducible workload generator and failure schedule before publishing performance numbers.
- Define small transport, checkpoint, replay, credential, metrics, and audit interfaces that contain no Kafka or PostgreSQL types.

Acceptance: every public performance statement names hardware, replicas, partitions, payload distribution, duration, percentiles, and failure conditions.

### E1 - DLQ and replay

- Store bounded diagnostics and terminal failure state beside immutable raw identity.
- List failures using bounded keyset pagination.
- Require an explicit replay request naming source event and target dataset revision.
- Make duplicate replay requests idempotent and record actor, reason, and timestamps.
- Never delete the original failure when replay succeeds.

Acceptance: crash, duplicate request, and poison-event tests prove that an admitted event is neither lost nor retried forever.

### E2 - API protection

- Persist only strong hashes of randomly generated opaque keys.
- Authorize dataset/action scopes before querying storage.
- Enforce per-key quotas with a local implementation and a replaceable distributed interface.
- Return stable `401`, `403`, and `429` problem responses and `Retry-After` without revealing key material.

Acceptance: concurrent tests prove limiter safety; logs and responses contain no plaintext secret.

### E3 - Observability

- Emit structured logs with event IDs, dataset revision, role, and error class, excluding payloads and secrets.
- Expose bounded-cardinality Prometheus metrics for collection, processing, queues, Kafka lag, DLQ, recovery, database work, and HTTP requests.
- Separate liveness, dependency readiness, and dataset freshness.
- Propagate W3C trace context at process boundaries; tracing export remains optional.

Acceptance: metrics remain bounded under arbitrary event IDs and source payloads; dashboards and alerts map to runbooks.

### E4 - Schemas and transforms

- Validate raw and normalized payloads against pinned JSON Schema revisions.
- Preserve schema ID and transform revision in event metadata.
- Support selection, rename, required/default, and explicit primitive conversion only.
- Reject incompatible configuration before workers claim assignments.

Acceptance: fixture conformance tests cover valid, invalid, forward-compatible, and intentionally breaking revisions; replay is deterministic for captured inputs.

### E5 - Secure composition

- Wire auth, quotas, metrics, DLQ administration, and schema validation into the single-host runtime.
- Add administrative commands for key creation/revocation, failure listing, and replay request.
- Keep health and metrics endpoints independently configurable from public dataset paths.

Acceptance: an end-to-end test starts with a dataset package, rejects an unauthenticated read, serves an authorized read, records metrics, and replays a failed event.

### E6 - Kafka transport and process roles

- Add raw and normalized topics with explicit partition-key, retention, compression, and maximum-message policies.
- Use idempotent producers and manual consumer acknowledgement after downstream durability.
- Add `worker collector`, `worker processor`, and `worker sink` commands using the same image.
- Handle partition revocation by stopping new work, finishing or cancelling bounded in-flight work, then committing only safe offsets.

Acceptance: multiple consumers rebalance under load; killing any one worker produces duplicates but no missing materialized identities.

### E7 - Ownership and checkpoints

- Store collector assignments, leases, fencing tokens, and resumable checkpoints in PostgreSQL.
- Renew leases with bounded jitter and reject writes from stale owners.
- Declare collector capabilities: resumable, partitionable, ordering scope, and acknowledgement mode.

Acceptance: two collectors cannot actively own the same non-partitionable assignment; takeover resumes from the last durably acknowledged checkpoint.

### E8 - Query and operational metadata

- Generate OpenAPI for configured datasets.
- Add allow-listed filters and sorts backed only by declared indexes.
- Persist key lifecycle, coarse usage aggregates, configuration revisions, and administrative audit entries.
- Keep billing and arbitrary SQL outside the service.

Acceptance: invalid query shapes cannot create SQL fragments; pagination is stable under inserts; revoked keys stop working within the documented propagation interval.

### E9 - Single-host provider deployment

- Compose PostgreSQL, a Kafka-compatible broker, each worker role, the query API, and a deterministic source.
- Add resource limits, health checks, non-root images, persistent volumes, and graceful termination.
- Test broker outage, database outage, slow sink, poison event, worker crash, and restart.

Acceptance: the documented fresh-clone path is repeatable and every failure scenario states whether the source was acknowledged.

### E10 - Kubernetes packaging

- Ship a Helm chart with separate Deployments for stateless roles and conservative defaults.
- Use Secrets, ConfigMaps, Services, pod security settings, disruption budgets, topology spread, and autoscaling examples.
- Treat PostgreSQL and Kafka as external dependencies by default; development subcharts are optional.

Acceptance: install, upgrade, rollback, scale-out, and pod-termination smoke tests pass on a local cluster. No custom operator is required.

### E11 - Evidence and operations

- Publish load profiles for small, medium, and saturation runs.
- Add alerts and runbooks for consumer lag, stale datasets, DLQ growth, database saturation, quota-store failure, and failed rollout.
- Document backup/restore, replay safety, key rotation, secret rotation, capacity planning, and rollback.

Acceptance: a fresh operator can diagnose and recover each injected failure using only the documented dashboards and runbooks.

### E12 - Release review

A senior reviewer audits correctness, security boundaries, migration safety, race behavior, rebalance semantics, query safety, deployment defaults, and benchmark reproducibility. All severity-one and severity-two findings block the release.

## Commit and promotion policy

Each independently useful milestone receives its own verified commit. A task is not promoted because it compiles: it requires unit tests, relevant integration/fault tests, formatting, static analysis, migration review, and documentation. Environment-limited checks may be deferred on intermediate commits only when tests are present, the limitation is explicit, and the release gate still requires them on a suitable host.

## Definition of done

Production v1 is done when a fresh deployment can install a versioned dataset, run multiple worker replicas, ingest through Kafka, materialize into PostgreSQL, expose an authenticated bounded API, deliberately route and replay poison records, display actionable metrics, survive the documented failures, and reproduce the published load test without code changes.
