# Provider-mode operations

Provider mode is a separately deployable pipeline:

`collector -> raw Kafka -> processor -> normalized Kafka -> sink -> PostgreSQL -> query API`

Failures from processor and sink are written to the revision-scoped Kafka DLQ,
then `dlq-indexer` persists them for `provider-admin`. The `replay` worker claims
audited requests from PostgreSQL and publishes them to the matching replay topic.
The public query API has no Kafka consumer and provider-admin is a separate,
provider-admin-scoped API-key surface.

## Collector runtime and ownership

`worker collector` is the only provider collector entrypoint. It acquires one
PostgreSQL-backed lease for the dataset source partition, then creates both a
fresh configured collector and a Kafka transactional producer inside that
lease. The transactional ID is stable for the logical assignment, so a
successor fences an earlier producer at Kafka. Lease contention, renewal loss,
and retry are all not-ready states; a worker is ready only while it owns a
lease and its collector and producer have been initialized.

Personal `server` uses the same configured collector factory in `personal`
mode. Only `http-json` is allowed there; `sse` and `subprocess` are rejected
before the API or metrics listener starts. Personal mode creates no Kafka
client.

Provider collector choices are configured beneath `collector` in the Dataset
YAML:

- `http-json` polls one bounded HTTP response.
- `sse` is resume-safe. On each acquired lease the worker reads the durable
  cursor, sends it as `Last-Event-ID`, publishes the event transactionally, and
  only then advances the cursor with that lease's fencing token. A stale owner
  cannot checkpoint a successor's stream.
- `subprocess` launches a trusted absolute-path connector with a clean,
  allow-listed environment. The host supplies dataset, source, event type, and
  pinned contract metadata; the connector's acknowledgement is written only
  after Kafka has accepted the event. Connector checkpoints are intentionally
  unsupported until the protocol has a restore handshake.

All collector source errors are classified by typed sentinels (`http-json`,
SSE, or subprocess) and retried with a finite worker retry budget. A permanent
configuration or protocol error stops the worker for operator action.

## Local Compose demonstration

`compose.provider.yaml` is intentionally a **single-host, single-broker local
development stack**, not an HA or production deployment. Set a unique
`STREAMFORGE_DB_PASSWORD` in your environment before running Compose; never
copy that value into a shared environment.
The example bundle at `deploy/provider-example` includes a normalization revision
and SHA-256-pinned local raw/normalized schemas, which provider workers require.

```powershell
$env:STREAMFORGE_DB_PASSWORD = "<unique-local-password>"
docker compose -f compose.provider.yaml config
docker compose -f compose.provider.yaml up --build -d
docker compose -f compose.provider.yaml ps
docker compose -f compose.provider.yaml logs --tail=100 collector processor sink dlq-indexer replay
```

Workers expose private container-only admin listeners on port 8081; their
`/healthz`, `/readyz`, and `/metrics` endpoints are used by Compose probes.
Only query (8080/9090) and provider-admin (8081) bind loopback host ports.
Create a provider-admin key using `key create -scopes '*:provider-admin'` and a
read key using `sample-events:read`; plaintext keys are displayed only once.

Stop cleanly with `docker compose -f compose.provider.yaml stop`. Do not use
`down -v` except when intentionally deleting the local broker/database evidence.

## Kubernetes / Helm

The Helm chart expects externally operated HA Kafka and PostgreSQL and secrets
created outside the chart. It deploys collector, processor, sink, dlq-indexer,
replay, query, and provider-admin as separate Deployments. Worker admin probes
are HTTP; provider-admin has no health route, so its probe is TCP listener-only.
TLS/SASL settings and certificate paths come from externally managed Kafka
secrets; there are no plaintext secret values in chart defaults. Dataset and
schemas are mounted read-only together; a config checksum rolls pods when that
package changes.

Use an immutable release tag, never `latest`:

```sh
kubectl create secret generic streamforge-database --from-literal=url='postgres://...'
kubectl create secret generic streamforge-kafka --from-literal=brokers='kafka-1:9093,kafka-2:9093' --from-literal=tls-enabled=true --from-literal=tls-server-name=kafka.internal --from-literal=sasl-mechanism=SCRAM-SHA-512 --from-literal=sasl-username=... --from-literal=sasl-password=...
kubectl create secret generic streamforge-kafka-tls --from-file=ca.pem --from-file=tls.crt --from-file=tls.key
helm upgrade --install streamforge ./deploy/helm/streamforge --set image.repository=registry.example.com/streamforge --set image.tag=1.2.3
```

The chart does not create a network policy, database role, Kafka ACL, backup,
or certificate rotation policy. Apply least-privilege identities and network
controls in the surrounding platform. HPA/PDB are limited to processor and sink:
they are consumer roles with redundant replicas. Collectors, replay, query, and
admin remain explicitly sized because source ownership, replay authority, and
the built-in process-local quota limiter have different semantics.

## Fault evidence runbook

These commands are the required evidence commands. Runtime success is
**unverified until they are executed against a target environment**; static
checks and unit tests do not prove broker, database, or Kubernetes behavior.

| Scenario | Action | Evidence |
| --- | --- | --- |
| Sink/database outage | Stop PostgreSQL for a short window, restart it. | Sink logs show retry/catch-up; query `/readyz` is unavailable during outage then recovers; row counts advance after recovery. |
| Broker outage | Stop the local Redpanda container, restart it. | Collector/processor logs show bounded retry and readiness returns after broker health. |
| Consumer failover | Stop one processor or sink pod/container. | Consumer group reassignment and continued records; no duplicate normalized revision identity. |
| DLQ durability | Send malformed raw/normalized input. | DLQ topic has the record, `dlq-indexer` persists one deterministic failure, and provider-admin list returns it. |
| Replay audit | Submit an authorized replay request. | PostgreSQL replay request state transitions requested -> claimed -> succeeded/failed and replay Kafka record contains target contract. |
| Graceful shutdown | Send SIGTERM during active work. | Pod/container waits up to 45 seconds; no new readiness; logs show bounded shutdown, then replacement becomes ready. |

Useful commands:

```powershell
docker compose -f compose.provider.yaml stop postgres
docker compose -f compose.provider.yaml start postgres
docker compose -f compose.provider.yaml logs --tail=200 sink query
docker compose -f compose.provider.yaml stop redpanda
docker compose -f compose.provider.yaml start redpanda
docker compose -f compose.provider.yaml logs --tail=200 collector processor
kubectl get pods -l app.kubernetes.io/name=streamforge -o wide
kubectl logs deploy/streamforge-streamforge-processor --tail=200
kubectl rollout status deploy/streamforge-streamforge-sink
```

Before release, run `scripts/verify-provider.ps1`. It records static test
results and prints `SKIP` for unavailable Docker/Helm runtime tooling instead
of claiming those gates passed.
