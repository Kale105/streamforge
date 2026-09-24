# StreamForge Helm chart

This chart deploys collector, processor, sink, DLQ indexer, replay worker,
query API, and provider-admin from one image. Kafka and PostgreSQL are external
HA dependencies; credentials are created and rotated outside the chart.

Use a non-`latest`, immutable image tag and replace the illustrative pinned
Dataset/schema package in `values.yaml` with your validated package. The chart
mounts config and schemas read-only and rolls pods when they change. Kafka
TLS/SASL settings and client material are referenced only through secrets.

Processor and sink may use HPA/PDB because they have redundant consumer-group
semantics. Other roles remain manually sized: collector ownership, replay
authority, and local API quota state make automatic scaling unsafe by default.

See `docs/operations/provider-mode.md` for prerequisites and runtime evidence.
