-- Provider-mode sink receipts make Kafka redelivery idempotent without storing
-- the raw Kafka payload in PostgreSQL. The hash detects reuse of one source
-- identity for different normalized content instead of silently discarding it.
CREATE TABLE IF NOT EXISTS provider_sink_receipts (
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    record_id TEXT NOT NULL,
    message_hash BYTEA NOT NULL CHECK (octet_length(message_hash) = 32),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event_id, dataset, dataset_version)
);

CREATE INDEX IF NOT EXISTS provider_sink_receipts_applied_at_idx
    ON provider_sink_receipts (applied_at);
