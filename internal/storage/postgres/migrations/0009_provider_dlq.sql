-- Provider failures are keyed by their immutable Kafka position and stage.
-- This is separate from personal-mode materialization replay: provider workers
-- need an explicit, fenced replay claim before publishing a record again.
CREATE TABLE IF NOT EXISTS provider_dead_letters (
    cluster_id TEXT NOT NULL,
    failure_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('processor', 'sink')),
    class TEXT NOT NULL CHECK (class IN ('decode', 'key', 'normalize', 'sink')),
    diagnostic TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    source_timestamp TIMESTAMPTZ NOT NULL,
    message_key BYTEA NOT NULL,
    message_value BYTEA NOT NULL,
    replay_topic TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, failure_id),
    UNIQUE (cluster_id, source_topic, source_partition, source_offset, stage)
);

CREATE INDEX IF NOT EXISTS provider_dead_letters_list_idx
    ON provider_dead_letters (cluster_id, failed_at, failure_id);

CREATE TABLE IF NOT EXISTS provider_replay_requests (
    request_id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    failure_id TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    target_revision TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('requested', 'running', 'succeeded', 'failed')),
    claim_token TEXT,
    claimed_by TEXT,
    claim_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    final_diagnostic TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    FOREIGN KEY (cluster_id, failure_id)
        REFERENCES provider_dead_letters(cluster_id, failure_id)
);

CREATE INDEX IF NOT EXISTS provider_replay_requests_claim_idx
    ON provider_replay_requests (state, claim_until, requested_at, request_id);
