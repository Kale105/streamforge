CREATE TABLE IF NOT EXISTS raw_events (
    id TEXT NOT NULL,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, id)
);

CREATE TABLE IF NOT EXISTS normalized_records (
    dataset TEXT NOT NULL,
    id TEXT NOT NULL,
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataset, id)
);

CREATE INDEX IF NOT EXISTS normalized_records_dataset_updated_id_idx
    ON normalized_records (dataset, updated_at, id);
