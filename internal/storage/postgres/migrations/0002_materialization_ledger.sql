CREATE TABLE IF NOT EXISTS materialization_ledger (
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts > 0),
    diagnostic_error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    succeeded_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event_id, dataset, dataset_version),
    FOREIGN KEY (source, event_id) REFERENCES raw_events(source, id)
);

CREATE INDEX IF NOT EXISTS materialization_ledger_retry_idx
    ON materialization_ledger (state, updated_at)
    WHERE state IN ('pending', 'failed');
