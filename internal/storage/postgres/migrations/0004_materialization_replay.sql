ALTER TABLE materialization_ledger
    DROP CONSTRAINT IF EXISTS materialization_ledger_state_check;

ALTER TABLE materialization_ledger
    ADD CONSTRAINT materialization_ledger_state_check
    CHECK (state IN ('pending', 'succeeded', 'failed', 'replay_requested'));

ALTER TABLE materialization_ledger
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS replay_requested_at TIMESTAMPTZ;

-- Existing failed rows predate failed_at; their last state transition is the
-- best durable timestamp available for pagination and DLQ inspection.
UPDATE materialization_ledger
SET failed_at = updated_at
WHERE state = 'failed' AND failed_at IS NULL;

CREATE INDEX IF NOT EXISTS materialization_ledger_failed_keyset_idx
    ON materialization_ledger (updated_at, source, event_id, dataset, dataset_version)
    WHERE state = 'failed';

CREATE TABLE IF NOT EXISTS materialization_replay_attempts (
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    request_id TEXT NOT NULL,
	actor TEXT NOT NULL,
	reason TEXT NOT NULL,
	original_diagnostic_error TEXT NOT NULL,
	original_failed_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('requested', 'running', 'succeeded', 'failed')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    diagnostic_error TEXT,
    PRIMARY KEY (source, event_id, dataset, dataset_version, request_id),
    FOREIGN KEY (source, event_id, dataset, dataset_version)
        REFERENCES materialization_ledger(source, event_id, dataset, dataset_version)
);

CREATE INDEX IF NOT EXISTS materialization_replay_attempts_active_idx
    ON materialization_replay_attempts (source, event_id, dataset, dataset_version)
    WHERE state IN ('requested', 'running');
