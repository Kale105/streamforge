-- Collector coordination uses a database-issued fencing token so a worker that
-- loses a lease cannot make progress after another worker takes over.
CREATE TABLE IF NOT EXISTS collector_assignments (
    dataset TEXT NOT NULL,
    source TEXT NOT NULL,
    partition TEXT NOT NULL,
    holder TEXT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_expires_at TIMESTAMPTZ NULL,
    PRIMARY KEY (dataset, source, partition)
);

CREATE TABLE IF NOT EXISTS collector_checkpoints (
    dataset TEXT NOT NULL,
    source TEXT NOT NULL,
    partition TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    cursor BYTEA NOT NULL CHECK (octet_length(cursor) > 0 AND octet_length(cursor) <= 65536),
    acknowledgement BYTEA NOT NULL DEFAULT ''::bytea CHECK (octet_length(acknowledgement) <= 65536),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, source, partition),
    FOREIGN KEY (dataset, source, partition) REFERENCES collector_assignments(dataset, source, partition) ON DELETE CASCADE
);
