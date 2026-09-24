-- API keys are stored only as SHA-256 digests. Scope rows preserve the exact
-- dataset/action grants used by the gateway authorization check.
CREATE TABLE IF NOT EXISTS api_keys (
    digest BYTEA PRIMARY KEY CHECK (octet_length(digest) = 32),
    principal_id TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ NULL,
    CHECK ((enabled AND revoked_at IS NULL) OR (NOT enabled AND revoked_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS api_key_scopes (
    digest BYTEA NOT NULL REFERENCES api_keys(digest) ON DELETE CASCADE,
    dataset TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('read', 'write')),
    PRIMARY KEY (digest, dataset, action)
);

-- This append-only log makes provisioning, rotation, and revocation auditable
-- without retaining any plaintext secret material.
CREATE TABLE IF NOT EXISTS api_key_audit (
    id BIGSERIAL PRIMARY KEY,
    digest BYTEA NOT NULL CHECK (octet_length(digest) = 32),
    event_type TEXT NOT NULL CHECK (event_type IN ('created', 'revoked')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS api_keys_active_digest_idx ON api_keys (digest) WHERE enabled;
