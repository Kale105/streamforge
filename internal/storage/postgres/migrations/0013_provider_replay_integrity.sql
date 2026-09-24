-- Provider replay is strict same-dataset, same-revision recovery. Existing
-- cross-revision requests cannot be made safe retroactively, so terminalize
-- only unfinished legacy rows before enforcing the invariant.
ALTER TABLE provider_replay_requests
    ADD COLUMN IF NOT EXISTS dataset TEXT;

UPDATE provider_replay_requests AS r
SET dataset = f.dataset
FROM provider_dead_letters AS f
WHERE r.dataset IS NULL
  AND f.cluster_id = r.cluster_id
  AND f.failure_id = r.failure_id;

-- A missing source DLQ row is itself corrupt historical state. Keep it
-- inspectable but terminal; it must not be claimable.
UPDATE provider_replay_requests
SET state = 'failed',
    finished_at = COALESCE(finished_at, now()),
    claim_until = NULL,
    final_diagnostic = COALESCE(final_diagnostic, 'legacy replay missing immutable dataset identity')
WHERE state IN ('requested', 'running')
  AND dataset IS NULL;

UPDATE provider_replay_requests
SET state = 'failed',
    finished_at = COALESCE(finished_at, now()),
    claim_until = NULL,
    final_diagnostic = COALESCE(final_diagnostic, 'legacy cross-revision replay terminalized')
WHERE state IN ('requested', 'running')
  AND source_revision <> target_revision;

ALTER TABLE provider_replay_requests
    ALTER COLUMN dataset SET NOT NULL;

ALTER TABLE provider_replay_requests
    ADD CONSTRAINT provider_replay_requests_same_revision_check
    CHECK (source_revision = target_revision) NOT VALID;

-- Claiming always filters on the configured ownership identity. This index
-- supports requested work and reclaimable expired leases without scanning
-- another dataset or revision's queue.
CREATE INDEX IF NOT EXISTS provider_replay_requests_claim_identity_idx
    ON provider_replay_requests (cluster_id, dataset, target_revision, state, claim_until, requested_at, request_id);

-- The first DLQ observation determines stable list cursors and incident
-- timestamps. A duplicate delivery is an upsert, never a new failure.
CREATE OR REPLACE FUNCTION streamforge_provider_dead_letters_failed_at_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.failed_at IS DISTINCT FROM OLD.failed_at THEN
        RAISE EXCEPTION 'provider_dead_letters.failed_at is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS provider_dead_letters_failed_at_immutable ON provider_dead_letters;
CREATE TRIGGER provider_dead_letters_failed_at_immutable
BEFORE UPDATE ON provider_dead_letters
FOR EACH ROW
EXECUTE FUNCTION streamforge_provider_dead_letters_failed_at_immutable();
