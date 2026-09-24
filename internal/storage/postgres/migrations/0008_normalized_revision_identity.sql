-- Keep each normalized dataset revision independently addressable. Existing
-- rows already have dataset_version from 0003 (or receive its legacy default),
-- so changing the key is append-only and preserves all data.
ALTER TABLE normalized_records
    DROP CONSTRAINT IF EXISTS normalized_records_pkey;

ALTER TABLE normalized_records
    ADD CONSTRAINT normalized_records_pkey PRIMARY KEY (dataset, dataset_version, id);

DROP INDEX IF EXISTS normalized_records_dataset_updated_id_idx;

CREATE INDEX normalized_records_dataset_version_updated_id_idx
    ON normalized_records (dataset, dataset_version, updated_at, id);
