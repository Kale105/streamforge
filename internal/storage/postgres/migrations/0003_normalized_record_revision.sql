ALTER TABLE normalized_records
    ADD COLUMN IF NOT EXISTS dataset_version TEXT NOT NULL DEFAULT 'legacy';
