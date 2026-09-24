-- Public scalar filters use JSONB containment (data @> '{"field":value}').
-- jsonb_path_ops is smaller and faster for that containment-only contract than
-- the default jsonb_ops class. Existing revision-aware B-tree indexes cover
-- the deterministic dataset/version keyset sort paths.
CREATE INDEX IF NOT EXISTS normalized_records_data_gin
    ON normalized_records USING GIN (data jsonb_path_ops);
