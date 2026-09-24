-- Contract identity must survive a poison record even when message_value is
-- malformed and cannot be decoded as an event envelope. NULL is deliberately
-- the unknown value for pre-contract rows; do not backfill invented metadata.
ALTER TABLE provider_dead_letters
    ADD COLUMN IF NOT EXISTS expected_contract JSONB,
    ADD COLUMN IF NOT EXISTS original_contract JSONB;

ALTER TABLE provider_dead_letters
    ADD CONSTRAINT provider_dead_letters_expected_contract_object_check
        CHECK (expected_contract IS NULL OR jsonb_typeof(expected_contract) = 'object'),
    ADD CONSTRAINT provider_dead_letters_original_contract_object_check
        CHECK (original_contract IS NULL OR jsonb_typeof(original_contract) = 'object');
