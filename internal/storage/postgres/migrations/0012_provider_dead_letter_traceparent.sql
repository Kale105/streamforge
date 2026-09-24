-- Legacy DLQ rows have no trace correlation and remain NULL/unknown. New
-- writes are validated by the shared telemetry contract before persistence.
ALTER TABLE provider_dead_letters
    ADD COLUMN IF NOT EXISTS traceparent TEXT;

ALTER TABLE provider_dead_letters
    ADD CONSTRAINT provider_dead_letters_traceparent_length_check
        CHECK (traceparent IS NULL OR char_length(traceparent) = 55);
