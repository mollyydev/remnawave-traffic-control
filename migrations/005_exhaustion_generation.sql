ALTER TABLE whitelist_users
    ADD COLUMN IF NOT EXISTS exhaustion_generation BIGINT NOT NULL DEFAULT 0;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS exhaustion_generation BIGINT NULL;

-- Existing exhaustion rows predate generation tracking. They are intentionally
-- left NULL and are treated as legacy events by the worker.
CREATE INDEX IF NOT EXISTS idx_outbox_exhaustion_generation
    ON outbox_events (user_id, exhaustion_generation)
    WHERE event_type = 'whitelist.exhausted';
