ALTER TABLE whitelist_users
    ADD COLUMN IF NOT EXISTS usage_baseline_at TIMESTAMPTZ NULL;

UPDATE whitelist_users
SET usage_baseline_at = period_started_at
WHERE usage_baseline_at IS NULL
  AND period_started_at IS NOT NULL;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS user_id BIGINT NULL;

UPDATE outbox_events
SET user_id = NULLIF(payload->>'user_id', '')::BIGINT
WHERE user_id IS NULL
  AND payload ? 'user_id';

CREATE INDEX IF NOT EXISTS idx_outbox_user_id
    ON outbox_events (user_id, created_at);

ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS outbox_events_event_type_check;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_event_type_check
    CHECK (event_type IN ('whitelist.exhausted', 'whitelist.access_sync'));
