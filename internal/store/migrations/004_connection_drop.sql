ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS connections_dropped_at TIMESTAMPTZ NULL;
