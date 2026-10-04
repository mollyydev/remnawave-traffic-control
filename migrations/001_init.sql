CREATE TABLE IF NOT EXISTS whitelist_users (
    id BIGINT PRIMARY KEY,
    remnawave_id BIGINT NOT NULL UNIQUE,
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    used_bytes BIGINT NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    reset_mode TEXT NOT NULL DEFAULT 'REMNAWAVE' CHECK (reset_mode IN ('REMNAWAVE', 'DAYS')),
    reset_days INTEGER NULL CHECK (reset_days IS NULL OR reset_days > 0),
    reset_strategy TEXT NOT NULL DEFAULT 'NO_RESET',
    next_reset_at TIMESTAMPTZ NULL,
    panel_last_reset_at TIMESTAMPTZ NULL,
    period_started_at TIMESTAMPTZ NULL,
    usage_baseline_at TIMESTAMPTZ NULL,
    limit_reached BOOLEAN NOT NULL DEFAULT FALSE,
    exhaustion_generation BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_whitelist_users_reset
    ON whitelist_users (reset_mode, next_reset_at);

CREATE INDEX IF NOT EXISTS idx_whitelist_users_remnawave_id
    ON whitelist_users (remnawave_id);

CREATE TABLE IF NOT EXISTS processed_stream_messages (
    stream_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    user_id BIGINT,
    event_type TEXT NOT NULL CHECK (event_type IN ('whitelist.exhausted', 'whitelist.access_sync')),
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ NULL,
    dead_at TIMESTAMPTZ NULL,
    enforcement_done_at TIMESTAMPTZ NULL,
    connections_dropped_at TIMESTAMPTZ NULL,
    exhaustion_generation BIGINT NULL,
    webhook_sent_at TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (sent_at, dead_at, next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_outbox_user_id
    ON outbox_events (user_id, created_at);
