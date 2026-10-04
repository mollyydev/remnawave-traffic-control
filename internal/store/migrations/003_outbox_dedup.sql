-- Access sync is a desired-state operation. Keep only one pending event per
-- user before adding the unique partial index. This makes the migration safe
-- even if older versions accumulated duplicate access_sync events.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at, id) AS rn
    FROM outbox_events
    WHERE event_type = 'whitelist.access_sync'
      AND sent_at IS NULL
      AND dead_at IS NULL
      AND user_id IS NOT NULL
)
DELETE FROM outbox_events o
USING ranked r
WHERE o.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_pending_access_sync_user
    ON outbox_events (user_id)
    WHERE event_type = 'whitelist.access_sync'
      AND sent_at IS NULL
      AND dead_at IS NULL;
