package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("user not found")

const maxInt64 = int64(^uint64(0) >> 1)

const (
	ResetModeRemnawave   = "REMNAWAVE"
	ResetModeDays        = "DAYS"
	ResetStrategyRolling = "MONTH_ROLLING"

	EventWhitelistExhausted  = "whitelist.exhausted"
	EventWhitelistAccessSync = "whitelist.access_sync"
)

// EventOptions controls which external side effects should be queued.
// Enforcement events are only created when enforcement is enabled; exhausted
// events are also created when webhook delivery is enabled.
type EventOptions struct {
	Enforcement     bool
	Webhook         bool
	DropConnections bool
}

func (o EventOptions) Enabled() bool {
	return o.Enforcement || o.Webhook || o.DropConnections
}

type User struct {
	ID               int64
	RemnawaveID      int64
	TotalBytes       int64
	UsedBytes        int64
	ResetMode        string
	ResetDays        *int
	ResetStrategy    string
	NextResetAt      *time.Time
	PanelLastResetAt *time.Time
	PeriodStartedAt  *time.Time
	UsageBaselineAt  *time.Time
	LimitReached         bool
	ExhaustionGeneration int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type PanelState struct {
	Strategy    string
	LastResetAt *time.Time
	CreatedAt   *time.Time
}

type OutboxItem struct {
	ID                   uuid.UUID
	UserID               int64
	EventType            string
	Payload              []byte
	Attempts             int
	EnforcementDoneAt    *time.Time
	ConnectionsDroppedAt *time.Time
	ExhaustionGeneration *int64
	WebhookSentAt        *time.Time
}

type Store struct {
	db                    *pgxpool.Pool
	remnaDB               *pgxpool.Pool
	whitelistNodeIDs      []int64
	reconcileSafetyMargin time.Duration
}

func New(db, remnaDB *pgxpool.Pool, nodeIDs []int64, reconcileSafetyMargin time.Duration) *Store {
	if reconcileSafetyMargin < 0 {
		reconcileSafetyMargin = 0
	}
	return &Store{
		db:                    db,
		remnaDB:               remnaDB,
		whitelistNodeIDs:      append([]int64(nil), nodeIDs...),
		reconcileSafetyMargin: reconcileSafetyMargin,
	}
}

func (s *Store) GetUser(ctx context.Context, id int64) (User, error) {
	var u User
	err := scanUser(s.db.QueryRow(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users WHERE id=$1`, id), &u)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}
	return u, nil
}

func scanUser(row pgx.Row, u *User) error {
	return row.Scan(
		&u.ID, &u.RemnawaveID, &u.TotalBytes, &u.UsedBytes, &u.ResetMode, &u.ResetDays,
		&u.ResetStrategy, &u.NextResetAt, &u.PanelLastResetAt, &u.PeriodStartedAt,
		&u.UsageBaselineAt, &u.LimitReached, &u.ExhaustionGeneration, &u.CreatedAt, &u.UpdatedAt,
	)
}

func (s *Store) GetPanelState(ctx context.Context, remnawaveID int64) (PanelState, error) {
	var state PanelState
	err := s.remnaDB.QueryRow(ctx, `
		SELECT traffic_limit_strategy, last_traffic_reset_at, created_at
		FROM users WHERE id=$1`, remnawaveID).Scan(&state.Strategy, &state.LastResetAt, &state.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return PanelState{}, ErrNotFound
	}
	return state, err
}

func (s *Store) ListUsersForPanelSync(ctx context.Context, afterID int64, limit int) ([]User, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users
		WHERE reset_mode=$1 AND id > $2
		ORDER BY id
		LIMIT $3`, ResetModeRemnawave, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(
			&u.ID, &u.RemnawaveID, &u.TotalBytes, &u.UsedBytes, &u.ResetMode, &u.ResetDays,
			&u.ResetStrategy, &u.NextResetAt, &u.PanelLastResetAt, &u.PeriodStartedAt,
			&u.UsageBaselineAt, &u.LimitReached, &u.ExhaustionGeneration, &u.CreatedAt, &u.UpdatedAt,
		); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) SyncAllPanelState(ctx context.Context, batchSize int, opts EventOptions) (int, error) {
	if batchSize <= 0 {
		batchSize = 500
	}
	processed := 0
	lastID := int64(0)

	for {
		users, err := s.ListUsersForPanelSync(ctx, lastID, batchSize)
		if err != nil {
			return processed, err
		}
		if len(users) == 0 {
			return processed, nil
		}

		remnaIDs := make([]int64, 0, len(users))
		for _, u := range users {
			remnaIDs = append(remnaIDs, u.RemnawaveID)
		}
		rows, err := s.remnaDB.Query(ctx, `
			SELECT id, traffic_limit_strategy, last_traffic_reset_at, created_at
			FROM users WHERE id = ANY($1::bigint[])`, remnaIDs)
		if err != nil {
			return processed, err
		}
		states := make(map[int64]PanelState, len(users))
		for rows.Next() {
			var id int64
			var state PanelState
			if err := rows.Scan(&id, &state.Strategy, &state.LastResetAt, &state.CreatedAt); err != nil {
				rows.Close()
				return processed, err
			}
			states[id] = state
		}
		rowErr := rows.Err()
		rows.Close()
		if rowErr != nil {
			return processed, rowErr
		}

		for _, u := range users {
			state, ok := states[u.RemnawaveID]
			if !ok {
				// The Remnawave user may have been deleted. Leave the local record
				// intact so an operator can decide what to do with it.
				lastID = u.ID
				continue
			}
			if _, err := s.ApplyPanelSync(ctx, u.ID, state, opts); err != nil {
				return processed, fmt.Errorf("apply panel state for user %d: %w", u.ID, err)
			}
			processed++
			lastID = u.ID
		}
	}
}

func (s *Store) CreateUser(ctx context.Context, user User, opts EventOptions) error {
	return s.CreateUserWithUsage(ctx, user, opts)
}

func (s *Store) CreateUserWithUsage(ctx context.Context, user User, opts EventOptions) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO whitelist_users (
			id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
			reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
			usage_baseline_at, limit_reached, exhaustion_generation
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		user.ID, user.RemnawaveID, user.TotalBytes, user.UsedBytes, user.ResetMode, user.ResetDays,
		user.ResetStrategy, user.NextResetAt, user.PanelLastResetAt, user.PeriodStartedAt,
		user.UsageBaselineAt, user.LimitReached, user.ExhaustionGeneration)
	if err != nil {
		return err
	}
	if opts.Enforcement {
		return s.enqueueAccessSync(ctx, user.ID)
	}
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, id int64) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `DELETE FROM whitelist_users WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	// Remove queued access/enforcement/webhook work for this local user.
	if _, err := tx.Exec(ctx, `DELETE FROM outbox_events WHERE user_id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type ResetAction struct {
	Mode string
	Days *int
}

// UpdateUser changes the quota and/or reset mode. It creates an outbox event
// when the access state changes so the Remnawave whitelist squad can be synced
// asynchronously and safely.
func (s *Store) UpdateUser(ctx context.Context, id int64, totalBytes *int64, resetAction *ResetAction, opts EventOptions) (User, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback(ctx)

	var current User
	err = scanUser(tx.QueryRow(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users WHERE id=$1 FOR UPDATE`, id), &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}

	wasReached := current.LimitReached

	if totalBytes != nil {
		current.TotalBytes = *totalBytes
	}

	if resetAction != nil {
		switch resetAction.Mode {
		case ResetModeDays:
			if resetAction.Days == nil || *resetAction.Days <= 0 {
				return User{}, fmt.Errorf("limit_reset must be > 0 for DAYS mode")
			}
			now := time.Now().UTC()
			next := now.AddDate(0, 0, *resetAction.Days)
			current.ResetMode = ResetModeDays
			current.ResetDays = resetAction.Days
			current.NextResetAt = &next
			// Changing the reset schedule must not silently discard already used
			// traffic. Preserve the current accounting period; only the next
			// automatic reset is rescheduled from now.
			if current.PeriodStartedAt == nil {
				current.PeriodStartedAt = &now
			}
			if current.UsageBaselineAt == nil {
				current.UsageBaselineAt = current.PeriodStartedAt
			}
		case ResetModeRemnawave:
			current.ResetMode = ResetModeRemnawave
			current.ResetDays = nil
			current.NextResetAt = nil
		default:
			return User{}, fmt.Errorf("unknown reset mode %q", resetAction.Mode)
		}
	}

	current.LimitReached = current.TotalBytes > 0 && current.UsedBytes >= current.TotalBytes
	if !wasReached && current.LimitReached {
		if current.ExhaustionGeneration == maxInt64 {
			return User{}, fmt.Errorf("exhaustion generation overflow for user %d", current.ID)
		}
		current.ExhaustionGeneration++
	}

	if _, err := tx.Exec(ctx, `
		UPDATE whitelist_users SET total_bytes=$2, reset_mode=$3, reset_days=$4,
		       next_reset_at=$5, period_started_at=$6, usage_baseline_at=$7,
		       limit_reached=$8, exhaustion_generation=$9, updated_at=NOW()
		WHERE id=$1`, id, current.TotalBytes, current.ResetMode, current.ResetDays,
		current.NextResetAt, current.PeriodStartedAt, current.UsageBaselineAt, current.LimitReached, current.ExhaustionGeneration); err != nil {
		return User{}, err
	}

	stateChanged := current.LimitReached != wasReached
	if opts.Enabled() && stateChanged && current.LimitReached {
		if err := insertExhaustedOutboxTx(ctx, tx, current.ID, current.RemnawaveID, current.TotalBytes, current.UsedBytes, current.ExhaustionGeneration); err != nil {
			return User{}, err
		}
	}
	// Any explicit quota/reset change is also a chance to repair a manually
	// drifted squad membership. The partial unique index makes this cheap when
	// an equivalent access-sync event is already pending.
	if opts.Enforcement {
		if err := insertAccessSyncOutboxTx(ctx, tx, current.ID); err != nil {
			return User{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return User{}, err
	}
	return current, nil
}

// SwitchToRemnawave aligns the local period boundary with the panel and clears
// the local custom schedule. Usage is reconciled separately after this change.
func (s *Store) SwitchToRemnawave(ctx context.Context, id int64, state PanelState) (User, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback(ctx)

	var u User
	err = scanUser(tx.QueryRow(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users WHERE id=$1 FOR UPDATE`, id), &u)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}

	boundary := state.LastResetAt
	if boundary == nil {
		boundary = state.CreatedAt
	}
	if boundary == nil {
		now := time.Now().UTC()
		boundary = &now
	}

	u.ResetMode = ResetModeRemnawave
	u.ResetDays = nil
	u.NextResetAt = nil
	u.ResetStrategy = state.Strategy
	u.PanelLastResetAt = state.LastResetAt
	u.PeriodStartedAt = boundary
	u.UsageBaselineAt = boundary

	if _, err := tx.Exec(ctx, `
		UPDATE whitelist_users SET reset_mode=$2, reset_days=NULL, next_reset_at=NULL,
		       reset_strategy=$3, panel_last_reset_at=$4, period_started_at=$5,
		       usage_baseline_at=$6, updated_at=NOW()
		WHERE id=$1`, id, u.ResetMode, u.ResetStrategy, u.PanelLastResetAt,
		u.PeriodStartedAt, u.UsageBaselineAt); err != nil {
		return User{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, err
	}
	return u, nil
}

func (s *Store) ResetUsage(ctx context.Context, id int64, opts EventOptions) (User, error) {
	now := time.Now().UTC()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback(ctx)

	var u User
	err = scanUser(tx.QueryRow(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users WHERE id=$1 FOR UPDATE`, id), &u)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, err
	}

	u.UsedBytes = 0
	u.LimitReached = false
	u.PeriodStartedAt = &now
	u.UsageBaselineAt = &now
	if u.ResetMode == ResetModeDays && u.ResetDays != nil {
		next := now.AddDate(0, 0, *u.ResetDays)
		u.NextResetAt = &next
	}

	if _, err := tx.Exec(ctx, `
		UPDATE whitelist_users SET used_bytes=0, limit_reached=FALSE,
		       period_started_at=$2, usage_baseline_at=$3, next_reset_at=$4, updated_at=NOW()
		WHERE id=$1`, id, now, now, u.NextResetAt); err != nil {
		return User{}, err
	}

	if opts.Enforcement {
		if err := insertAccessSyncOutboxTx(ctx, tx, id); err != nil {
			return User{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, err
	}
	return u, nil
}

// ApplyUsage consumes a Remnawave Redis Stream message. Remnawave exports
// per-node user traffic deltas, so the value in records is added once per
// stream message. eventAt prevents delayed messages from an earlier local
// period from leaking into a freshly reset counter.
func (s *Store) ApplyUsage(ctx context.Context, streamID string, eventAt time.Time, nodeID int64, records map[int64]int64, opts EventOptions) error {
	if !s.isWhitelistNode(nodeID) || len(records) == 0 {
		return nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO processed_stream_messages(stream_id) VALUES($1)
		ON CONFLICT DO NOTHING RETURNING TRUE`, streamID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}

	for remnaID, delta := range records {
		if delta <= 0 {
			continue
		}

		var u User
		err = scanUser(tx.QueryRow(ctx, `
			SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
			       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
			       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
			FROM whitelist_users WHERE remnawave_id=$1 FOR UPDATE`, remnaID), &u)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}

		if u.ResetMode == ResetModeRemnawave && u.ResetStrategy == ResetStrategyRolling {
			// A rolling 30-day window cannot be represented by monotonically
			// adding stream deltas. Reconciliation computes the trailing window.
			continue
		}
		if u.PeriodStartedAt != nil && !eventAt.After(*u.PeriodStartedAt) {
			continue
		}
		if u.UsageBaselineAt != nil && !eventAt.After(*u.UsageBaselineAt) {
			continue
		}
		if delta > maxInt64-u.UsedBytes {
			return fmt.Errorf("usage overflow for user %d", u.ID)
		}

		newUsed := u.UsedBytes + delta
		wasReached := u.LimitReached
		newReached := u.TotalBytes > 0 && newUsed >= u.TotalBytes
		newGeneration := u.ExhaustionGeneration
		if !wasReached && newReached {
			if newGeneration == maxInt64 {
				return fmt.Errorf("exhaustion generation overflow for user %d", u.ID)
			}
			newGeneration++
		}

		if _, err := tx.Exec(ctx, `
			UPDATE whitelist_users SET used_bytes=$2, limit_reached=$3, exhaustion_generation=$4, updated_at=NOW() WHERE id=$1`,
			u.ID, newUsed, newReached, newGeneration); err != nil {
			return err
		}

		if opts.Enabled() && !wasReached && newReached {
			if err := insertExhaustedOutboxTx(ctx, tx, u.ID, u.RemnawaveID, u.TotalBytes, newUsed, newGeneration); err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) ApplyPanelSync(ctx context.Context, id int64, state PanelState, opts EventOptions) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var u User
	err = scanUser(tx.QueryRow(ctx, `
		SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
		       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
		       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
		FROM whitelist_users WHERE id=$1 FOR UPDATE`, id), &u)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}

	resetOccurred := false
	if u.ResetMode == ResetModeRemnawave && state.LastResetAt != nil {
		if u.PanelLastResetAt != nil && state.LastResetAt.After(*u.PanelLastResetAt) {
			resetOccurred = true
		} else if u.PanelLastResetAt == nil && u.PeriodStartedAt != nil && state.LastResetAt.After(*u.PeriodStartedAt) {
			resetOccurred = true
		}
	}

	if resetOccurred {
		u.UsedBytes = 0
		u.LimitReached = false
		u.PeriodStartedAt = state.LastResetAt
		u.UsageBaselineAt = state.LastResetAt
	}
	if state.LastResetAt != nil {
		u.PanelLastResetAt = state.LastResetAt
	}
	u.ResetStrategy = state.Strategy

	if _, err := tx.Exec(ctx, `
		UPDATE whitelist_users SET used_bytes=$2, reset_strategy=$3,
		       panel_last_reset_at=$4, period_started_at=$5, usage_baseline_at=$6,
		       limit_reached=$7, updated_at=NOW() WHERE id=$1`, id, u.UsedBytes, u.ResetStrategy,
		u.PanelLastResetAt, u.PeriodStartedAt, u.UsageBaselineAt, u.LimitReached); err != nil {
		return false, err
	}

	if opts.Enforcement && resetOccurred {
		if err := insertAccessSyncOutboxTx(ctx, tx, u.ID); err != nil {
			return false, err
		}
	}

	return resetOccurred, tx.Commit(ctx)
}

// RefreshPanelStateAndUsage pulls the current reset marker/strategy from
// Remnawave and, when the panel period or strategy changed, immediately
// reconciles local usage before an external access decision is made.
func (s *Store) RefreshPanelStateAndUsage(ctx context.Context, id int64, opts EventOptions) (bool, error) {
	u, err := s.GetUser(ctx, id)
	if err != nil {
		return false, err
	}
	if u.ResetMode != ResetModeRemnawave {
		return false, nil
	}

	state, err := s.GetPanelState(ctx, u.RemnawaveID)
	if err != nil {
		return false, err
	}

	strategyChanged := u.ResetStrategy != state.Strategy
	resetOccurred, err := s.ApplyPanelSync(ctx, id, state, opts)
	if err != nil {
		return false, err
	}
	if resetOccurred || strategyChanged {
		if _, err := s.ReconcileUserUsage(ctx, id, opts); err != nil {
			return false, err
		}
	}
	return resetOccurred || strategyChanged, nil
}

// ReconcileUserUsage rebuilds current local usage from the authoritative
// Remnawave history. It uses optimistic concurrency so a reset/top-up cannot
// be overwritten by a reconciliation query that started before that change.
//
// MONTH_ROLLING is special: the usage window is the trailing 30 days rather
// than the time since period_started_at. Stream deltas are intentionally not
// applied directly for rolling users; reconciliation is the source of truth.
func (s *Store) ReconcileUserUsage(ctx context.Context, id int64, opts EventOptions) (int64, error) {
	const maxAttempts = 5

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		u, err := s.GetUser(ctx, id)
		if err != nil {
			return 0, err
		}
		isRolling := u.ResetMode == ResetModeRemnawave && u.ResetStrategy == ResetStrategyRolling

		cutoff := time.Now().UTC().Add(-s.reconcileSafetyMargin)
		if u.UsageBaselineAt != nil && cutoff.Before(*u.UsageBaselineAt) {
			cutoff = *u.UsageBaselineAt
		}
		if u.PeriodStartedAt != nil && cutoff.Before(*u.PeriodStartedAt) {
			cutoff = *u.PeriodStartedAt
		}

		var total int64
		if isRolling {
			windowStart := cutoff.AddDate(0, 0, -30)
			if u.PeriodStartedAt != nil && u.PeriodStartedAt.After(windowStart) {
				windowStart = *u.PeriodStartedAt
			}
			err = s.remnaDB.QueryRow(ctx, `
				SELECT COALESCE(SUM(h.total_bytes),0)
				FROM nodes_user_usage_history h
				WHERE h.user_id=$1 AND h.node_id = ANY($2::bigint[])
				  AND h.created_at > $3 AND h.created_at <= $4`,
				u.RemnawaveID, s.whitelistNodeIDs, windowStart, cutoff).Scan(&total)
		} else if u.PeriodStartedAt == nil {
			err = s.remnaDB.QueryRow(ctx, `
				SELECT COALESCE(SUM(h.total_bytes),0)
				FROM nodes_user_usage_history h
				WHERE h.user_id=$1 AND h.node_id = ANY($2::bigint[]) AND h.created_at <= $3`,
				u.RemnawaveID, s.whitelistNodeIDs, cutoff).Scan(&total)
		} else {
			err = s.remnaDB.QueryRow(ctx, `
				SELECT COALESCE(SUM(h.total_bytes),0)
				FROM nodes_user_usage_history h
				WHERE h.user_id=$1 AND h.node_id = ANY($2::bigint[])
				  AND h.created_at > $3 AND h.created_at <= $4`,
				u.RemnawaveID, s.whitelistNodeIDs, *u.PeriodStartedAt, cutoff).Scan(&total)
		}
		if err != nil {
			return 0, err
		}
		if total < 0 {
			return 0, fmt.Errorf("negative reconciled usage for user %d", id)
		}

		tx, err := s.db.Begin(ctx)
		if err != nil {
			return 0, err
		}
		var current User
		err = scanUser(tx.QueryRow(ctx, `
			SELECT id, remnawave_id, total_bytes, used_bytes, reset_mode, reset_days,
			       reset_strategy, next_reset_at, panel_last_reset_at, period_started_at,
			       usage_baseline_at, limit_reached, exhaustion_generation, created_at, updated_at
			FROM whitelist_users WHERE id=$1 FOR UPDATE`, id), &current)
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return 0, ErrNotFound
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return 0, err
		}

		// A reset, mode change, or panel boundary change that happened while we
		// were reading Remnawave must win. Retry the history query against the new
		// accounting period instead of overwriting it with stale data.
		if current.RemnawaveID != u.RemnawaveID ||
			current.ResetMode != u.ResetMode ||
			current.ResetStrategy != u.ResetStrategy ||
			!sameNullableTime(current.PeriodStartedAt, u.PeriodStartedAt) ||
			!sameNullableTime(current.UsageBaselineAt, u.UsageBaselineAt) ||
			!current.UpdatedAt.Equal(u.UpdatedAt) {
			_ = tx.Rollback(ctx)
			continue
		}

		wasReached := current.LimitReached
		authoritativeUsed := total
		if !isRolling && authoritativeUsed < current.UsedBytes {
			// Remnawave history can lag behind the Redis stream. Within an already
			// established period, never move usage backwards during reconciliation;
			// the stream path will continue accounting and the next reconciliation can
			// catch up. A real reset changes period_started_at and therefore permits a
			// fresh counter. This favors a conservative (slightly overcounted) quota
			// over accidentally restoring access after a history lag.
			authoritativeUsed = current.UsedBytes
		}
		reached := current.TotalBytes > 0 && authoritativeUsed >= current.TotalBytes
		if !wasReached && reached {
			if current.ExhaustionGeneration == maxInt64 {
				_ = tx.Rollback(ctx)
				return 0, fmt.Errorf("exhaustion generation overflow for user %d", current.ID)
			}
			current.ExhaustionGeneration++
		}
		current.UsedBytes = authoritativeUsed
		current.LimitReached = reached
		current.UsageBaselineAt = &cutoff

		if _, err := tx.Exec(ctx, `
			UPDATE whitelist_users SET used_bytes=$2, limit_reached=$3,
			       exhaustion_generation=$4, usage_baseline_at=$5, updated_at=NOW() WHERE id=$1`,
			id, authoritativeUsed, reached, current.ExhaustionGeneration, cutoff); err != nil {
			_ = tx.Rollback(ctx)
			return 0, err
		}

		if opts.Enabled() && wasReached != reached {
			if reached {
				if err := insertExhaustedOutboxTx(ctx, tx, current.ID, current.RemnawaveID, current.TotalBytes, authoritativeUsed, current.ExhaustionGeneration); err != nil {
					_ = tx.Rollback(ctx)
					return 0, err
				}
			} else if opts.Enforcement {
				if err := insertAccessSyncOutboxTx(ctx, tx, current.ID); err != nil {
					_ = tx.Rollback(ctx)
					return 0, err
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return authoritativeUsed, nil
	}

	return 0, fmt.Errorf("reconcile user %d conflicted with concurrent state changes", id)
}

func sameNullableTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// IsCurrentExhaustion reports whether an exhaustion outbox item still belongs
// to the current exhaustion generation. The generation prevents an old event
// from firing again after a top-up/reset and a later re-exhaustion cycle.
func (s *Store) IsCurrentExhaustion(item *OutboxItem, u User) (bool, error) {
	if !u.LimitReached {
		return false, nil
	}
	if item.ExhaustionGeneration != nil {
		var payload struct {
			Total int64 `json:"total"`
		}
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			return false, fmt.Errorf("decode exhaustion payload: %w", err)
		}
		return *item.ExhaustionGeneration == u.ExhaustionGeneration && payload.Total == u.TotalBytes, nil
	}

	// Legacy events from before generation tracking are accepted only for the
	// initial generation and only if their snapshot still describes this quota.
	var payload struct {
		Total int64 `json:"total"`
		Used  int64 `json:"used"`
	}
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode legacy exhaustion payload: %w", err)
	}
	if u.ExhaustionGeneration != 0 {
		return false, nil
	}
	return payload.Total == u.TotalBytes && payload.Used <= u.UsedBytes, nil
}

func (s *Store) ReconcileAll(ctx context.Context, opts EventOptions, pageSize int) (int, error) {
	if pageSize <= 0 {
		pageSize = 500
	}
	processed := 0
	lastID := int64(0)

	for {
		rows, err := s.db.Query(ctx, `
			SELECT id FROM whitelist_users WHERE id > $1 ORDER BY id LIMIT $2`, lastID, pageSize)
		if err != nil {
			return processed, err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return processed, err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return processed, err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if _, err := s.ReconcileUserUsage(ctx, id, opts); err != nil {
				return processed, fmt.Errorf("reconcile user %d: %w", id, err)
			}
			processed++
			lastID = id
		}
	}
	return processed, nil
}

func (s *Store) RunDueResets(ctx context.Context, opts EventOptions) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, reset_days, next_reset_at
		FROM whitelist_users
		WHERE reset_mode=$1 AND reset_days IS NOT NULL AND next_reset_at IS NOT NULL AND next_reset_at <= NOW()
		FOR UPDATE SKIP LOCKED`, ResetModeDays)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type item struct {
		id   int64
		days int
		next time.Time
	}
	var items []item
	for rows.Next() {
		var i item
		if err := rows.Scan(&i.id, &i.days, &i.next); err != nil {
			return 0, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, item := range items {
		now := time.Now().UTC()
		// The period boundary is the scheduled reset instant, not the moment
		// when this worker happens to notice it. This prevents losing traffic
		// generated between the scheduled reset and a delayed worker run.
		resetAt := item.next
		next := resetAt.AddDate(0, 0, item.days)
		for !next.After(now) {
			resetAt = next
			next = next.AddDate(0, 0, item.days)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE whitelist_users SET used_bytes=0, limit_reached=FALSE,
			       period_started_at=$2, usage_baseline_at=$2, next_reset_at=$3, updated_at=NOW()
			WHERE id=$1`, item.id, resetAt, next); err != nil {
			return 0, err
		}
		if opts.Enforcement {
			if err := insertAccessSyncOutboxTx(ctx, tx, item.id); err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(items), nil
}

func (s *Store) Cleanup(ctx context.Context, processedRetention, outboxRetention time.Duration) error {
	if processedRetention > 0 {
		if _, err := s.db.Exec(ctx, `DELETE FROM processed_stream_messages WHERE processed_at < NOW() - ($1 * INTERVAL '1 second')`, processedRetention.Seconds()); err != nil {
			return err
		}
	}
	if outboxRetention > 0 {
		if _, err := s.db.Exec(ctx, `
			DELETE FROM outbox_events
			WHERE (sent_at IS NOT NULL AND sent_at < NOW() - ($1 * INTERVAL '1 second'))
			   OR (dead_at IS NOT NULL AND dead_at < NOW() - ($1 * INTERVAL '1 second'))`, outboxRetention.Seconds()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ClaimOutbox(ctx context.Context, claimLease time.Duration) (*OutboxItem, error) {
	row := s.db.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id FROM outbox_events
			WHERE sent_at IS NULL AND dead_at IS NULL AND next_attempt_at <= NOW()
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE outbox_events o
		SET attempts=o.attempts+1,
		    next_attempt_at=NOW() + ($1 * INTERVAL '1 second')
		FROM candidate c WHERE o.id=c.id
		RETURNING o.id,o.user_id,o.event_type,o.payload,o.attempts,o.enforcement_done_at,
		       o.connections_dropped_at,o.exhaustion_generation,o.webhook_sent_at`,
		claimLease.Seconds())

	var item OutboxItem
	if err := row.Scan(&item.ID, &item.UserID, &item.EventType, &item.Payload, &item.Attempts, &item.EnforcementDoneAt,
		&item.ConnectionsDroppedAt, &item.ExhaustionGeneration, &item.WebhookSentAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func (s *Store) MarkOutboxEnforced(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `UPDATE outbox_events SET enforcement_done_at=NOW(), last_error=NULL WHERE id=$1`, id)
	return err
}

func (s *Store) MarkOutboxWebhookSent(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `UPDATE outbox_events SET webhook_sent_at=NOW(), last_error=NULL WHERE id=$1`, id)
	return err
}

func (s *Store) MarkOutboxConnectionsDropped(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `UPDATE outbox_events SET connections_dropped_at=NOW(), last_error=NULL WHERE id=$1`, id)
	return err
}

// MarkOutboxWebhookHandled marks an exhaustion webhook as handled without sending it.
// This is used when the event became stale before delivery (for example, the user
// purchased more traffic or the quota period was reset).
func (s *Store) MarkOutboxWebhookHandled(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := s.db.Exec(ctx, `UPDATE outbox_events SET webhook_sent_at=NOW(), last_error=$2 WHERE id=$1`, id, reason)
	return err
}

func (s *Store) CompleteOutboxIfReady(ctx context.Context, id uuid.UUID, requireEnforcement, requireConnectionDrop, webhook bool) error {
	_, err := s.db.Exec(ctx, `
		UPDATE outbox_events SET sent_at=NOW(), last_error=NULL
		WHERE id=$1 AND sent_at IS NULL AND dead_at IS NULL
		  AND ($2=false OR enforcement_done_at IS NOT NULL)
		  AND ($3=false OR connections_dropped_at IS NOT NULL)
		  AND ($4=false OR webhook_sent_at IS NOT NULL)`, id, requireEnforcement, requireConnectionDrop, webhook)
	return err
}

func (s *Store) MarkOutboxFailed(ctx context.Context, id uuid.UUID, attempts int, errText string, maxRetries int, nextAttempt time.Time) error {
	if maxRetries > 0 && attempts >= maxRetries {
		_, err := s.db.Exec(ctx, `UPDATE outbox_events SET dead_at=NOW(), last_error=$2 WHERE id=$1`, id, errText)
		return err
	}
	_, err := s.db.Exec(ctx, `
		UPDATE outbox_events SET last_error=$2, next_attempt_at=$3 WHERE id=$1`, id, errText, nextAttempt)
	return err
}

// EnqueueAccessSync queues an idempotent desired-state enforcement event for one user.
func (s *Store) EnqueueAccessSync(ctx context.Context, userID int64) error {
	return s.enqueueAccessSync(ctx, userID)
}

// EnqueueAccessSyncAll queues a desired-state enforcement event for every local
// user. It is primarily used after startup so enabling enforcement can repair
// already-exhausted users and manually drifted squad memberships.
func (s *Store) EnqueueAccessSyncAll(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 500
	}
	processed := 0
	lastID := int64(0)

	for {
		rows, err := s.db.Query(ctx, `
			SELECT id
			FROM whitelist_users
			WHERE id > $1
			ORDER BY id
			LIMIT $2`, lastID, batchSize)
		if err != nil {
			return processed, err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return processed, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return processed, err
		}
		rows.Close()
		if len(ids) == 0 {
			return processed, nil
		}

		tx, err := s.db.Begin(ctx)
		if err != nil {
			return processed, err
		}
		for _, id := range ids {
			if err := insertAccessSyncOutboxTx(ctx, tx, id); err != nil {
				_ = tx.Rollback(ctx)
				return processed, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			_ = tx.Rollback(ctx)
			return processed, err
		}
		processed += len(ids)
		lastID = ids[len(ids)-1]
	}
}

func (s *Store) enqueueAccessSync(ctx context.Context, userID int64) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := insertAccessSyncOutboxTx(ctx, tx, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertAccessSyncOutboxTx(ctx context.Context, tx pgx.Tx, userID int64) error {
	eventID := uuid.New()
	payload := map[string]any{
		"event":      EventWhitelistAccessSync,
		"event_id":   eventID.String(),
		"user_id":    userID,
		"created_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(id,user_id,event_type,payload)
		VALUES($1,$2,$3,$4)
		ON CONFLICT DO NOTHING`, eventID, userID, EventWhitelistAccessSync, data)
	return err
}

func insertExhaustedOutboxTx(ctx context.Context, tx pgx.Tx, userID, remnawaveID, total, used, generation int64) error {
	eventID := uuid.New()
	payload := map[string]any{
		"event":        EventWhitelistExhausted,
		"event_id":     eventID.String(),
		"user_id":      userID,
		"remnawave_id": remnawaveID,
		"total":        total,
		"used":         used,
		"remaining":    int64(0),
		"generation":   generation,
		"occurred_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events(id,user_id,event_type,payload,exhaustion_generation)
		VALUES($1,$2,$3,$4,$5)`, eventID, userID, EventWhitelistExhausted, data, generation)
	return err
}

func (s *Store) isWhitelistNode(nodeID int64) bool {
	for _, id := range s.whitelistNodeIDs {
		if id == nodeID {
			return true
		}
	}
	return false
}
