package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"whitelists-service/internal/api"
	"whitelists-service/internal/config"
	"whitelists-service/internal/remna"
	"whitelists-service/internal/store"
	"whitelists-service/internal/stream"
	"whitelists-service/internal/webhook"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config error", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := newPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		logger.Error("whitelists database init error", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	remnaDB, err := newPool(ctx, cfg.RemnawaveDatabaseURL, cfg.RemnawaveDBMaxConns)
	if err != nil {
		logger.Error("Remnawave database init error", "error", err)
		os.Exit(1)
	}
	defer remnaDB.Close()

	if err := pingWithTimeout(ctx, func(pctx context.Context) error { return db.Ping(pctx) }); err != nil {
		logger.Error("whitelists database ping failed", "error", err)
		os.Exit(1)
	}
	migrationCtx, cancelMigrations := context.WithTimeout(ctx, cfg.MigrationTimeout)
	if err := store.RunMigrations(migrationCtx, db); err != nil {
		cancelMigrations()
		logger.Error("whitelists database migrations failed", "error", err)
		os.Exit(1)
	}
	cancelMigrations()
	if err := pingWithTimeout(ctx, func(pctx context.Context) error { return remnaDB.Ping(pctx) }); err != nil {
		logger.Error("Remnawave database ping failed", "error", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     cfg.RedisPoolSize,
		MinIdleConns: 0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PoolTimeout:  15 * time.Second,
		MaxRetries:   3,
	})
	defer rdb.Close()
	if err := pingWithTimeout(ctx, func(pctx context.Context) error { return rdb.Ping(pctx).Err() }); err != nil {
		logger.Error("redis ping failed", "error", err)
		os.Exit(1)
	}

	eventOptions := store.EventOptions{
		Enforcement:     cfg.EnforceWhitelistAccess,
		Webhook:         cfg.WebhookEnabled,
		DropConnections: cfg.DropConnectionsOnLimit,
	}

	st := store.New(db, remnaDB, cfg.WhitelistsNodeIDs, cfg.ReconcileSafetyMargin)
	remnaClient := remna.New(cfg.RemnawaveURL, cfg.RemnawaveToken, cfg.RemnawaveHTTPTimeout, cfg.RemnawaveMaxRetries)
	whClient := webhook.New(cfg.WebhookEnabled, cfg.WebhookURL, cfg.WebhookSecret, cfg.WebhookTimeout)

	// First align all REMNAWAVE-mode users with the panel's current reset
	// marker, then rebuild current-period usage from the authoritative history.
	if _, err := st.SyncAllPanelState(ctx, cfg.RemnawaveSyncBatchSize, eventOptions); err != nil {
		logger.Error("initial panel sync failed", "error", err)
		os.Exit(1)
	}
	if count, err := st.ReconcileAll(ctx, eventOptions, cfg.ReconcilePageSize); err != nil {
		logger.Error("initial traffic reconciliation failed", "error", err)
		os.Exit(1)
	} else {
		logger.Info("initial traffic reconciliation complete", "users", count)
	}

	if cfg.EnforceWhitelistAccess {
		count, err := st.EnqueueAccessSyncAll(ctx, cfg.ReconcilePageSize)
		if err != nil {
			logger.Error("initial whitelist access sync enqueue failed", "error", err)
			os.Exit(1)
		}
		logger.Info("initial whitelist access sync queued", "users", count)
		if cfg.AccessReconcileInterval > 0 {
			go runAccessReconcileWorker(ctx, st, cfg.AccessReconcileInterval, cfg.ReconcilePageSize, logger)
		}
	}

	consumer := stream.New(
		rdb,
		st,
		cfg.RedisStream,
		cfg.RedisGroup,
		cfg.RedisConsumerName,
		cfg.RedisStartID,
		logger,
		eventOptions,
	)

	go runTrafficConsumer(ctx, consumer, logger)
	go runCustomResetWorker(ctx, st, cfg.WorkerPollInterval, eventOptions, logger)
	go runPanelSyncWorker(ctx, st, cfg.RemnawaveSyncInterval, cfg.RemnawaveSyncBatchSize, eventOptions, logger)
	go runReconcileWorker(ctx, st, cfg.ReconcileInterval, cfg.ReconcilePageSize, eventOptions, logger)
	go runOutboxWorker(ctx, st, remnaClient, whClient, cfg.WhitelistSquadID, cfg.EnforceWhitelistAccess, cfg.DropConnectionsOnLimit, cfg.WebhookEnabled, cfg.OutboxMaxRetries, cfg.OutboxClaimLease, cfg.WorkerPollInterval, logger)
	go runCleanupWorker(ctx, st, 24*time.Hour, cfg.ProcessedMessageRetention, cfg.OutboxRetention, logger)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, db, remnaDB, rdb, cfg.APIKey, eventOptions, cfg.EnforceWhitelistAccess, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP graceful shutdown failed", "error", err)
		}
	}()

	logger.Info("whitelists service started",
		"addr", cfg.HTTPAddr,
		"stream", cfg.RedisStream,
		"nodes", cfg.WhitelistsNodeIDs,
		"enforce_whitelist_access", cfg.EnforceWhitelistAccess,
		"drop_connections_on_limit", cfg.DropConnectionsOnLimit,
		"webhook_enabled", cfg.WebhookEnabled,
	)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}

func newPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	return pgxpool.NewWithConfig(ctx, cfg)
}

func pingWithTimeout(parent context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	return fn(ctx)
}

func runTrafficConsumer(ctx context.Context, consumer *stream.Consumer, logger *slog.Logger) {
	for {
		err := consumer.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		logger.Error("traffic consumer stopped; restarting", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func runAccessReconcileWorker(ctx context.Context, st *store.Store, interval time.Duration, pageSize int, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := st.EnqueueAccessSyncAll(ctx, pageSize)
			if err != nil {
				logger.Error("access reconcile enqueue failed", "error", err)
			} else if count > 0 {
				logger.Info("access reconcile queued", "users", count)
			}
		}
	}
}

func runCustomResetWorker(ctx context.Context, st *store.Store, interval time.Duration, opts store.EventOptions, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := st.RunDueResets(ctx, opts)
			if err != nil {
				logger.Error("custom reset worker failed", "error", err)
			} else if count > 0 {
				logger.Info("custom resets applied", "count", count)
			}
		}
	}
}

func runPanelSyncWorker(ctx context.Context, st *store.Store, interval time.Duration, batchSize int, opts store.EventOptions, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := st.SyncAllPanelState(ctx, batchSize, opts)
			if err != nil {
				logger.Error("panel sync worker failed", "error", err)
			} else if count > 0 {
				logger.Info("panel sync complete", "users", count)
			}
		}
	}
}

func runReconcileWorker(ctx context.Context, st *store.Store, interval time.Duration, pageSize int, opts store.EventOptions, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := st.ReconcileAll(ctx, opts, pageSize)
			if err != nil {
				logger.Error("traffic reconciliation failed", "error", err)
			} else {
				logger.Info("traffic reconciliation complete", "users", count)
			}
		}
	}
}

func runOutboxWorker(
	ctx context.Context,
	st *store.Store,
	client *remna.Client,
	wh *webhook.Client,
	squadID string,
	enforce bool,
	dropConnections bool,
	webhookEnabled bool,
	maxRetries int,
	claimLease time.Duration,
	pollInterval time.Duration,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			item, err := st.ClaimOutbox(ctx, claimLease)
			if err != nil {
				logger.Error("claim outbox failed", "error", err)
				continue
			}
			if item == nil {
				continue
			}

			if err := processOutboxItem(ctx, st, client, wh, item, squadID, enforce, dropConnections, webhookEnabled, logger); err != nil {
				maxAttempts := maxRetries
				nextAttempt := time.Now().UTC().Add(outboxBackoff(item.Attempts))
				if remna.IsPermanent(err) || webhook.IsPermanent(err) {
					maxAttempts = 1
					nextAttempt = time.Now().UTC()
				}
				if markErr := st.MarkOutboxFailed(ctx, item.ID, item.Attempts, err.Error(), maxAttempts, nextAttempt); markErr != nil {
					logger.Error("mark outbox failed", "event_id", item.ID, "error", markErr)
				}
				logger.Error("outbox event processing failed", "event_id", item.ID, "user_id", item.UserID, "attempts", item.Attempts, "error", err)
				continue
			}

			isExhausted := item.EventType == store.EventWhitelistExhausted
			requireEnforcement := enforce
			requireConnectionDrop := dropConnections && isExhausted
			requireWebhook := webhookEnabled && isExhausted
			if err := st.CompleteOutboxIfReady(ctx, item.ID, requireEnforcement, requireConnectionDrop, requireWebhook); err != nil {
				logger.Error("complete outbox failed", "event_id", item.ID, "user_id", item.UserID, "error", err)
			}
		}
	}
}

func enforceWhitelistState(ctx context.Context, st *store.Store, client *remna.Client, eventID uuid.UUID, userID int64, squadID string, logger *slog.Logger) error {
	const maxStateChecks = 3

	for attempt := 1; attempt <= maxStateChecks; attempt++ {
		u, err := st.GetUser(ctx, userID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return st.MarkOutboxEnforced(ctx, eventID)
			}
			return err
		}

		wantPresent := !u.LimitReached
		if err := client.SyncWhitelistSquad(ctx, u.RemnawaveID, squadID, wantPresent); err != nil {
			if remna.IsNotFound(err) {
				// The panel user no longer exists. There is nothing left for
				// enforcement to change, so do not retry this event forever.
				logger.Warn("Remnawave user not found; marking enforcement complete",
					"event_id", eventID, "user_id", userID, "remnawave_id", u.RemnawaveID)
				return st.MarkOutboxEnforced(ctx, eventID)
			}
			return err
		}

		latest, err := st.GetUser(ctx, userID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return st.MarkOutboxEnforced(ctx, eventID)
			}
			return err
		}

		if latest.LimitReached == u.LimitReached {
			return st.MarkOutboxEnforced(ctx, eventID)
		}

		logger.Info("whitelist quota changed during enforcement; rechecking",
			"event_id", eventID, "user_id", userID, "attempt", attempt,
			"old_limit_reached", u.LimitReached, "new_limit_reached", latest.LimitReached)
	}

	return fmt.Errorf("whitelist quota changed repeatedly during enforcement")
}

func processOutboxItem(ctx context.Context, st *store.Store, client *remna.Client, wh *webhook.Client, item *store.OutboxItem, squadID string, enforce, dropConnections, webhookEnabled bool, logger *slog.Logger) error {
	if item.EventType != store.EventWhitelistExhausted {
		if enforce && item.EnforcementDoneAt == nil {
			// A panel reset can happen between two local sync ticks. Refreshing
			// here keeps the desired access decision aligned with Remnawave before
			// we restore/remove the squad. A missing panel user is handled by the
			// normal API enforcement path below.
			if _, err := st.RefreshPanelStateAndUsage(ctx, item.UserID, store.EventOptions{Enforcement: true, Webhook: webhookEnabled && wh.Enabled(), DropConnections: dropConnections}); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if err := enforceWhitelistState(ctx, st, client, item.ID, item.UserID, squadID, logger); err != nil {
				return err
			}
		}
		return nil
	}

	// Before acting on an exhaustion event, refresh the panel's reset marker.
	// This closes the most important false-positive window: a user can reach the
	// local limit just after Remnawave reset traffic, before the periodic sync
	// worker has noticed the new panel boundary.
	u, err := st.GetUser(ctx, item.UserID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return markDeletedOutbox(ctx, st, item, enforce, dropConnections, webhookEnabled, wh)
		}
		return err
	}

	refreshOpts := store.EventOptions{
		Enforcement:     enforce,
		Webhook:         webhookEnabled && wh.Enabled(),
		DropConnections: dropConnections,
	}
	if u.ResetMode == store.ResetModeRemnawave {
		if _, err := st.RefreshPanelStateAndUsage(ctx, item.UserID, refreshOpts); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return markDeletedOutbox(ctx, st, item, enforce, dropConnections, webhookEnabled, wh)
			}
			return err
		}
	}

	u, err = st.GetUser(ctx, item.UserID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return markDeletedOutbox(ctx, st, item, enforce, dropConnections, webhookEnabled, wh)
		}
		return err
	}

	current, err := st.IsCurrentExhaustion(item, u)
	if err != nil {
		return err
	}
	if !current {
		if enforce && item.EnforcementDoneAt == nil {
			if err := st.MarkOutboxEnforced(ctx, item.ID); err != nil {
				return err
			}
		}
		if dropConnections && item.ConnectionsDroppedAt == nil {
			if err := st.MarkOutboxConnectionsDropped(ctx, item.ID); err != nil {
				return err
			}
		}
		if webhookEnabled && wh.Enabled() && item.WebhookSentAt == nil {
			if err := st.MarkOutboxWebhookHandled(ctx, item.ID, "stale exhaustion event: quota generation is no longer current"); err != nil {
				return err
			}
		}
		logger.Info("stale whitelist exhaustion event skipped", "event_id", item.ID, "user_id", item.UserID)
		return nil
	}

	// Remove the squad first. enforceWhitelistState re-reads the latest local
	// quota and repeats the decision if a top-up/reset races with this job.
	if enforce && item.EnforcementDoneAt == nil {
		if err := enforceWhitelistState(ctx, st, client, item.ID, item.UserID, squadID, logger); err != nil {
			return err
		}
		latest, err := st.GetUser(ctx, item.UserID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return markDeletedOutbox(ctx, st, item, false, dropConnections, webhookEnabled, wh)
			}
			return err
		}
		current, err := st.IsCurrentExhaustion(item, latest)
		if err != nil {
			return err
		}
		if !current {
			return nil
		}
	}

	// Dropping existing connections is optional. It is deliberately a separate
	// outbox flag so a successful drop is not repeated on webhook retries.
	if dropConnections && item.ConnectionsDroppedAt == nil {
		latest, err := st.GetUser(ctx, item.UserID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return markDeletedOutbox(ctx, st, item, false, true, webhookEnabled, wh)
			}
			return err
		}
		current, err := st.IsCurrentExhaustion(item, latest)
		if err != nil {
			return err
		}
		if !current {
			return nil
		}
		if err := client.DropUserConnections(ctx, latest.RemnawaveID); err != nil {
			return err
		}
		if err := st.MarkOutboxConnectionsDropped(ctx, item.ID); err != nil {
			return err
		}
		logger.Info("current whitelist connections dropped", "event_id", item.ID, "user_id", item.UserID, "remnawave_id", latest.RemnawaveID)
	}

	if webhookEnabled && item.WebhookSentAt == nil && wh.Enabled() {
		if !json.Valid(item.Payload) {
			return fmt.Errorf("webhook payload is invalid JSON")
		}
		latest, err := st.GetUser(ctx, item.UserID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return markDeletedOutbox(ctx, st, item, false, false, true, wh)
			}
			return err
		}
		current, err := st.IsCurrentExhaustion(item, latest)
		if err != nil {
			return err
		}
		if !current {
			return st.MarkOutboxWebhookHandled(ctx, item.ID, "stale exhaustion event before webhook delivery")
		}
		if err := wh.Send(ctx, item.ID.String(), item.Payload); err != nil {
			return err
		}
		if err := st.MarkOutboxWebhookSent(ctx, item.ID); err != nil {
			return err
		}
		logger.Info("whitelist exhausted webhook delivered", "event_id", item.ID, "user_id", item.UserID)
	}
	return nil
}

func markDeletedOutbox(ctx context.Context, st *store.Store, item *store.OutboxItem, enforce, dropConnections, webhookEnabled bool, wh *webhook.Client) error {
	if enforce && item.EnforcementDoneAt == nil {
		if err := st.MarkOutboxEnforced(ctx, item.ID); err != nil {
			return err
		}
	}
	if dropConnections && item.ConnectionsDroppedAt == nil {
		if err := st.MarkOutboxConnectionsDropped(ctx, item.ID); err != nil {
			return err
		}
	}
	if webhookEnabled && wh.Enabled() && item.WebhookSentAt == nil {
		if err := st.MarkOutboxWebhookHandled(ctx, item.ID, "user deleted before exhaustion delivery"); err != nil {
			return err
		}
	}
	return nil
}

func outboxBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	seconds := 1 << min(attempt, 9)
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func runCleanupWorker(ctx context.Context, st *store.Store, interval, processedRetention, outboxRetention time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := st.Cleanup(ctx, processedRetention, outboxRetention); err != nil {
				logger.Error("cleanup failed", "error", err)
			}
		}
	}
}
