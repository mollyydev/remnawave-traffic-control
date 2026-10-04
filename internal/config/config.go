package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	HTTPAddr string
	APIKey   string

	DatabaseURL          string
	RemnawaveDatabaseURL string
	DBMaxConns           int32
	RemnawaveDBMaxConns  int32

	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	RedisPoolSize     int
	RedisStream       string
	RedisGroup        string
	RedisConsumerName string
	RedisStartID      string

	WhitelistsNodeIDs []int64

	RemnawaveURL         string
	RemnawaveToken       string
	RemnawaveHTTPTimeout time.Duration
	RemnawaveMaxRetries  int

	RemnawaveSyncInterval     time.Duration
	ReconcileInterval         time.Duration
	ReconcileSafetyMargin     time.Duration
	AccessReconcileInterval   time.Duration
	WorkerPollInterval        time.Duration
	OutboxClaimLease          time.Duration
	ProcessedMessageRetention time.Duration
	OutboxRetention           time.Duration
	OutboxMaxRetries          int
	ReconcilePageSize         int
	RemnawaveSyncBatchSize    int
	ShutdownTimeout           time.Duration
	MigrationTimeout          time.Duration
	EnforceWhitelistAccess    bool
	DropConnectionsOnLimit    bool
	WhitelistSquadID          string

	WebhookEnabled    bool
	WebhookURL        string
	WebhookSecret     string
	WebhookTimeout    time.Duration
	WebhookMaxRetries int
}

func Load() (Config, error) {
	legacyEnforce, err := getBool("ENFORCE_WHITELIST_LIMIT", false)
	if err != nil {
		return Config{}, err
	}
	defaultEnforce, err := getBool("ENFORCE_WHITELIST_ACCESS", legacyEnforce)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPAddr:               getString("HTTP_ADDR", ":8080"),
		APIKey:                 getString("API_KEY", ""),
		DatabaseURL:            getString("DATABASE_URL", ""),
		RemnawaveDatabaseURL:   getString("REMNAWAVE_DATABASE_URL", ""),
		RedisAddr:              getString("REDIS_ADDR", "remnawave-redis:6379"),
		RedisPassword:          getString("REDIS_PASSWORD", ""),
		RedisStream:            getString("REDIS_STREAM", "ioraw:export:user_usage"),
		RedisGroup:             getString("REDIS_CONSUMER_GROUP", "whitelists-v1"),
		RedisConsumerName:      getString("REDIS_CONSUMER_NAME", hostname()),
		RedisStartID:           getString("REDIS_START_ID", "$"),
		RemnawaveURL:           strings.TrimRight(getString("REMNAWAVE_URL", ""), "/"),
		RemnawaveToken:         getString("REMNAWAVE_TOKEN", ""),
		EnforceWhitelistAccess: defaultEnforce,
		DropConnectionsOnLimit: false,
		WhitelistSquadID:       getString("WHITELIST_SQUAD_ID", ""),
		WebhookEnabled:         false,
		WebhookURL:             getString("WEBHOOK_URL", ""),
		WebhookSecret:          getString("WEBHOOK_SECRET", ""),
	}

	if cfg.APIKey == "" || len(cfg.APIKey) < 32 {
		return Config{}, fmt.Errorf("API_KEY is required and must be at least 32 characters")
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RemnawaveDatabaseURL == "" {
		return Config{}, fmt.Errorf("REMNAWAVE_DATABASE_URL is required")
	}
	if cfg.RemnawaveURL == "" || cfg.RemnawaveToken == "" {
		return Config{}, fmt.Errorf("REMNAWAVE_URL and REMNAWAVE_TOKEN are required")
	}
	if getString("REDIS_ADDR", "") == "" && cfg.RedisAddr == "" {
		return Config{}, fmt.Errorf("REDIS_ADDR is required")
	}
	if cfg.RedisStream == "" || cfg.RedisGroup == "" || cfg.RedisConsumerName == "" {
		return Config{}, fmt.Errorf("REDIS_STREAM, REDIS_CONSUMER_GROUP and REDIS_CONSUMER_NAME are required")
	}

	cfg.DBMaxConns, err = getPositiveInt32("DB_MAX_CONNS", 4)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveDBMaxConns, err = getPositiveInt32("REMNAWAVE_DB_MAX_CONNS", 4)
	if err != nil {
		return Config{}, err
	}
	cfg.RedisDB, err = getRangeInt("REDIS_DB", 0, 0, 15)
	if err != nil {
		return Config{}, err
	}
	cfg.RedisPoolSize, err = getPositiveInt("REDIS_POOL_SIZE", 8)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveMaxRetries, err = getNonNegativeInt("REMNAWAVE_MAX_RETRIES", 3)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconcilePageSize, err = getPositiveInt("RECONCILE_PAGE_SIZE", 250)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveSyncBatchSize, err = getPositiveInt("REMNAWAVE_SYNC_BATCH_SIZE", 500)
	if err != nil {
		return Config{}, err
	}
	legacyOutboxRetries, err := getNonNegativeInt("WEBHOOK_MAX_RETRIES", 12)
	if err != nil {
		return Config{}, err
	}
	cfg.OutboxMaxRetries, err = getNonNegativeInt("OUTBOX_MAX_RETRIES", legacyOutboxRetries)
	if err != nil {
		return Config{}, err
	}
	cfg.WebhookMaxRetries = legacyOutboxRetries

	cfg.RemnawaveHTTPTimeout, err = getPositiveDuration("REMNAWAVE_HTTP_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveSyncInterval, err = getPositiveDuration("REMNAWAVE_SYNC_INTERVAL", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconcileInterval, err = getPositiveDuration("RECONCILE_INTERVAL", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconcileSafetyMargin, err = getPositiveDuration("RECONCILE_SAFETY_MARGIN", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.AccessReconcileInterval, err = getNonNegativeDuration("ACCESS_RECONCILE_INTERVAL", 0)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkerPollInterval, err = getPositiveDuration("WORKER_POLL_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.OutboxClaimLease, err = getPositiveDuration("OUTBOX_CLAIM_LEASE", 2*time.Minute)
	if err != nil {
		return Config{}, err
	}
	cfg.ProcessedMessageRetention, err = getPositiveDuration("PROCESSED_MESSAGE_RETENTION", 35*24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	cfg.OutboxRetention, err = getPositiveDuration("OUTBOX_RETENTION", 30*24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	cfg.ShutdownTimeout, err = getPositiveDuration("SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.MigrationTimeout, err = getPositiveDuration("MIGRATION_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.WebhookTimeout, err = getPositiveDuration("WEBHOOK_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	cfg.WhitelistsNodeIDs, err = getPositiveInt64List("WHITELISTS_NODE_IDS")
	if err != nil {
		return Config{}, err
	}
	if len(cfg.WhitelistsNodeIDs) == 0 {
		return Config{}, fmt.Errorf("WHITELISTS_NODE_IDS must contain at least one node id")
	}

	if cfg.EnforceWhitelistAccess {
		if cfg.WhitelistSquadID == "" {
			return Config{}, fmt.Errorf("WHITELIST_SQUAD_ID is required when whitelist access enforcement is enabled")
		}
		if _, err := uuid.Parse(cfg.WhitelistSquadID); err != nil {
			return Config{}, fmt.Errorf("WHITELIST_SQUAD_ID must be a valid UUID: %w", err)
		}
	}

	cfg.DropConnectionsOnLimit, err = getBool("DROP_CONNECTIONS_ON_LIMIT", false)
	if err != nil {
		return Config{}, err
	}
	if cfg.DropConnectionsOnLimit && !cfg.EnforceWhitelistAccess {
		return Config{}, fmt.Errorf("DROP_CONNECTIONS_ON_LIMIT=true requires ENFORCE_WHITELIST_ACCESS=true")
	}

	cfg.WebhookEnabled, err = getBool("WEBHOOK_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	if cfg.WebhookEnabled {
		if cfg.WebhookURL == "" {
			return Config{}, fmt.Errorf("WEBHOOK_URL is required when WEBHOOK_ENABLED=true")
		}
		if !validHTTPURL(cfg.WebhookURL) {
			return Config{}, fmt.Errorf("WEBHOOK_URL must be a valid http(s) URL")
		}
		if len(cfg.WebhookSecret) < 32 {
			return Config{}, fmt.Errorf("WEBHOOK_SECRET must be at least 32 characters when WEBHOOK_ENABLED=true")
		}
	}
	if cfg.RedisStartID == "" {
		return Config{}, fmt.Errorf("REDIS_START_ID cannot be empty")
	}

	return cfg, nil
}

func validHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func getString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return def
}

func getBool(key string, def bool) (bool, error) {
	v := strings.ToLower(getString(key, ""))
	if v == "" {
		return def, nil
	}
	switch v {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be one of: true,false,1,0,yes,no,on,off", key)
	}
}

func getPositiveInt(key string, def int) (int, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func getNonNegativeInt(key string, def int) (int, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func getRangeInt(key string, def, min, max int) (int, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max)
	}
	return n, nil
}

func getPositiveInt32(key string, def int32) (int32, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive 32-bit integer", key)
	}
	return int32(n), nil
}

func getNonNegativeDuration(key string, def time.Duration) (time.Duration, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s must be a non-negative duration such as 0s or 10m", key)
	}
	return d, nil
}

func getPositiveDuration(key string, def time.Duration) (time.Duration, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 15s or 2m", key)
	}
	return d, nil
}

func getPositiveInt64List(key string) ([]int64, error) {
	v := getString(key, "")
	if v == "" {
		return nil, nil
	}
	parts := strings.Split(v, ",")
	result := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%s contains an empty node id", key)
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%s contains invalid node id %q", key, part)
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		result = append(result, n)
	}
	return result, nil
}

func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "whitelists-worker"
}
