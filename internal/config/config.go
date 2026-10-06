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

	WhitelistsNodeIDs []int64

	RemnawaveURL         string
	RemnawaveToken       string
	RemnawaveHTTPTimeout time.Duration
	RemnawaveMaxRetries  int

	ReconcileSafetyMargin  time.Duration
	WorkerPollInterval     time.Duration
	OutboxClaimLease       time.Duration
	OutboxRetention        time.Duration
	OutboxMaxRetries       int
	ShutdownTimeout        time.Duration
	MigrationTimeout       time.Duration
	EnforceWhitelistAccess bool
	DropConnectionsOnLimit bool
	WhitelistSquadID       string

	WebhookEnabled    bool
	WebhookURL        string
	WebhookSecret     string
	WebhookTimeout    time.Duration
	WebhookMaxRetries int
}

func Load() (Config, error) {
	loadDotEnv()

	legacyEnforce, err := getBool("ENFORCE_WHITELIST_LIMIT", false)
	if err != nil {
		return Config{}, err
	}
	defaultEnforce, err := getBool("ENFORCE_WHITELIST_ACCESS", legacyEnforce)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPAddr:               getHTTPAddr(),
		APIKey:                 getString("API_KEY", ""),
		DatabaseURL:            getString("DATABASE_URL", ""),
		RemnawaveDatabaseURL:   getString("REMNAWAVE_DATABASE_URL", ""),
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

	cfg.DBMaxConns, err = getPositiveInt32("DB_MAX_CONNS", 4)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveDBMaxConns, err = getPositiveInt32("REMNAWAVE_DB_MAX_CONNS", 4)
	if err != nil {
		return Config{}, err
	}
	cfg.RemnawaveMaxRetries, err = getNonNegativeInt("REMNAWAVE_MAX_RETRIES", 3)
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
	cfg.ReconcileSafetyMargin, err = getNonNegativeDuration("RECONCILE_SAFETY_MARGIN", 0)
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

	return cfg, nil
}

func validHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func loadDotEnv() {
	data, err := os.ReadFile(".env")
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}

func getHTTPAddr() string {
	keys := []string{
		"HTTP_ADDR", "http_addr",
		"HTTP_ADR", "http_adr",
		"HTTP_PORT", "http_port",
		"PORT", "port",
	}
	for _, key := range keys {
		if v, ok := os.LookupEnv(key); ok {
			v = strings.TrimSpace(v)
			if v != "" {
				if !strings.Contains(v, ":") {
					return ":" + v
				}
				return v
			}
		}
	}
	return ":8080"
}

func getString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	lowerKey := strings.ToLower(key)
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 && strings.ToLower(strings.TrimSpace(parts[0])) == lowerKey {
			v := strings.TrimSpace(parts[1])
			if v != "" {
				return v
			}
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

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	lower := strings.ToLower(s)
	for _, suffix := range []string{"days", "day", "d"} {
		if strings.HasSuffix(lower, suffix) {
			numStr := strings.TrimSpace(lower[:len(lower)-len(suffix)])
			val, err := strconv.ParseFloat(numStr, 64)
			if err == nil {
				return time.Duration(val * float64(24*time.Hour)), nil
			}
		}
	}

	return time.ParseDuration(s)
}

func getNonNegativeDuration(key string, def time.Duration) (time.Duration, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	d, err := parseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s must be a non-negative duration such as 0s, 10m, or 30d", key)
	}
	return d, nil
}

func getPositiveDuration(key string, def time.Duration) (time.Duration, error) {
	v := getString(key, "")
	if v == "" {
		return def, nil
	}
	d, err := parseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 15s, 2m, or 30d", key)
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
