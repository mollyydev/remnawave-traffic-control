package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const migrationLockKey int64 = 0x77686974656c6973 // Stable advisory lock key dedicated to the Whitelists service.

// RunMigrations applies embedded SQL migrations exactly once. A transaction-
// scoped advisory lock makes startup safe when multiple service replicas are
// started at the same time.
func RunMigrations(ctx context.Context, db *pgxpool.Pool) error {
	entries, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(entries)

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	if _, err := tx.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT NULL`); err != nil {
		return fmt.Errorf("add migration checksum column: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	for _, path := range entries {
		version, name, err := migrationMeta(path)
		if err != nil {
			return err
		}

		sqlBytes, err := migrationFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}
		checksumBytes := sha256.Sum256(sqlBytes)
		checksum := hex.EncodeToString(checksumBytes[:])

		var storedName string
		var storedChecksum *string
		err = tx.QueryRow(ctx, `SELECT name, checksum FROM schema_migrations WHERE version=$1`, version).Scan(&storedName, &storedChecksum)
		if err == nil {
			if storedName != name {
				return fmt.Errorf("migration version %d name changed: database=%q file=%q", version, storedName, name)
			}
			if storedChecksum != nil && *storedChecksum != checksum {
				return fmt.Errorf("migration %s checksum mismatch: database=%s file=%s", path, *storedChecksum, checksum)
			}
			if storedChecksum == nil {
				if _, err := tx.Exec(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, version, checksum); err != nil {
					return fmt.Errorf("backfill migration checksum %s: %w", path, err)
				}
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read migration metadata %s: %w", path, err)
		}

		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", path, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, version, name, checksum); err != nil {
			return fmt.Errorf("record migration %s: %w", path, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func migrationMeta(path string) (int64, string, error) {
	base := filepath.Base(path)
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 || !strings.HasSuffix(parts[1], ".sql") {
		return 0, "", fmt.Errorf("invalid migration filename %q", base)
	}
	version, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || version <= 0 {
		return 0, "", fmt.Errorf("invalid migration version in %q", base)
	}
	name := strings.TrimSuffix(parts[1], ".sql")
	if name == "" {
		return 0, "", fmt.Errorf("invalid migration name in %q", base)
	}
	return version, name, nil
}
