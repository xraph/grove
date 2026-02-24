// Package clickhousemigrate provides a ClickHouse-specific migration executor
// for the Grove migration system.
package clickhousemigrate

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

const (
	migrationTableName = "grove_migrations"
	lockTableName      = "grove_migration_locks"
)

// Executor implements migrate.Executor for ClickHouse.
type Executor struct {
	drv driver.Driver
}

var _ migrate.Executor = (*Executor)(nil)

// New creates a new ClickHouse migration executor.
func New(drv driver.Driver) *Executor {
	return &Executor{drv: drv}
}

// Exec executes a SQL statement that does not return rows.
func (e *Executor) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return e.drv.Exec(ctx, query, args...)
}

// Query executes a SQL statement that returns rows.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return e.drv.Query(ctx, query, args...)
}

// EnsureMigrationTable creates the grove_migrations table if it doesn't exist.
// Uses MergeTree engine with ORDER BY (id) as required by ClickHouse.
func (e *Executor) EnsureMigrationTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          Int64,
		version     String,
		name        String,
		`+"`group`"+`     String,
		migrated_at DateTime64(3) DEFAULT now64(3)
	) ENGINE = MergeTree()
	ORDER BY (id)`, migrationTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// EnsureLockTable creates the grove_migration_locks table if it doesn't exist.
// Uses ReplacingMergeTree so that we can "upsert" lock rows.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          Int64 DEFAULT 1,
		locked_at   Nullable(DateTime64(3)),
		locked_by   Nullable(String)
	) ENGINE = ReplacingMergeTree()
	ORDER BY (id)`, lockTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// AcquireLock attempts to acquire the migration lock using a table-based
// approach. ClickHouse does not support INSERT OR IGNORE, so we use
// INSERT ... SELECT WHERE NOT EXISTS.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	// Insert the lock row if it does not already exist.
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (id, locked_at, locked_by) SELECT 1, NULL, NULL WHERE NOT EXISTS (SELECT 1 FROM %s WHERE id = 1)`,
		lockTableName, lockTableName)
	_, err := e.drv.Exec(ctx, insertQuery)
	if err != nil {
		// Ignore errors if the row already exists.
		_ = err
	}

	// Try to acquire the lock by inserting a new version of the row
	// (ReplacingMergeTree will keep the latest version).
	// First check if the lock is currently held.
	checkQuery := fmt.Sprintf(
		`SELECT count() FROM %s FINAL WHERE id = 1 AND locked_at IS NOT NULL`,
		lockTableName)
	var count int64
	row := e.drv.QueryRow(ctx, checkQuery)
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("clickhousemigrate: check lock: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("clickhousemigrate: migration lock is held by another process")
	}

	// Acquire the lock by inserting a new row version.
	acquireQuery := fmt.Sprintf(
		`INSERT INTO %s (id, locked_at, locked_by) VALUES (1, now64(3), ?)`,
		lockTableName)
	_, err = e.drv.Exec(ctx, acquireQuery, lockedBy)
	if err != nil {
		return fmt.Errorf("clickhousemigrate: acquire lock: %w", err)
	}

	return nil
}

// ReleaseLock releases the migration lock.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, locked_at, locked_by) VALUES (1, NULL, NULL)`,
		lockTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// ListApplied returns all migrations that have been applied, ordered by
// id ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	query := fmt.Sprintf(
		"SELECT id, version, name, `group`, migrated_at FROM %s ORDER BY id ASC",
		migrationTableName)

	rows, err := e.drv.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []*migrate.AppliedMigration
	for rows.Next() {
		a := &migrate.AppliedMigration{}
		if err := rows.Scan(&a.ID, &a.Version, &a.Name, &a.Group, &a.MigratedAt); err != nil {
			return nil, fmt.Errorf("clickhousemigrate: scan applied: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// RecordApplied records that a migration was successfully applied.
// Uses a subquery to compute the next ID since ClickHouse does not support
// auto-increment.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	// Get the next ID.
	nextIDQuery := fmt.Sprintf(
		`SELECT coalesce(max(id), 0) + 1 FROM %s`,
		migrationTableName)
	var nextID int64
	row := e.drv.QueryRow(ctx, nextIDQuery)
	if err := row.Scan(&nextID); err != nil {
		return fmt.Errorf("clickhousemigrate: get next id: %w", err)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (id, version, name, `group`) VALUES (?, ?, ?, ?)",
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, nextID, m.Version, m.Name, m.Group)
	return err
}

// RemoveApplied removes the record of an applied migration (for rollback).
// ClickHouse uses ALTER TABLE ... DELETE for mutations.
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		"ALTER TABLE %s DELETE WHERE version = ? AND `group` = ?",
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, m.Version, m.Group)
	return err
}
