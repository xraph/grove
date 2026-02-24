// Package sqlitemigrate provides a SQLite-specific migration executor
// for the Grove migration system.
package sqlitemigrate

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

func init() {
	migrate.RegisterExecutor("sqlite", func(drv any) migrate.Executor {
		return New(drv.(driver.Driver))
	})
}

const (
	migrationTableName = "grove_migrations"
	lockTableName      = "grove_migration_locks"
)

// Executor implements migrate.Executor for SQLite.
type Executor struct {
	drv driver.Driver
}

var _ migrate.Executor = (*Executor)(nil)

// New creates a new SQLite migration executor.
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
func (e *Executor) EnsureMigrationTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		version     TEXT NOT NULL,
		name        TEXT NOT NULL,
		"group"     TEXT NOT NULL,
		migrated_at TEXT NOT NULL DEFAULT (DATETIME('now')),
		UNIQUE(version, "group")
	)`, migrationTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// EnsureLockTable creates the grove_migration_locks table if it doesn't exist.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
		locked_at   TEXT,
		locked_by   TEXT
	)`, lockTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// AcquireLock attempts to acquire the migration lock using a table-based
// approach. SQLite is single-writer by nature, so this provides a simple
// cooperative lock via INSERT OR IGNORE + UPDATE.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	// Try to insert the lock row. If it already exists, this is a no-op.
	insertQuery := fmt.Sprintf(
		`INSERT OR IGNORE INTO %s (id, locked_at, locked_by) VALUES (1, NULL, NULL)`,
		lockTableName)
	_, err := e.drv.Exec(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("sqlitemigrate: ensure lock row: %w", err)
	}

	// Try to acquire the lock by updating the row only if it's not locked.
	updateQuery := fmt.Sprintf(
		`UPDATE %s SET locked_at = DATETIME('now'), locked_by = ? WHERE id = 1 AND locked_at IS NULL`,
		lockTableName)
	res, err := e.drv.Exec(ctx, updateQuery, lockedBy)
	if err != nil {
		return fmt.Errorf("sqlitemigrate: acquire lock: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlitemigrate: check lock acquisition: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("sqlitemigrate: migration lock is held by another process")
	}

	return nil
}

// ReleaseLock releases the migration lock.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	query := fmt.Sprintf(
		`UPDATE %s SET locked_at = NULL, locked_by = NULL WHERE id = 1`,
		lockTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// ListApplied returns all migrations that have been applied, ordered by
// id ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	query := fmt.Sprintf(
		`SELECT id, version, name, "group", migrated_at FROM %s ORDER BY id ASC`,
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
			return nil, fmt.Errorf("sqlitemigrate: scan applied: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// RecordApplied records that a migration was successfully applied.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (version, name, "group") VALUES (?, ?, ?)`,
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, m.Version, m.Name, m.Group)
	return err
}

// RemoveApplied removes the record of an applied migration (for rollback).
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE version = ? AND "group" = ?`,
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, m.Version, m.Group)
	return err
}
