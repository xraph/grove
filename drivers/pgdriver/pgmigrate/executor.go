// Package pgmigrate provides a PostgreSQL-specific migration executor
// for the Grove migration system.
package pgmigrate

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

func init() {
	migrate.RegisterExecutor("pg", func(drv any) migrate.Executor {
		return New(drv.(driver.Driver))
	})
}

const (
	migrationTableName = "grove_migrations"
	lockTableName      = "grove_migration_locks"
)

// Executor implements migrate.Executor for PostgreSQL.
//
// When the underlying driver supports [driver.ConnAcquirer], the executor
// acquires a dedicated connection in [AcquireLock] and routes ALL subsequent
// operations through it until [ReleaseLock]. This guarantees that the
// session-level pg_try_advisory_lock is acquired and released on the same
// connection, preventing advisory-lock leaks in pooled environments.
type Executor struct {
	drv       driver.Driver
	dedicated driver.DedicatedConn // non-nil while migration lock is held
}

var _ migrate.Executor = (*Executor)(nil)

// New creates a new PostgreSQL migration executor.
func New(drv driver.Driver) *Executor {
	return &Executor{drv: drv}
}

// --- routing helpers: prefer dedicated conn when available ---

func (e *Executor) exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if e.dedicated != nil {
		return e.dedicated.Exec(ctx, query, args...)
	}
	return e.drv.Exec(ctx, query, args...)
}

func (e *Executor) query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if e.dedicated != nil {
		return e.dedicated.Query(ctx, query, args...)
	}
	return e.drv.Query(ctx, query, args...)
}

func (e *Executor) queryRow(ctx context.Context, query string, args ...any) driver.Row {
	if e.dedicated != nil {
		return e.dedicated.QueryRow(ctx, query, args...)
	}
	return e.drv.QueryRow(ctx, query, args...)
}

func (e *Executor) releaseDedicated() {
	if e.dedicated != nil {
		e.dedicated.Release()
		e.dedicated = nil
	}
}

// Exec executes a SQL statement that does not return rows.
func (e *Executor) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return e.exec(ctx, query, args...)
}

// Query executes a SQL statement that returns rows.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return e.query(ctx, query, args...)
}

// EnsureMigrationTable creates the grove_migrations table if it doesn't exist.
func (e *Executor) EnsureMigrationTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          BIGSERIAL PRIMARY KEY,
		version     VARCHAR(14) NOT NULL,
		name        VARCHAR(255) NOT NULL,
		"group"     VARCHAR(255) NOT NULL,
		migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE(version, "group")
	)`, migrationTableName)
	_, err := e.exec(ctx, query)
	return err
}

// EnsureLockTable creates the grove_migration_locks table if it doesn't exist.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INT PRIMARY KEY DEFAULT 1,
		locked_at   TIMESTAMPTZ,
		locked_by   VARCHAR(255),
		CONSTRAINT single_lock CHECK (id = 1)
	)`, lockTableName)
	_, err := e.exec(ctx, query)
	return err
}

// AcquireLock attempts to acquire the distributed migration lock using
// PostgreSQL advisory locks for immediate feedback.
//
// If the driver implements [driver.ConnAcquirer], a dedicated connection is
// acquired first so that the advisory lock and all subsequent migration
// operations share the same session. This prevents lock leaks when using
// connection pools.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	// Acquire a dedicated connection if the driver supports it.
	if acq, ok := e.drv.(driver.ConnAcquirer); ok {
		conn, err := acq.AcquireConn(ctx)
		if err != nil {
			return fmt.Errorf("pgmigrate: acquire dedicated conn: %w", err)
		}
		e.dedicated = conn
	}

	// Use PostgreSQL advisory lock with a fixed key.
	// pg_try_advisory_lock returns true if lock acquired, false otherwise.
	row := e.queryRow(ctx, "SELECT pg_try_advisory_lock(1)")
	var acquired bool
	if err := row.Scan(&acquired); err != nil {
		e.releaseDedicated()
		return fmt.Errorf("pgmigrate: advisory lock: %w", err)
	}
	if !acquired {
		e.releaseDedicated()
		return fmt.Errorf("pgmigrate: %w", migrate.ErrLockHeld)
	}

	// Record who holds the lock for debugging.
	query := fmt.Sprintf(`INSERT INTO %s (id, locked_at, locked_by)
		VALUES (1, NOW(), $1)
		ON CONFLICT (id) DO UPDATE SET locked_at = NOW(), locked_by = $1`,
		lockTableName)
	if _, err := e.exec(ctx, query, lockedBy); err != nil {
		// Best-effort unlock before releasing the connection.
		_, _ = e.exec(ctx, "SELECT pg_advisory_unlock(1)") //nolint:errcheck // best-effort cleanup on error path
		e.releaseDedicated()
		return err
	}

	return nil
}

// ReleaseLock releases the distributed migration lock.
// If a dedicated connection was acquired in [AcquireLock], the advisory lock
// is released on that same connection before the connection is returned to
// the pool.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	// Clear the lock record.
	query := fmt.Sprintf(`UPDATE %s SET locked_at = NULL, locked_by = NULL WHERE id = 1`,
		lockTableName)
	_, _ = e.exec(ctx, query) //nolint:errcheck // best-effort lock record clearing

	// Release the advisory lock (on the SAME connection that acquired it).
	_, err := e.exec(ctx, "SELECT pg_advisory_unlock(1)")

	// Release the dedicated connection back to the pool.
	e.releaseDedicated()

	return err
}

// ListApplied returns all migrations that have been applied, ordered by
// migrated_at ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	query := fmt.Sprintf(
		`SELECT id, version, name, "group", migrated_at::text FROM %s ORDER BY id ASC`,
		migrationTableName)

	rows, err := e.query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []*migrate.AppliedMigration
	for rows.Next() {
		a := &migrate.AppliedMigration{}
		if err := rows.Scan(&a.ID, &a.Version, &a.Name, &a.Group, &a.MigratedAt); err != nil {
			return nil, fmt.Errorf("pgmigrate: scan applied: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// RecordApplied records that a migration was successfully applied.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (version, name, "group") VALUES ($1, $2, $3)`,
		migrationTableName)
	_, err := e.exec(ctx, query, m.Version, m.Name, m.Group)
	return err
}

// RemoveApplied removes the record of an applied migration (for rollback).
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE version = $1 AND "group" = $2`,
		migrationTableName)
	_, err := e.exec(ctx, query, m.Version, m.Group)
	return err
}
