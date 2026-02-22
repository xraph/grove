package migrate

import (
	"context"

	"github.com/xraph/grove/driver"
)

// Executor is the interface that driver-specific migration executors implement.
// It provides methods for running DDL/DML within migrations and managing
// the migration version table and lock.
type Executor interface {
	// Exec executes a SQL statement that does not return rows.
	Exec(ctx context.Context, query string, args ...any) (driver.Result, error)

	// Query executes a SQL statement that returns rows.
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)

	// EnsureMigrationTable creates the migration tracking table if it doesn't exist.
	EnsureMigrationTable(ctx context.Context) error

	// EnsureLockTable creates the migration lock table if it doesn't exist.
	EnsureLockTable(ctx context.Context) error

	// AcquireLock attempts to acquire the distributed migration lock.
	// Returns an error if the lock is held by another process.
	AcquireLock(ctx context.Context, lockedBy string) error

	// ReleaseLock releases the distributed migration lock.
	ReleaseLock(ctx context.Context) error

	// ListApplied returns all migrations that have been applied.
	ListApplied(ctx context.Context) ([]*AppliedMigration, error)

	// RecordApplied records that a migration was successfully applied.
	RecordApplied(ctx context.Context, m *Migration) error

	// RemoveApplied removes the record of an applied migration (for rollback).
	RemoveApplied(ctx context.Context, m *Migration) error
}
