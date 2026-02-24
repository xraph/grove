// Package mysqlmigrate provides a MySQL-specific migration executor
// for the Grove migration system.
package mysqlmigrate

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

const (
	migrationTableName = "grove_migrations"
	lockTableName      = "grove_migration_locks"
	// advisoryLockName is the name used for MySQL's GET_LOCK/RELEASE_LOCK.
	advisoryLockName = "grove_migration_lock"
)

// Executor implements migrate.Executor for MySQL.
type Executor struct {
	drv driver.Driver
}

var _ migrate.Executor = (*Executor)(nil)

// New creates a new MySQL migration executor.
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
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ("+
		"`id` BIGINT AUTO_INCREMENT PRIMARY KEY, "+
		"`version` VARCHAR(14) NOT NULL, "+
		"`name` VARCHAR(255) NOT NULL, "+
		"`group` VARCHAR(255) NOT NULL, "+
		"`migrated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), "+
		"UNIQUE KEY `uq_version_group` (`version`, `group`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", migrationTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// EnsureLockTable creates the grove_migration_locks table if it doesn't exist.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ("+
		"`id` INT PRIMARY KEY DEFAULT 1, "+
		"`locked_at` DATETIME(6), "+
		"`locked_by` VARCHAR(255), "+
		"CONSTRAINT `single_lock` CHECK (`id` = 1)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", lockTableName)
	_, err := e.drv.Exec(ctx, query)
	return err
}

// AcquireLock attempts to acquire the distributed migration lock using
// MySQL advisory locks (GET_LOCK) for immediate feedback.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	// Use MySQL advisory lock. GET_LOCK returns 1 if acquired, 0 if timeout.
	// Timeout of 0 means try immediately without waiting.
	row := e.drv.QueryRow(ctx, "SELECT GET_LOCK(?, 0)", advisoryLockName)
	var acquired int
	if err := row.Scan(&acquired); err != nil {
		return fmt.Errorf("mysqlmigrate: advisory lock: %w", err)
	}
	if acquired != 1 {
		return fmt.Errorf("mysqlmigrate: migration lock is held by another process")
	}

	// Record who holds the lock for debugging.
	query := fmt.Sprintf("INSERT INTO `%s` (`id`, `locked_at`, `locked_by`) "+
		"VALUES (1, NOW(6), ?) "+
		"ON DUPLICATE KEY UPDATE `locked_at` = NOW(6), `locked_by` = ?",
		lockTableName)
	_, err := e.drv.Exec(ctx, query, lockedBy, lockedBy)
	return err
}

// ReleaseLock releases the distributed migration lock.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	// Clear the lock record.
	query := fmt.Sprintf("UPDATE `%s` SET `locked_at` = NULL, `locked_by` = NULL WHERE `id` = 1",
		lockTableName)
	_, _ = e.drv.Exec(ctx, query)

	// Release the advisory lock.
	_, err := e.drv.Exec(ctx, "SELECT RELEASE_LOCK(?)", advisoryLockName)
	return err
}

// ListApplied returns all migrations that have been applied, ordered by
// id ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	query := fmt.Sprintf(
		"SELECT `id`, `version`, `name`, `group`, CAST(`migrated_at` AS CHAR) FROM `%s` ORDER BY `id` ASC",
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
			return nil, fmt.Errorf("mysqlmigrate: scan applied: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// RecordApplied records that a migration was successfully applied.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		"INSERT INTO `%s` (`version`, `name`, `group`) VALUES (?, ?, ?)",
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, m.Version, m.Name, m.Group)
	return err
}

// RemoveApplied removes the record of an applied migration (for rollback).
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		"DELETE FROM `%s` WHERE `version` = ? AND `group` = ?",
		migrationTableName)
	_, err := e.drv.Exec(ctx, query, m.Version, m.Group)
	return err
}
