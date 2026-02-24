// Package migrate provides a database-agnostic migration system with
// multi-module support, dependency ordering, and distributed locking.
//
// Migrations are Go functions (not SQL files). Any Go module can register
// migrations into a shared, ordered, dependency-aware migration plan.
// Forge extensions ship their own migrations that compose with the host app.
package migrate

import "context"

// MigrateFunc is a function that performs a migration step.
// It receives a context and an Executor for running DDL/DML statements.
type MigrateFunc func(ctx context.Context, exec Executor) error //nolint:revive // MigrateFunc is the established public API name

// Migration represents a single versioned migration.
type Migration struct {
	// Name is a human-readable identifier (e.g., "create_users").
	Name string

	// Version is a timestamp-based version string (e.g., "20240115120000").
	// Migrations run in version order within dependency constraints.
	Version string

	// Group identifies the module/extension that owns this migration.
	// Set automatically when registered with a Group.
	Group string

	// Up runs the forward migration.
	Up MigrateFunc

	// Down runs the rollback.
	Down MigrateFunc

	// Comment is an optional description.
	Comment string
}

// AppliedMigration records that a migration has been applied.
type AppliedMigration struct {
	ID         int64
	Version    string
	Name       string
	Group      string
	MigratedAt string // ISO 8601 timestamp
}

// MigrationStatus describes the state of a single migration.
type MigrationStatus struct {
	Migration *Migration
	Applied   bool
	AppliedAt string // empty if not applied
}

// GroupStatus describes the state of all migrations in a group.
type GroupStatus struct {
	Name    string
	Applied []*MigrationStatus
	Pending []*MigrationStatus
}
