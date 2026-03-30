package migrate

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"time"
)

// MigrateResult holds the result of a Migrate or Rollback operation.
type MigrateResult struct { //nolint:revive // MigrateResult is the established public API name
	Applied  []*Migration // Migrations that were applied
	Rollback []*Migration // Migrations that were rolled back (for Rollback only)
}

// Orchestrator manages migration execution across multiple groups.
type Orchestrator struct {
	executor Executor
	groups   []*Group
}

// NewOrchestrator creates a new migration orchestrator.
func NewOrchestrator(executor Executor, groups ...*Group) *Orchestrator {
	return &Orchestrator{
		executor: executor,
		groups:   groups,
	}
}

// Migrate runs all pending migrations in dependency-resolved order.
//
// Steps:
//  1. Ensure migration and lock tables exist
//  2. Acquire distributed lock
//  3. Load already-applied migrations
//  4. Topologically sort groups by dependencies
//  5. Execute pending migrations in order
//  6. Release lock
func (o *Orchestrator) Migrate(ctx context.Context) (*MigrateResult, error) {
	if err := o.executor.EnsureMigrationTable(ctx); err != nil {
		return nil, fmt.Errorf("migrate: ensure migration table: %w", err)
	}
	if err := o.executor.EnsureLockTable(ctx); err != nil {
		return nil, fmt.Errorf("migrate: ensure lock table: %w", err)
	}

	hostname, _ := os.Hostname() //nolint:errcheck // hostname is best-effort for lock identifier
	lockedBy := fmt.Sprintf("%s:%d", hostname, os.Getpid())

	if err := o.acquireLockWithRetry(ctx, lockedBy); err != nil {
		return nil, fmt.Errorf("migrate: acquire lock: %w", err)
	}
	defer func() {
		o.executor.ReleaseLock(ctx) //nolint:errcheck // best-effort lock release in defer
	}()

	applied, err := o.executor.ListApplied(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: list applied: %w", err)
	}

	appliedSet := make(map[string]bool, len(applied))
	for _, a := range applied {
		key := a.Group + ":" + a.Version
		appliedSet[key] = true
	}

	plan, err := planMigrations(o.groups)
	if err != nil {
		return nil, err
	}

	result := &MigrateResult{}

	for _, m := range plan {
		key := m.Group + ":" + m.Version
		if appliedSet[key] {
			continue
		}

		if m.Up == nil {
			return nil, fmt.Errorf("migrate: migration %s/%s has no Up function", m.Group, m.Name)
		}

		if err := m.Up(ctx, o.executor); err != nil {
			return result, fmt.Errorf("migrate: %s/%s up failed: %w", m.Group, m.Name, err)
		}

		if err := o.executor.RecordApplied(ctx, m); err != nil {
			return result, fmt.Errorf("migrate: record %s/%s: %w", m.Group, m.Name, err)
		}

		result.Applied = append(result.Applied, m)
	}

	return result, nil
}

// Rollback rolls back the last batch of applied migrations (one per group,
// most recently applied first).
func (o *Orchestrator) Rollback(ctx context.Context) (*MigrateResult, error) {
	if err := o.executor.EnsureMigrationTable(ctx); err != nil {
		return nil, fmt.Errorf("migrate: ensure migration table: %w", err)
	}
	if err := o.executor.EnsureLockTable(ctx); err != nil {
		return nil, fmt.Errorf("migrate: ensure lock table: %w", err)
	}

	hostname, _ := os.Hostname() //nolint:errcheck // hostname is best-effort for lock identifier
	lockedBy := fmt.Sprintf("%s:%d", hostname, os.Getpid())

	if err := o.acquireLockWithRetry(ctx, lockedBy); err != nil {
		return nil, fmt.Errorf("migrate: acquire lock: %w", err)
	}
	defer func() {
		o.executor.ReleaseLock(ctx) //nolint:errcheck // best-effort lock release in defer
	}()

	applied, err := o.executor.ListApplied(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: list applied: %w", err)
	}

	if len(applied) == 0 {
		return &MigrateResult{}, nil
	}

	// Find the last applied migration.
	last := applied[len(applied)-1]

	// Find the corresponding migration definition.
	plan, err := planMigrations(o.groups)
	if err != nil {
		return nil, err
	}

	var target *Migration
	for _, m := range plan {
		if m.Group == last.Group && m.Version == last.Version {
			target = m
			break
		}
	}

	if target == nil {
		return nil, fmt.Errorf("migrate: applied migration %s/%s not found in registered groups", last.Group, last.Version)
	}

	result := &MigrateResult{}

	if target.Down != nil {
		if err := target.Down(ctx, o.executor); err != nil {
			return result, fmt.Errorf("migrate: %s/%s down failed: %w", target.Group, target.Name, err)
		}
	}

	if err := o.executor.RemoveApplied(ctx, target); err != nil {
		return result, fmt.Errorf("migrate: remove record %s/%s: %w", target.Group, target.Name, err)
	}

	result.Rollback = append(result.Rollback, target)
	return result, nil
}

// Status returns the status of all migrations across all groups.
func (o *Orchestrator) Status(ctx context.Context) ([]*GroupStatus, error) {
	if err := o.executor.EnsureMigrationTable(ctx); err != nil {
		return nil, fmt.Errorf("migrate: ensure migration table: %w", err)
	}

	applied, err := o.executor.ListApplied(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: list applied: %w", err)
	}

	appliedMap := make(map[string]*AppliedMigration, len(applied))
	for _, a := range applied {
		key := a.Group + ":" + a.Version
		appliedMap[key] = a
	}

	var statuses []*GroupStatus
	for _, g := range o.groups {
		gs := &GroupStatus{Name: g.Name()}
		for _, m := range g.Migrations() {
			key := m.Group + ":" + m.Version
			ms := &MigrationStatus{Migration: m}
			if a, ok := appliedMap[key]; ok {
				ms.Applied = true
				ms.AppliedAt = a.MigratedAt
				gs.Applied = append(gs.Applied, ms)
			} else {
				gs.Pending = append(gs.Pending, ms)
			}
		}
		statuses = append(statuses, gs)
	}

	return statuses, nil
}

// acquireLockWithRetry attempts to acquire the migration lock with
// exponential backoff. It retries only when the error is a lock-held
// error (another migration is in progress). Other errors are returned
// immediately. The total retry window is capped at 30 seconds.
func (o *Orchestrator) acquireLockWithRetry(ctx context.Context, lockedBy string) error {
	const (
		maxWait     = 30 * time.Second
		initialWait = 100 * time.Millisecond
		maxInterval = 2 * time.Second
	)

	deadline := time.Now().Add(maxWait)

	for attempt := 0; ; attempt++ {
		err := o.executor.AcquireLock(ctx, lockedBy)
		if err == nil {
			return nil
		}

		// Only retry on lock-held errors. Any other error (connection
		// failure, permission denied, etc.) is returned immediately.
		if !IsLockError(err) {
			return err
		}

		// Check if we have exceeded the time budget.
		if time.Now().After(deadline) {
			return err
		}

		// Exponential backoff with jitter (matches kv/middleware/retry.go).
		backoff := time.Duration(float64(initialWait) * math.Pow(2, float64(attempt)))
		if backoff > maxInterval {
			backoff = maxInterval
		}
		backoff = time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5)) //nolint:gosec // jitter does not need crypto-grade randomness

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}
