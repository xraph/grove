package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/forge"

	"github.com/xraph/grove/migrate"
)

// Compile-time check that Extension implements forge.MigratableExtension.
var _ forge.MigratableExtension = (*Extension)(nil)

// Migrate runs all pending migrations forward.
// In single-DB mode, runs migrations from e.groups.
// In multi-DB mode, runs migrations for each named database from e.dbMigrations.
func (e *Extension) Migrate(ctx context.Context) (*forge.MigrationResult, error) {
	if e.config.DisableMigrate {
		return nil, errors.New("grove: migrations are disabled via configuration")
	}

	result := &forge.MigrationResult{}

	if e.isMultiDB() {
		if err := e.migrateMultiDB(ctx, result); err != nil {
			return result, err
		}
	} else {
		if err := e.migrateSingleDB(ctx, result); err != nil {
			return result, err
		}
	}

	return result, nil
}

// Rollback rolls back the last batch of applied migrations.
// In single-DB mode, rolls back from e.groups.
// In multi-DB mode, rolls back for each named database from e.dbMigrations.
func (e *Extension) Rollback(ctx context.Context) (*forge.MigrationResult, error) {
	if e.config.DisableMigrate {
		return nil, errors.New("grove: migrations are disabled via configuration")
	}

	result := &forge.MigrationResult{}

	if e.isMultiDB() {
		if err := e.rollbackMultiDB(ctx, result); err != nil {
			return result, err
		}
	} else {
		if err := e.rollbackSingleDB(ctx, result); err != nil {
			return result, err
		}
	}

	return result, nil
}

// MigrationStatus returns the current state of all migrations grouped by
// their owning module/extension.
func (e *Extension) MigrationStatus(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	if e.isMultiDB() {
		return e.statusMultiDB(ctx)
	}
	return e.statusSingleDB(ctx)
}

// --- Single-DB helpers ---

func (e *Extension) migrateSingleDB(ctx context.Context, result *forge.MigrationResult) error {
	if len(e.groups) == 0 {
		return nil
	}

	orch, err := e.buildOrchestrator(e.db.Driver(), e.groups)
	if err != nil {
		return err
	}

	r, err := orch.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("grove: migrate: %w", err)
	}

	result.Applied += len(r.Applied)
	for _, m := range r.Applied {
		result.Names = append(result.Names, m.Group+"/"+m.Name)
	}
	return nil
}

func (e *Extension) rollbackSingleDB(ctx context.Context, result *forge.MigrationResult) error {
	if len(e.groups) == 0 {
		return nil
	}

	orch, err := e.buildOrchestrator(e.db.Driver(), e.groups)
	if err != nil {
		return err
	}

	r, err := orch.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("grove: rollback: %w", err)
	}

	result.RolledBack += len(r.Rollback)
	for _, m := range r.Rollback {
		result.Names = append(result.Names, m.Group+"/"+m.Name)
	}
	return nil
}

func (e *Extension) statusSingleDB(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	if len(e.groups) == 0 {
		return nil, nil
	}

	orch, err := e.buildOrchestrator(e.db.Driver(), e.groups)
	if err != nil {
		return nil, err
	}

	statuses, err := orch.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("grove: migration status: %w", err)
	}

	return convertGroupStatuses(statuses), nil
}

// --- Multi-DB helpers ---

func (e *Extension) migrateMultiDB(ctx context.Context, result *forge.MigrationResult) error {
	// Run migrations for each named database that has migration groups.
	for dbName, groups := range e.dbMigrations {
		if len(groups) == 0 {
			continue
		}

		db, err := e.manager.Get(dbName)
		if err != nil {
			return fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		orch, err := e.buildOrchestrator(db.Driver(), groups)
		if err != nil {
			return fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		r, err := orch.Migrate(ctx)
		if err != nil {
			return fmt.Errorf("grove: migrate %q: %w", dbName, err)
		}

		result.Applied += len(r.Applied)
		for _, m := range r.Applied {
			result.Names = append(result.Names, dbName+":"+m.Group+"/"+m.Name)
		}
	}

	// Also run single-DB groups if present (applied to default DB).
	if len(e.groups) > 0 && e.db != nil {
		if err := e.migrateSingleDB(ctx, result); err != nil {
			return err
		}
	}

	return nil
}

func (e *Extension) rollbackMultiDB(ctx context.Context, result *forge.MigrationResult) error {
	for dbName, groups := range e.dbMigrations {
		if len(groups) == 0 {
			continue
		}

		db, err := e.manager.Get(dbName)
		if err != nil {
			return fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		orch, err := e.buildOrchestrator(db.Driver(), groups)
		if err != nil {
			return fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		r, err := orch.Rollback(ctx)
		if err != nil {
			return fmt.Errorf("grove: rollback %q: %w", dbName, err)
		}

		result.RolledBack += len(r.Rollback)
		for _, m := range r.Rollback {
			result.Names = append(result.Names, dbName+":"+m.Group+"/"+m.Name)
		}
	}

	if len(e.groups) > 0 && e.db != nil {
		if err := e.rollbackSingleDB(ctx, result); err != nil {
			return err
		}
	}

	return nil
}

func (e *Extension) statusMultiDB(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	var all []*forge.MigrationGroupInfo

	for dbName, groups := range e.dbMigrations {
		if len(groups) == 0 {
			continue
		}

		db, err := e.manager.Get(dbName)
		if err != nil {
			return nil, fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		orch, err := e.buildOrchestrator(db.Driver(), groups)
		if err != nil {
			return nil, fmt.Errorf("grove: database %q: %w", dbName, err)
		}

		statuses, err := orch.Status(ctx)
		if err != nil {
			return nil, fmt.Errorf("grove: migration status %q: %w", dbName, err)
		}

		// Prefix group names with database name for clarity.
		for _, gs := range convertGroupStatuses(statuses) {
			gs.Name = dbName + ":" + gs.Name
			all = append(all, gs)
		}
	}

	if len(e.groups) > 0 && e.db != nil {
		single, err := e.statusSingleDB(ctx)
		if err != nil {
			return nil, err
		}
		all = append(all, single...)
	}

	return all, nil
}

// --- Shared helpers ---

// buildOrchestrator creates a migration orchestrator for the given driver and groups.
func (e *Extension) buildOrchestrator(drv any, groups []*migrate.Group) (*migrate.Orchestrator, error) {
	executor, err := migrate.NewExecutorFor(drv)
	if err != nil {
		return nil, fmt.Errorf("grove: create migration executor: %w", err)
	}
	return migrate.NewOrchestrator(executor, groups...), nil
}

// convertGroupStatuses converts grove migrate.GroupStatus to forge.MigrationGroupInfo.
func convertGroupStatuses(statuses []*migrate.GroupStatus) []*forge.MigrationGroupInfo {
	result := make([]*forge.MigrationGroupInfo, len(statuses))
	for i, gs := range statuses {
		info := &forge.MigrationGroupInfo{
			Name: gs.Name,
		}
		for _, ms := range gs.Applied {
			info.Applied = append(info.Applied, &forge.MigrationInfo{
				Name:      ms.Migration.Name,
				Version:   ms.Migration.Version,
				Group:     ms.Migration.Group,
				Comment:   ms.Migration.Comment,
				Applied:   true,
				AppliedAt: ms.AppliedAt,
			})
		}
		for _, ms := range gs.Pending {
			info.Pending = append(info.Pending, &forge.MigrationInfo{
				Name:    ms.Migration.Name,
				Version: ms.Migration.Version,
				Group:   ms.Migration.Group,
				Comment: ms.Migration.Comment,
				Applied: false,
			})
		}
		result[i] = info
	}
	return result
}
