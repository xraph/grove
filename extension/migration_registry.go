package extension

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/grove"
	"github.com/xraph/grove/migrate"
)

// MigrationRegistry collects migration groups contributed by every extension and
// runs them as a single ordered pass per database. Putting every group into one
// orchestrator lets grove's topological sort resolve cross-extension DependsOn
// declarations (which fail when each extension runs its own orchestrator), and
// collapses N per-extension lock cycles into one acquire/release per database.
type MigrationRegistry struct {
	mu          sync.Mutex
	dbs         map[string]*dbContribution
	lockTimeout time.Duration
}

// dbEntry pairs a database key with its contribution, used by snapshot-driven loops.
type dbEntry struct {
	key string
	c   *dbContribution
}

type dbContribution struct {
	drv    grove.GroveDriver
	groups []*migrate.Group
}

// RegistryOption configures a MigrationRegistry.
type RegistryOption func(*MigrationRegistry)

// WithRegistryLockTimeout sets the lock-wait budget for every orchestrator the
// registry runs. 0 uses migrate.DefaultLockTimeout.
func WithRegistryLockTimeout(d time.Duration) RegistryOption {
	return func(r *MigrationRegistry) { r.lockTimeout = d }
}

// NewMigrationRegistry creates an empty registry.
func NewMigrationRegistry(opts ...RegistryOption) *MigrationRegistry {
	r := &MigrationRegistry{dbs: make(map[string]*dbContribution)}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Contribute adds migration groups for a database. dbKey "" is the default
// database. The first driver contributed for a dbKey wins; later contributions
// for the same dbKey append their groups (callers must contribute the same
// driver instance for a given database).
func (r *MigrationRegistry) Contribute(dbKey string, drv grove.GroveDriver, groups ...*migrate.Group) {
	if len(groups) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.dbs[dbKey]
	if !ok {
		c = &dbContribution{drv: drv}
		r.dbs[dbKey] = c
	}
	if c.drv == nil {
		c.drv = drv
	}
	c.groups = append(c.groups, groups...)
}

// snapshot returns all contributed databases in deterministic (alphabetical key)
// order. "" sorts first. RollbackAll iterates the slice in reverse.
func (r *MigrationRegistry) snapshot() []dbEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.dbs))
	for k := range r.dbs {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic; "" sorts first
	out := make([]dbEntry, 0, len(keys))
	for _, k := range keys {
		out = append(out, dbEntry{k, r.dbs[k]})
	}
	return out
}

func (r *MigrationRegistry) orchestrator(c *dbContribution) (*migrate.Orchestrator, error) {
	exec, err := migrate.NewExecutorFor(c.drv)
	if err != nil {
		return nil, fmt.Errorf("grove: create migration executor: %w", err)
	}
	orch := migrate.NewOrchestrator(exec, c.groups...)
	if r.lockTimeout != 0 {
		d := r.lockTimeout
		if d < 0 {
			d = 0 // negative configured timeout → wait until context deadline (migrate semantics: 0 = no timeout)
		}
		orch.SetLockTimeout(d)
	}
	return orch, nil
}

// RunAll runs one ordered migration pass per contributed database.
func (r *MigrationRegistry) RunAll(ctx context.Context) (*forge.MigrationResult, error) {
	result := &forge.MigrationResult{}
	for _, e := range r.snapshot() {
		orch, err := r.orchestrator(e.c)
		if err != nil {
			return result, err
		}
		mr, err := orch.Migrate(ctx)
		if err != nil {
			return result, fmt.Errorf("grove: migrate %q: %w", e.key, err)
		}
		result.Applied += len(mr.Applied)
		for _, m := range mr.Applied {
			result.Names = append(result.Names, label(e.key, m.Group, m.Name))
		}
	}
	return result, nil
}

// RollbackAll rolls back the last batch per database, databases in reverse order.
func (r *MigrationRegistry) RollbackAll(ctx context.Context) (*forge.MigrationResult, error) {
	result := &forge.MigrationResult{}
	snap := r.snapshot()
	for i := len(snap) - 1; i >= 0; i-- {
		e := snap[i]
		orch, err := r.orchestrator(e.c)
		if err != nil {
			return result, err
		}
		mr, err := orch.Rollback(ctx)
		if err != nil {
			return result, fmt.Errorf("grove: rollback %q: %w", e.key, err)
		}
		result.RolledBack += len(mr.Rollback)
		for _, m := range mr.Rollback {
			result.Names = append(result.Names, label(e.key, m.Group, m.Name))
		}
	}
	return result, nil
}

// StatusAll returns migration status across all contributed databases.
func (r *MigrationRegistry) StatusAll(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	var all []*forge.MigrationGroupInfo
	for _, e := range r.snapshot() {
		orch, err := r.orchestrator(e.c)
		if err != nil {
			return nil, err
		}
		statuses, err := orch.Status(ctx)
		if err != nil {
			return nil, fmt.Errorf("grove: migration status %q: %w", e.key, err)
		}
		for _, gs := range convertGroupStatuses(statuses) {
			if e.key != "" {
				gs.Name = e.key + ":" + gs.Name
			}
			all = append(all, gs)
		}
	}
	return all, nil
}

func label(dbKey, group, name string) string {
	if dbKey == "" {
		return group + "/" + name
	}
	return dbKey + ":" + group + "/" + name
}
