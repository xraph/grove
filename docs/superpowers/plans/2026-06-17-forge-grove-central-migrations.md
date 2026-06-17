# Forge/Grove Central, Ordered Migrations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the migration crash loop and make all extensions migrate as one ordered, cross-extension-aware pass under a single lock.

**Architecture:** Two parts. **Part A** makes grove's advisory-lock wait configurable so concurrent in-process migration runs serialize instead of failing the boot. **Part B** adds an `extension.MigrationRegistry` that all extensions contribute their migration groups to, runs one grove orchestrator per database (grove's existing topo-sort then resolves cross-extension `DependsOn`), and is triggered once by a new forge opt-in `CentralMigrations` split-phase startup (Register-all → migrate → Start-all).

**Tech Stack:** Go 1.25, grove (`github.com/xraph/grove`), grove `extension` submodule (imports `github.com/xraph/forge`, DI via `github.com/xraph/vessel`), forge v1.7.x, pgx/v5.

## Global Constraints

- Go version: `go 1.25.7` (per `go.mod`).
- No `Co-Authored-By` trailers in any commit (user global rule).
- Grove root module and the `extension/` submodule are separate Go modules; `extension/` has `replace github.com/xraph/grove => ../`.
- Default behavior must not change for existing apps: Part B is opt-in via a `CentralMigrations` flag (default off).
- The advisory lock key stays `pg_advisory_lock(1)` — global per database, intentional.
- `grove_migrations` record format (keys on `(version, "group")`) must not change.
- Forge and twinos changes live in **separate repos** not checked out in this workspace. Phases 1–2 are implementable here against grove + forge v1.7.1. Phases 3–4 require checking out the forge and twinos repos respectively; they are specified precisely but executed there.

## Repo / file map

**grove (this repo) — Phases 1, 2:**
- Modify: `migrate/migrator.go` — configurable lock-wait budget.
- Create: `migrate/options.go` — `OrchestratorOption`, `DefaultLockTimeout`, `LockInspector`.
- Modify: `drivers/pgdriver/pgmigrate/executor.go` — implement `LockInfo` (lock-holder enrichment).
- Modify: `extension/config.go`, `extension/options.go` — `LockTimeout` config + option.
- Modify: `extension/migratable.go` — pass lock timeout into orchestrators.
- Create: `extension/migration_registry.go` — `MigrationRegistry`.
- Create: `extension/migration_registry_test.go`.
- Modify: `extension/extension.go` — contribute-or-self-migrate wiring.
- Test: `migrate/migrator_test.go` (extend).

**forge (separate repo) — Phase 3:**
- Modify: `app_impl.go` — opt-in split-phase startup.
- Modify: `config.go` / app config — `CentralMigrations bool`.

**twinos (separate repo) — Phase 4:**
- Enable flag; switch trove + other extensions to `Contribute`; declare cross-extension `DependsOn`.

---

## Phase 1 — Grove: configurable lock wait (Part A)

### Task 1: Make the migration lock-wait budget configurable

**Files:**
- Create: `migrate/options.go`
- Modify: `migrate/migrator.go` (struct `Orchestrator`, `NewOrchestrator`, `acquireLockWithRetry`)
- Test: `migrate/migrator_test.go`

**Interfaces:**
- Produces:
  - `migrate.DefaultLockTimeout = 5 * time.Minute`
  - `func (o *Orchestrator) SetLockTimeout(d time.Duration) *Orchestrator` — `d == 0` means wait until the context deadline.
  - `type LockInspector interface { LockInfo(ctx context.Context) (*LockInfo, error) }` (optional executor capability)
- Consumes: existing `Executor`, `ErrLockHeld`, `IsLockError`, `LockInfo` (from `migrate/lock.go`).

- [ ] **Step 1: Write the failing test**

Add to `migrate/migrator_test.go`. Use the existing in-repo fake executor pattern (search the file for the current test double; this plan assumes a `fakeExecutor` struct exists — if the existing tests use a different name, reuse that one). The test forces a lock-held result twice, then success, and asserts the orchestrator retried rather than failing:

```go
func TestAcquireLockWithRetry_WaitsThenSucceeds(t *testing.T) {
	exec := newFakeExecutor()
	exec.failLockTimes = 2 // first two AcquireLock calls return ErrLockHeld
	o := NewOrchestrator(exec).SetLockTimeout(5 * time.Second)

	if _, err := o.Migrate(context.Background()); err != nil {
		t.Fatalf("expected migrate to succeed after retries, got %v", err)
	}
	if exec.acquireCalls < 3 {
		t.Fatalf("expected at least 3 acquire attempts, got %d", exec.acquireCalls)
	}
}

func TestAcquireLockWithRetry_ZeroTimeoutWaitsUntilContext(t *testing.T) {
	exec := newFakeExecutor()
	exec.alwaysLockHeld = true
	o := NewOrchestrator(exec).SetLockTimeout(0) // wait until ctx deadline
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := o.Migrate(ctx)
	if err == nil {
		t.Fatal("expected error when context deadline elapses while lock held")
	}
}
```

If `newFakeExecutor`/fields don't exist yet, add them to the test file: a struct implementing `Executor` whose `AcquireLock` returns `fmt.Errorf("pgmigrate: %w", ErrLockHeld)` for the first `failLockTimes` calls (or always when `alwaysLockHeld`), counts `acquireCalls`, and whose `EnsureMigrationTable`/`EnsureLockTable`/`ListApplied`/`RecordApplied`/`ReleaseLock` are no-ops returning zero values.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./migrate/ -run TestAcquireLockWithRetry -v`
Expected: FAIL (compile error: `SetLockTimeout` undefined, or retries exhausted at the old 30s const not honoring the 5s budget).

- [ ] **Step 3: Create `migrate/options.go`**

```go
package migrate

import (
	"context"
	"time"
)

// DefaultLockTimeout is the default maximum time Migrate/Rollback will wait to
// acquire the migration lock before giving up. Raised from the original 30s so
// that several extensions migrating the same database in one process serialize
// and wait instead of failing the boot.
const DefaultLockTimeout = 5 * time.Minute

// OrchestratorOption configures an Orchestrator.
type OrchestratorOption func(*Orchestrator)

// WithLockTimeout sets how long to wait for the migration lock. A value of 0
// means "wait until the context deadline / cancellation".
func WithLockTimeout(d time.Duration) OrchestratorOption {
	return func(o *Orchestrator) { o.lockTimeout = d }
}

// LockInspector is an optional capability: executors that can report who holds
// the migration lock implement it, letting the orchestrator produce a
// diagnosable error when the wait budget is exhausted.
type LockInspector interface {
	LockInfo(ctx context.Context) (*LockInfo, error)
}
```

- [ ] **Step 4: Modify `migrate/migrator.go`**

Add the field + default in the struct and constructor, add the setter, and rewrite `acquireLockWithRetry` to use `o.lockTimeout`:

```go
// Orchestrator manages migration execution across multiple groups.
type Orchestrator struct {
	executor    Executor
	groups      []*Group
	lockTimeout time.Duration
}

// NewOrchestrator creates a new migration orchestrator.
func NewOrchestrator(executor Executor, groups ...*Group) *Orchestrator {
	return &Orchestrator{
		executor:    executor,
		groups:      groups,
		lockTimeout: DefaultLockTimeout,
	}
}

// SetLockTimeout overrides the lock-wait budget. 0 = wait until ctx deadline.
func (o *Orchestrator) SetLockTimeout(d time.Duration) *Orchestrator {
	o.lockTimeout = d
	return o
}
```

Rewrite `acquireLockWithRetry` (replace the `maxWait` const usage):

```go
func (o *Orchestrator) acquireLockWithRetry(ctx context.Context, lockedBy string) error {
	const (
		initialWait = 100 * time.Millisecond
		maxInterval = 2 * time.Second
	)

	var deadline time.Time
	if o.lockTimeout > 0 {
		deadline = time.Now().Add(o.lockTimeout)
	}

	for attempt := 0; ; attempt++ {
		err := o.executor.AcquireLock(ctx, lockedBy)
		if err == nil {
			return nil
		}
		if !IsLockError(err) {
			return err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return o.enrichLockError(ctx, err)
		}

		backoff := time.Duration(float64(initialWait) * math.Pow(2, float64(attempt)))
		if backoff > maxInterval {
			backoff = maxInterval
		}
		backoff = time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5)) //nolint:gosec // jitter

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// enrichLockError adds lock-holder details to a budget-exhausted lock error
// when the executor can report them.
func (o *Orchestrator) enrichLockError(ctx context.Context, err error) error {
	insp, ok := o.executor.(LockInspector)
	if !ok {
		return err
	}
	info, ierr := insp.LockInfo(context.WithoutCancel(ctx))
	if ierr != nil || info == nil || !info.Held {
		return err
	}
	return fmt.Errorf("%w (held by %s since %s)", err, info.LockedBy, info.LockedAt)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./migrate/ -run TestAcquireLockWithRetry -v`
Expected: PASS (both tests).

- [ ] **Step 6: Run the full migrate package + vet**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./migrate/... && go vet ./migrate/...`
Expected: PASS, no vet errors.

- [ ] **Step 7: Commit**

```bash
cd /Users/rexraphael/Work/xraph/forgery/grove
git add migrate/options.go migrate/migrator.go migrate/migrator_test.go
git commit -m "feat(migrate): configurable lock-wait budget (default 5m), enriched lock error"
```

### Task 2: Implement `LockInfo` on the pg executor

**Files:**
- Modify: `drivers/pgdriver/pgmigrate/executor.go`
- Test: `drivers/pgdriver/pgmigrate/executor_test.go` (create if absent; otherwise extend). If no live-DB harness exists in this package, gate the test behind the existing pattern used by `concurrent_create_test.go` (inspect it for the env-var / skip convention this repo uses) and keep the unit assertion minimal.

**Interfaces:**
- Produces: `func (e *Executor) LockInfo(ctx context.Context) (*migrate.LockInfo, error)` — satisfies `migrate.LockInspector`.
- Consumes: `lockTableName`, `e.query`/`e.queryRow` helpers (already in the file).

- [ ] **Step 1: Write the failing test**

Mirror the skip/connect convention in `drivers/pgdriver/pgmigrate/concurrent_create_test.go`. The test acquires the lock, then reads it back:

```go
func TestExecutor_LockInfo_ReportsHolder(t *testing.T) {
	exec := newTestExecutor(t) // reuse the helper concurrent_create_test.go uses
	ctx := context.Background()
	if err := exec.EnsureLockTable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exec.AcquireLock(ctx, "host:123"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.ReleaseLock(ctx) }()

	info, err := exec.LockInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Held || info.LockedBy != "host:123" {
		t.Fatalf("expected held by host:123, got %+v", info)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./drivers/pgdriver/pgmigrate/ -run TestExecutor_LockInfo -v`
Expected: FAIL (`LockInfo` undefined), or SKIP if no DB configured — if skipped, note it and proceed; the unit-level wiring is still verified by Task 1's `LockInspector` assertion.

- [ ] **Step 3: Implement `LockInfo`**

Add to `drivers/pgdriver/pgmigrate/executor.go`:

```go
// LockInfo reports the current migration lock holder recorded in the lock table.
// It satisfies migrate.LockInspector, letting the orchestrator produce a
// diagnosable error when the lock-wait budget is exhausted.
func (e *Executor) LockInfo(ctx context.Context) (*migrate.LockInfo, error) {
	query := fmt.Sprintf(
		`SELECT locked_by, locked_at::text FROM %s WHERE id = 1`, lockTableName)
	row := e.queryRow(ctx, query)
	var by, at *string
	if err := row.Scan(&by, &at); err != nil {
		// No row / not initialized: report not held rather than erroring.
		return &migrate.LockInfo{Held: false}, nil //nolint:nilerr // absence == not held
	}
	info := &migrate.LockInfo{Held: by != nil}
	if by != nil {
		info.LockedBy = *by
	}
	if at != nil {
		info.LockedAt = *at
	}
	return info, nil
}
```

Add the compile-time assertion near the top of the file:

```go
var _ migrate.LockInspector = (*Executor)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./drivers/pgdriver/pgmigrate/ -run TestExecutor_LockInfo -v`
Expected: PASS (or SKIP with no DB; the `var _ = ` assertion guarantees the interface is satisfied at compile time).

- [ ] **Step 5: Build the driver package**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go build ./drivers/pgdriver/... && go vet ./drivers/pgdriver/pgmigrate/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/rexraphael/Work/xraph/forgery/grove
git add drivers/pgdriver/pgmigrate/executor.go drivers/pgdriver/pgmigrate/executor_test.go
git commit -m "feat(pgmigrate): report lock holder via migrate.LockInspector"
```

### Task 3: Thread lock timeout from the grove extension config

**Files:**
- Modify: `extension/config.go` (add `LockTimeout`)
- Modify: `extension/options.go` (add `WithLockTimeout`)
- Modify: `extension/migratable.go` (`buildOrchestrator` applies the timeout)
- Test: `extension/migratable_test.go` (create if absent)

**Interfaces:**
- Produces:
  - `Config.LockTimeout time.Duration` (YAML key `lock_timeout`)
  - `func WithLockTimeout(d time.Duration) ExtOption`
- Consumes: `migrate.WithLockTimeout`, `migrate.NewOrchestrator`.

- [ ] **Step 1: Write the failing test**

```go
func TestBuildOrchestrator_AppliesLockTimeout(t *testing.T) {
	e := New(WithLockTimeout(7 * time.Second))
	// buildOrchestrator needs a driver + groups; use the repo's existing test
	// driver double (search extension tests for it). Assert no error and that
	// the orchestrator was constructed (non-nil).
	orch, err := e.buildOrchestrator(newTestDriver(t), []*migrate.Group{migrate.NewGroup("t")})
	if err != nil {
		t.Fatal(err)
	}
	if orch == nil {
		t.Fatal("expected orchestrator")
	}
}
```

(The timeout field is unexported on the orchestrator; this test verifies wiring compiles and runs. Behavioral coverage of the timeout itself lives in Task 1.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestBuildOrchestrator_AppliesLockTimeout -v`
Expected: FAIL (`WithLockTimeout` undefined).

- [ ] **Step 3: Add config + option + wire into `buildOrchestrator`**

In `extension/config.go`, add to the `Config` struct:

```go
	// LockTimeout caps how long migrations wait for the database migration
	// lock. 0 uses migrate.DefaultLockTimeout. Negative means wait until the
	// context deadline (maps to migrate's 0).
	LockTimeout time.Duration `yaml:"lock_timeout" json:"lock_timeout"`
```

In `extension/options.go`:

```go
// WithLockTimeout sets how long migrations wait for the migration lock.
func WithLockTimeout(d time.Duration) ExtOption {
	return func(e *Extension) { e.config.LockTimeout = d }
}
```

In `extension/migratable.go`, update `buildOrchestrator`:

```go
func (e *Extension) buildOrchestrator(drv any, groups []*migrate.Group) (*migrate.Orchestrator, error) {
	executor, err := migrate.NewExecutorFor(drv)
	if err != nil {
		return nil, fmt.Errorf("grove: create migration executor: %w", err)
	}
	orch := migrate.NewOrchestrator(executor, groups...)
	if e.config.LockTimeout != 0 {
		// Negative config => wait until context deadline (migrate's 0).
		d := e.config.LockTimeout
		if d < 0 {
			d = 0
		}
		orch.SetLockTimeout(d)
	}
	return orch, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestBuildOrchestrator_AppliesLockTimeout -v`
Expected: PASS.

- [ ] **Step 5: Build the extension module**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove/extension && go build ./... && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/rexraphael/Work/xraph/forgery/grove
git add extension/config.go extension/options.go extension/migratable.go extension/migratable_test.go
git commit -m "feat(extension): configurable migration lock timeout"
```

---

## Phase 2 — Grove: MigrationRegistry (Part B core)

### Task 4: `MigrationRegistry` — collect groups, run one ordered pass per database

**Files:**
- Create: `extension/migration_registry.go`
- Test: `extension/migration_registry_test.go`

**Interfaces:**
- Produces:
  - `type MigrationRegistry struct { ... }`
  - `func NewMigrationRegistry(opts ...RegistryOption) *MigrationRegistry`
  - `func WithRegistryLockTimeout(d time.Duration) RegistryOption`
  - `func (r *MigrationRegistry) Contribute(dbKey string, drv grove.GroveDriver, groups ...*migrate.Group)`
  - `func (r *MigrationRegistry) RunAll(ctx context.Context) (*forge.MigrationResult, error)`
  - `func (r *MigrationRegistry) RollbackAll(ctx context.Context) (*forge.MigrationResult, error)`
  - `func (r *MigrationRegistry) StatusAll(ctx context.Context) ([]*forge.MigrationGroupInfo, error)`
- Consumes: `migrate.NewExecutorFor`, `migrate.NewOrchestrator`, `migrate.WithLockTimeout`/`SetLockTimeout`, `convertGroupStatuses` (already in `extension/migratable.go`), `forge.MigrationResult`, `forge.MigrationGroupInfo`.

- [ ] **Step 1: Write the failing test**

```go
func TestMigrationRegistry_MergesGroupsAndOrders(t *testing.T) {
	r := NewMigrationRegistry()
	drv := newTestDriver(t) // reuse the extension package's driver double

	// identity contributes its group; trove depends on it (cross-extension).
	identity := migrate.NewGroup("identity/postgres")
	_ = identity.Register(&migrate.Migration{Version: "0001", Name: "users",
		Up: func(ctx context.Context, e migrate.Executor) error { return nil }})

	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	_ = trove.Register(&migrate.Migration{Version: "0001", Name: "items",
		Up: func(ctx context.Context, e migrate.Executor) error { return nil }})

	// Contributed by two different "extensions" but same dbKey.
	r.Contribute("", drv, identity)
	r.Contribute("", drv, trove)

	if _, err := r.RunAll(context.Background()); err != nil {
		t.Fatalf("RunAll should resolve cross-extension DependsOn, got %v", err)
	}
}
```

The point: this would fail today (each extension had its own orchestrator → `topoSort` errors with "depends on unknown group"). Through the registry both groups share one orchestrator and the dep resolves.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestMigrationRegistry -v`
Expected: FAIL (`NewMigrationRegistry` undefined).

- [ ] **Step 3: Implement the registry**

```go
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
	mu            sync.Mutex
	dbs           map[string]*dbContribution
	order         []string // dbKeys in first-contribution order, for deterministic runs
	lockTimeout   time.Duration
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
		r.order = append(r.order, dbKey)
	}
	if c.drv == nil {
		c.drv = drv
	}
	c.groups = append(c.groups, groups...)
}

func (r *MigrationRegistry) snapshot() []struct {
	key string
	c   *dbContribution
} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]struct {
		key string
		c   *dbContribution
	}, 0, len(r.order))
	keys := append([]string(nil), r.order...)
	sort.Strings(keys) // deterministic; "" sorts first
	for _, k := range keys {
		out = append(out, struct {
			key string
			c   *dbContribution
		}{k, r.dbs[k]})
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
			d = 0
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestMigrationRegistry -v`
Expected: PASS.

- [ ] **Step 5: Add rollback + cross-dep-failure tests, run package**

Add a test asserting an *unresolved* cross-extension dep still fails loudly (contributing only `trove` whose dep `identity/postgres` is absent → `RunAll` returns an error containing "unknown group"). Then:

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove/extension && go test ./... && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/rexraphael/Work/xraph/forgery/grove
git add extension/migration_registry.go extension/migration_registry_test.go
git commit -m "feat(extension): MigrationRegistry runs one ordered pass per database"
```

### Task 5: Extension contributes to the registry instead of self-migrating

**Files:**
- Modify: `extension/extension.go` (`Register`, plus a helper to resolve/create the registry)
- Modify: `extension/migratable.go` (`Migrate` no-ops when contribution mode is active)
- Test: `extension/migration_registry_test.go` (extend)

**Interfaces:**
- Consumes: `vessel.HasType`, `vessel.ProvideValue`, `vessel.MustInject` (from `github.com/xraph/vessel`); `fapp.Container()`.
- Produces:
  - `func (e *Extension) registryFromContainer(c vessel.Vessel) *MigrationRegistry` — get-or-create the shared registry.
  - Internal flag `e.contributed bool` so `Migrate` knows the registry owns execution.

- [ ] **Step 1: Write the failing test**

```go
func TestExtension_ContributesWhenRegistryPresent(t *testing.T) {
	c := vessel.New() // use the constructor the repo uses for a bare container
	vessel.ProvideValue(c, NewMigrationRegistry())

	e := New(WithMigrations(migrate.NewGroup("grove-core")))
	reg := e.registryFromContainer(c)
	if reg == nil {
		t.Fatal("expected to resolve the shared registry")
	}
	// Second extension resolves the SAME instance.
	e2 := New()
	if e2.registryFromContainer(c) != reg {
		t.Fatal("expected the same registry instance across extensions")
	}
}
```

(If `vessel.New()` is not the bare-container constructor, use whatever the repo's tests use to build a container — inspect `extension/*_test.go` for the existing pattern.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestExtension_Contributes -v`
Expected: FAIL (`registryFromContainer` undefined).

- [ ] **Step 3: Implement get-or-create + contribution**

Add to `extension/extension.go`:

```go
// registryFromContainer returns the shared MigrationRegistry, creating and
// providing it on first use. Forge registers extensions sequentially, so the
// check-then-provide is race-free.
func (e *Extension) registryFromContainer(c vessel.Vessel) *MigrationRegistry {
	if c == nil {
		return nil
	}
	if !vessel.HasType[*MigrationRegistry](c) {
		reg := NewMigrationRegistry(WithRegistryLockTimeout(e.config.LockTimeout))
		if err := vessel.ProvideValue(c, reg); err != nil {
			return nil
		}
	}
	return vessel.MustInject[*MigrationRegistry](c)
}
```

In `Register`, after groups/driver are resolved and before any self-migration, contribute when central mode is active. Central mode is active when the app opts in; detect it via a config flag the extension reads (e.g. `e.config.CentralMigrations bool`, default false) OR when the registry already exists in the container (forge provides it when `CentralMigrations` is on — see Phase 3). Use the flag for an explicit signal:

```go
	// Central migration mode: contribute groups to the shared registry and let
	// the forge-triggered pass run them. Otherwise fall back to self-migrate.
	if e.config.CentralMigrations {
		reg := e.registryFromContainer(fapp.Container())
		if reg != nil {
			if e.isMultiDB() {
				for name, groups := range e.dbMigrations {
					if db, err := e.manager.Get(name); err == nil {
						reg.Contribute(name, db.Driver(), groups...)
					}
				}
				if len(e.groups) > 0 && e.db != nil {
					reg.Contribute("", e.db.Driver(), e.groups...)
				}
			} else if len(e.groups) > 0 && e.db != nil {
				reg.Contribute("", e.db.Driver(), e.groups...)
			}
			e.contributed = true
		}
	}
```

Add `CentralMigrations bool` to `Config` (`extension/config.go`, yaml `central_migrations`) and `contributed bool` to the `Extension` struct. Add `WithCentralMigrations() ExtOption` in `extension/options.go`.

In `extension/migratable.go`, make `Migrate`/`Rollback` no-op when contributed (the registry owns execution):

```go
func (e *Extension) Migrate(ctx context.Context) (*forge.MigrationResult, error) {
	if e.config.DisableMigrate {
		return nil, errors.New("grove: migrations are disabled via configuration")
	}
	if e.contributed {
		// Central migrator owns execution; nothing to do per-extension.
		return &forge.MigrationResult{}, nil
	}
	// ... existing single/multi-DB logic unchanged ...
}
```

Apply the same `e.contributed` guard at the top of `Rollback`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove && go test ./extension/ -run TestExtension_Contributes -v`
Expected: PASS.

- [ ] **Step 5: Full extension module test + vet**

Run: `cd /Users/rexraphael/Work/xraph/forgery/grove/extension && go test ./... && go vet ./...`
Expected: PASS. Confirm existing self-migrate tests still pass (fallback path unchanged when `CentralMigrations` is off).

- [ ] **Step 6: Commit**

```bash
cd /Users/rexraphael/Work/xraph/forgery/grove
git add extension/extension.go extension/migratable.go extension/config.go extension/options.go extension/migration_registry_test.go
git commit -m "feat(extension): contribute groups to shared registry in central mode"
```

---

## Phase 3 — Forge: opt-in `CentralMigrations` split-phase startup *(requires the forge repo)*

> Execute in the forge repo (`github.com/xraph/forge`), not this workspace. After release, bump grove's `extension/go.mod` to the new forge version.

### Task 6: Add `CentralMigrations` config + split-phase startup + correctly-timed `PhaseAfterRegister`

**Files (in forge):**
- Modify: `app_impl.go` (`Start`, the extension loop at lines ~530-560)
- Modify: forge app config struct (add `CentralMigrations bool`)
- Modify: `app_impl.go` registry provisioning — provide a registry the grove extension can resolve.

**Behavior:**
- Add `CentralMigrations bool` to the app config (default false), surfaced via `.forge.yaml` and a builder option (e.g. `forge.WithCentralMigrations()`), mirroring existing `MigrationsDisabled()` plumbing.
- In `Start`, when `CentralMigrations` is **false**: keep the current interleaved per-extension `Register` then `Start` loop verbatim (no change).
- When **true**, replace the single loop with three passes over the same dependency `order`:
  1. **Register-all:** `for name := range order { ext.Register(a) }`
  2. **Migrate:** `a.lifecycleManager.ExecuteHooks(ctx, PhaseAfterRegister, a)` — this now fires at the moment its doc comment (`lifecycle.go:19`) already promises ("after extensions register but before they start"). The grove registry's hook (Task 7) runs here.
  3. **Start-all:** `for name := range order { ext.Start(ctx) }`
- Keep the existing post-loop `PhaseAfterRegister` execution only in the non-central path to avoid firing it twice; in the central path it has already fired between the two passes.

- [ ] **Step 1: Write the failing test (in forge)**

Add a forge test with two fake extensions recording call order into a shared slice. With `CentralMigrations` on, assert order is `[A.Register, B.Register, <hook>, A.Start, B.Start]`; with it off, assert `[A.Register, A.Start, B.Register, B.Start]`.

- [ ] **Step 2: Run it — expect FAIL** (`CentralMigrations` unknown / order wrong).

- [ ] **Step 3: Implement** the config flag and the split loop in `app_impl.go` as described.

- [ ] **Step 4: Run the test — expect PASS.**

- [ ] **Step 5: Run forge's suite** (`go test ./...`), confirm no regressions in the default path.

- [ ] **Step 6: Commit** in the forge repo: `feat(app): opt-in CentralMigrations split-phase startup`.

### Task 7: Grove registry registers the migrate hook *(grove `extension`, after forge release)*

**Files:**
- Modify: `extension/extension.go` (`Register`)

**Behavior:** when `CentralMigrations` is active and this is the first extension to create the registry, register a `PhaseAfterRegister` hook that calls `reg.RunAll(ctx)` exactly once:

```go
	if e.config.CentralMigrations {
		reg := e.registryFromContainer(fapp.Container())
		if reg != nil && !reg.hookRegistered() { // guard so only one hook is added
			reg.markHookRegistered()
			_ = fapp.RegisterHook(forge.PhaseAfterRegister, func(ctx context.Context, a forge.App) error {
				if a.MigrationsDisabled() {
					return nil
				}
				_, err := reg.RunAll(ctx)
				return err
			}, forge.LifecycleHookOptions{Name: "grove-central-migrate", Priority: 1000})
		}
		// ... then Contribute as in Task 5 ...
	}
```

Add `hookRegistered()`/`markHookRegistered()` (a `sync.Once`-guarded bool) to `MigrationRegistry`.

- [ ] **Step 1:** Write a test asserting the hook is registered exactly once across multiple extensions.
- [ ] **Step 2:** Run — expect FAIL.
- [ ] **Step 3:** Implement the once-guard + hook registration.
- [ ] **Step 4:** Run — expect PASS.
- [ ] **Step 5:** `cd extension && go test ./... && go vet ./...`.
- [ ] **Step 6:** Commit: `feat(extension): register central migrate hook on PhaseAfterRegister`.

---

## Phase 4 — twinos adoption *(requires the twinos repo)*

### Task 8: Enable central migrations in twinos

- [ ] Bump twinos to the grove + forge versions that include Phases 1–3.
- [ ] Enable `CentralMigrations` in the forge app config / `.forge.yaml`.
- [ ] For trove and any extension that runs grove migrations itself: stop calling grove's orchestrator directly; instead register groups so the grove extension contributes them (or contribute directly to the resolved `*extension.MigrationRegistry`).
- [ ] Declare real cross-extension ordering with `migrate.DependsOn("<owner-group>")` (e.g. `trove/postgres` depends on `identity/postgres`).
- [ ] Boot twinos; confirm a single ordered migration pass in logs, no `lock is held by another process`, and that previously-failing concurrent init now serializes.
- [ ] Commit in the twinos repo.

---

## Self-Review

**Spec coverage:**
- Part A (configurable lock wait, enriched error) → Tasks 1, 2, 3. ✓
- Part B.1 (`MigrationRegistry`) → Task 4. ✓
- Part B.2 (extension contribute/fallback) → Task 5. ✓
- Part B.3 (non-grove extensions adopt) → Task 8. ✓
- Part B.4 (forge `CentralMigrations`, split-phase, trigger) → Tasks 6, 7. ✓
- Cross-extension `DependsOn` works → exercised by Task 4 test (and the unresolved-dep failure test in Task 4 Step 5). ✓
- Lifecycle decision (schema-before-Start via Register-all → migrate → Start-all) → Task 6. ✓
- Back-compat default-off → Tasks 5 (`contributed` guard), 6 (non-central path unchanged). ✓
- Data integrity (group-keyed records, idempotency) → preserved by reusing `migrate.Orchestrator`; no record-format change. ✓

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Test-double references explicitly say to reuse the repo's existing pattern and name the fallback. Phases 3–4 carry full behavior specs and signatures, with the cross-repo constraint called out (not a placeholder — a real boundary).

**Type consistency:** `SetLockTimeout`/`WithLockTimeout`, `LockInspector.LockInfo`, `MigrationRegistry.{Contribute,RunAll,RollbackAll,StatusAll}`, `registryFromContainer`, `e.contributed`, `e.config.{LockTimeout,CentralMigrations}` are used consistently across tasks. `forge.MigrationResult` fields (`Applied`, `RolledBack`, `Names`) match `extension/migratable.go` usage. `convertGroupStatuses` reused from the existing file.

**Note for the implementer:** Phases 1–2 are fully executable in this grove checkout now. Phase 3 requires the forge repo; Phase 4 requires twinos. Land and release Phases 1–2 first — Part A alone stops the crash loop in production.
