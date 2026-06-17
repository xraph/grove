# Forge/Grove Central, Ordered Migrations — Design

**Date:** 2026-06-17
**Status:** Approved design (pre-implementation)
**Repos affected:** `grove` (migrate, drivers, extension), `forge` (v1.7.x lifecycle + new interface)

## Problem

Boot fails intermittently in the twinos backend with:

```
failed to register extension trove: trove: migrations: trove/postgres: migration failed:
migrate: acquire lock: pgmigrate: migrate: lock is held by another process
```

It recurs ("goes on and on") — the app fails to start, restarts, fails again: a **crash loop**, not a deadlock.

### Root cause (verified against code)

1. **One global, fixed-key advisory lock.** Every migration run — every group, every extension —
   contends on `pg_try_advisory_lock(1)` in `drivers/pgdriver/pgmigrate/executor.go:163`. The key is
   the literal `1` for the whole database.

2. **Every extension migrates independently.** Forge has no central migrator. Both the registration
   path and the CLI auto-migrate path loop over extensions calling `m.Migrate(ctx)` one at a time
   (`forge cli/app_runner_commands.go:48`). Each grove extension builds its **own** orchestrator
   (`extension/migratable.go:255`) and does a full **acquire-lock(1) → ensure tables → list applied →
   apply → release-lock(1)** cycle. N extensions = N independent lock cycles on the same global key.

3. **Lock acquisition is non-blocking with a hard 30s ceiling, then fatal.** `acquireLockWithRetry`
   (`migrate/migrator.go:212`) retries `pg_try_advisory_lock` for 30 seconds, then returns
   `ErrLockHeld`. That error is fatal at registration → process exits → restart → re-contend.

4. **Trigger is concurrent migration in one process.** Forge registers extensions *sequentially*
   (`app_impl.go:532-560`), and the pg path releases the lock correctly on a dedicated pinned
   connection (`grove.DB.Driver()` returns the raw `*PgDB`, so `ConnAcquirer` is honored — **no leak**).
   So a single-process hang on lock(1) means something runs grove migrations *concurrently* inside the
   process — most likely trove initializing sub-plugins/modules in goroutines (or multiple grove DB
   handles), each firing its own orchestrator and racing for lock(1). One wins; the rest burn 30s and
   fail the boot.

5. **Cross-extension ordering is currently broken.** `migrate/planner.go:55` makes `topoSort` **error**
   on an unknown dependency: a group doing `DependsOn("identity/postgres")` fails with *"depends on
   unknown group"* unless that group is in the **same** orchestrator. Because each extension runs its
   own orchestrator with only its own groups, cross-extension `DependsOn` cannot work today.

## Goals

- Stop the crash loop (immediate).
- One ordered migration pass per database, under one lock, across all extensions.
- Make cross-extension `DependsOn` ordering actually work.
- No surprise behavior change for existing apps (opt-in).

## Non-goals

- Rewriting grove's migration engine. Grove's `topoSort`/orchestrator already does the hard part.
- A forge-native SQL/migration model. Forge coordinates timing/collection; grove executes.
- Changing the on-disk migration record format.

## Approach: Hybrid (A ships first, B builds on it)

### Part A — Grove lock waits instead of failing the boot (safety net)

**Change `migrate/migrator.go`:** make the lock-wait budget configurable instead of a hard-coded 30s.

- Add `migrate.WithLockTimeout(d time.Duration)` orchestrator option. Default raised to **5m**.
  `0` means "wait until the context deadline."
- Keep the portable `pg_try_advisory_lock` + exponential-backoff loop (works unchanged across all six
  drivers: pg, mysql, sqlite, turso, clickhouse, mongo).
- On exhaustion, return an error enriched with `locked_by` / `locked_at` read from the lock table so
  the failure is diagnosable instead of opaque.
- Thread the timeout from grove extension config (`extension/config.go`) → orchestrator.

**Effect:** concurrent in-process migration runs *serialize and wait* rather than one dying at 30s.
The crash loop stops, independent of Part B. No leak work needed (pg dedicated-conn path is correct).

**Files:** `migrate/migrator.go`, `migrate/migration.go` (option plumbing if needed),
`extension/config.go`, `extension/options.go`, `extension/migratable.go` (pass option into
`buildOrchestrator`).

### Part B — Central, ordered migration coordinator

**Core idea:** all groups for a given database land in **one** grove orchestrator, run **once**, under
**one** lock. Grove's existing `topoSort` resolves cross-extension `DependsOn` for free — and stops
erroring on "unknown group" because every group is now present in the same plan.

#### B.1 New: `extension.MigrationRegistry` (grove `extension` package)

The `extension` package already imports forge, so this is the correct home.

```go
// MigrationRegistry collects migration groups from all extensions and runs them
// as a single ordered pass per database.
type MigrationRegistry struct { /* mu, contributions keyed by dbKey */ }

type dbContribution struct {
    drv    grove.GroveDriver       // raw driver for this database
    groups []*migrate.Group        // accumulated across all extensions
}

// Contribute adds groups for a database. dbKey "" = default database.
func (r *MigrationRegistry) Contribute(dbKey string, drv grove.GroveDriver, groups ...*migrate.Group)

// RunAll builds ONE orchestrator per database with all contributed groups and runs once.
func (r *MigrationRegistry) RunAll(ctx context.Context) (*forge.MigrationResult, error)
func (r *MigrationRegistry) RollbackAll(ctx context.Context) (*forge.MigrationResult, error)
func (r *MigrationRegistry) StatusAll(ctx context.Context) ([]*forge.MigrationGroupInfo, error)
```

- `RunAll` buckets by `dbKey`, builds one `migrate.NewOrchestrator(executor, allGroups...)` per
  database, and runs it (one lock acquire/release per database, topo-sorted plan).
- `RollbackAll` walks databases/groups in reverse.
- Registered as a forge DI service so every extension resolves the **same** instance.

#### B.2 grove `extension.Extension` change

In `Register`: if a `MigrationRegistry` is resolvable from forge DI, `Contribute` this extension's
groups (single-DB `e.groups` under `""`, multi-DB `e.dbMigrations` under their names) and **skip** the
per-extension orchestrator. Otherwise fall back to today's self-migrate behavior (standalone grove
use, and the `CentralMigrations`-disabled path). This keeps `extension/migratable.go` working as the
fallback.

#### B.3 Non-grove extensions (trove, etc.)

Extensions that use grove's `migrate` package directly resolve the `MigrationRegistry` service from
forge DI and `Contribute` their groups during `Register`, instead of running their own orchestrator.
This is the adoption step for twinos.

#### B.4 Forge changes

- Define `forge.CentralMigrator interface { RunMigrations(ctx context.Context) error }`. The grove
  `MigrationRegistry` implements it and registers itself in DI.
- Add an opt-in app config flag `CentralMigrations` (off by default).
- **When enabled**, split the startup loop in `app_impl.go` from per-extension interleaved
  Register+Start into three phases:
  1. **Register-all** extensions in dependency order (each contributes its groups here).
  2. **`RunMigrations(ctx)`** — the single ordered pass across all databases.
  3. **Start-all** extensions in dependency order (schema now exists for seeding/queries at Start).
- **When disabled**, keep today's interleaved loop exactly. No behavior change.

#### Cross-extension dependencies

Expressed with the existing API, now functional under the central pass:

```go
var Migrations = migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
```

No new dependency mechanism — the central run is what makes the existing one resolve.

## Lifecycle decision (resolved)

Some extensions query/seed their schema during `Start`, so the central pass must run **before any
extension starts**. Forge's current loop interleaves Register+Start per extension
(`app_impl.go:530-560`) and has no "all-registered, none-started" hook (`PhaseAfterRegister` actually
fires *after* the whole loop — its doc comment at `lifecycle.go:19` is stale). Therefore Part B uses
the **split-phase** lifecycle (Register-all → RunMigrations → Start-all), gated by `CentralMigrations`.

**Relaxation under the new lifecycle:** an extension's `Register` can no longer assume a dependency has
fully *Started* (only that it is registered). Mitigation: DI `Resolve` auto-starts services on demand
(`app_impl.go:548`), so cross-extension service use during Register still works. Documented as a
constraint of enabling `CentralMigrations`.

## Data integrity & back-compat

- `grove_migrations` keys on `(version, "group")`; central running preserves per-extension group
  identity and idempotency (already-applied migrations are skipped by the orchestrator's `appliedSet`).
- The orchestrator's "unknown group" error becomes a *useful* signal: a `DependsOn` on a group that no
  registered extension contributes is a real misconfiguration and should fail loudly.
- Extensions not migrated through the registry keep working (fallback path), protected from crashing by
  Part A.
- Default (`CentralMigrations` off) = today's behavior, zero change.

## Testing

- **Part A:** unit test that two concurrent orchestrators on the same DB serialize (second waits, both
  succeed); test that exhausting the budget returns the enriched `locked_by` error; test `0` =
  wait-until-ctx-deadline.
- **Part B:** unit test `MigrationRegistry` merges groups across extensions and runs one ordered plan;
  test cross-extension `DependsOn` resolves (and that a previously-broken cross-orchestrator dep now
  works); test reverse-order rollback; test multi-DB bucketing.
- **Forge:** test the split-phase lifecycle runs Register-all → RunMigrations → Start-all in order when
  enabled, and that disabled keeps interleaved behavior; integration test reproducing the concurrent
  in-process race that fails today and passes after the change.

## Rollout

1. Land Part A in grove (small, low-risk). Tag a grove release. twinos picks it up → crash loop stops.
2. Land `MigrationRegistry` + extension contribute/fallback in grove.
3. Land `forge.CentralMigrator` + `CentralMigrations` split-phase in forge.
4. twinos: enable `CentralMigrations`, switch trove + other extensions to `Contribute`, declare
   cross-extension `DependsOn` where needed.

## Open questions / risks

- **trove's concurrency source** (where the goroutines/multiple-DB-handles are) is unconfirmed from the
  grove repo. Part A makes the fix robust regardless, but confirming it (a goroutine dump at the hang,
  or trove's sub-plugin init code) would let us verify the exact race the integration test should cover.
- The split-phase relaxation (dependency *Started* before dependent *Register*) needs a pass over
  existing extensions to confirm none rely on it outside DI resolve.
