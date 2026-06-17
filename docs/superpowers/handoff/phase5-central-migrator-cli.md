# §5 — CentralMigrator: CLI `migrate down`/`status` in central mode

**Goal:** With `CentralMigrations` on, make `migrate down` and `migrate status` operate on the single central migration set (and `migrate up` report correctly), instead of the per-extension loop where central-mode `Rollback` errors and `status` only shows one extension at a time. Also fix the quirk that `migrate down`/`status` bootstrap via `app.Start()`, which in central mode fires the forward-migration hook.

## Why a new interface + resolver

`vessel.Inject[T]` resolves by **exact type key**, not interface satisfaction. So forge cannot pull the grove `*extension.MigrationRegistry` out of the container by an interface unless grove registers it under that interface. Forge defines `CentralMigrator`; grove registers its registry under it.

The grove `MigrationRegistry` already returns forge types (`*forge.MigrationResult`, `[]*forge.MigrationGroupInfo`) from `RunAll`/`RollbackAll`/`StatusAll`, so the interface method names match the existing methods — grove implements it for free.

## Forge changes (repo: github.com/xraph/forge, branch feat/central-migrations)

### 1. `CentralMigrator` interface (new file `central_migrator.go`)
```go
package forge

import "context"

// CentralMigrator runs all extension migrations as one ordered set per database.
// The grove MigrationRegistry implements it; it is resolved from the DI container.
type CentralMigrator interface {
	RunAll(ctx context.Context) (*MigrationResult, error)
	RollbackAll(ctx context.Context) (*MigrationResult, error)
	StatusAll(ctx context.Context) ([]*MigrationGroupInfo, error)
}
```

### 2. Runtime suppression of auto-migration — App interface + impl
The grove forward hook (PhaseAfterRegister) already short-circuits on `a.MigrationsDisabled()`. Add a runtime setter so the CLI can suppress it for `down`/`status`:
- `App` interface: `SetMigrationsDisabled(bool)`.
- `(a *app) SetMigrationsDisabled(v bool) { a.config.DisableMigrations = v }` (next to `MigrationsDisabled()`).

### 3. Resolver — App interface + impl
- `App` interface: `CentralMigrator() (CentralMigrator, bool)`.
- ```go
  func (a *app) CentralMigrator() (CentralMigrator, bool) {
      if a.container == nil { return nil, false }
      if !vessel.HasType[CentralMigrator](a.container) { return nil, false }
      cm, err := vessel.Inject[CentralMigrator](a.container)
      if err != nil { return nil, false }
      return cm, true
  }
  ```

### 4. CLI routing (`cli/app_runner_commands.go`)
Each command keeps its existing per-extension path as the `else`; add a central branch:

- **`up`** (central): `app.Start()` (the forward hook applies migrations before Start-all — schema ready), then resolve `CentralMigrator`; call `StatusAll` and report applied/pending counts. Do NOT suppress (extensions that seed at Start need schema first).
- **`down`** (central): `app.SetMigrationsDisabled(true)` BEFORE `app.Start()` (so the forward hook no-ops), Start, resolve migrator, confirm (unless `--force`), call `RollbackAll`, report `RolledBack` + `Names`.
- **`status`** (central): `app.SetMigrationsDisabled(true)` before Start, Start, resolve migrator, call `StatusAll`, render the existing table over the returned groups.
- If `CentralMigrationsEnabled()` is true but `CentralMigrator()` returns `false` (nothing contributed), print an informative message and return nil.

`down`/`status` suppress the forward migration because they bootstrap against an existing schema (you're rolling back / inspecting already-applied migrations); not applying new pending migrations is correct for those commands.

## Grove change (release-gated — needs the new forge)

In `extension/extension.go` `registryFromContainer`, inside the first-time create block, ALSO register under the interface:
```go
if !vessel.HasType[*MigrationRegistry](c) {
    reg := NewMigrationRegistry(WithRegistryLockTimeout(e.config.LockTimeout))
    if err := vessel.ProvideValue(c, reg); err != nil { return nil }
    _ = vessel.ProvideValue[forge.CentralMigrator](c, reg) // resolve-by-interface for the CLI
}
```
And a compile-time assertion in `migration_registry.go`:
```go
var _ forge.CentralMigrator = (*MigrationRegistry)(nil)
```
This references `forge.CentralMigrator`, so it only compiles once `extension/go.mod` is bumped to the forge release that includes §5. Apply it together with the Phase-4 forge bump.

## Verification
End-to-end (worktree with `replace forge => local`): with `CentralMigrations` on, `migrate status` lists all groups across extensions without applying anything; `migrate down` rolls back the last batch via `RollbackAll` (no spurious forward migration); `migrate up` applies then reports. Forge-side unit tests use a fake `CentralMigrator` registered in a test app's container.
