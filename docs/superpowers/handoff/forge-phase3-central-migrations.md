# Phase 3 Handoff — Forge `CentralMigrations` split-phase startup

**Repo:** `github.com/xraph/forge` (NOT this repo — check it out separately).
**Target version baseline:** v1.7.1 (line numbers below are from v1.7.1; re-locate if drifted).
**Depends on:** grove `extension` (Phase 2, already done) — the grove extension already contributes its groups to a shared `extension.MigrationRegistry` and registers a `PhaseAfterRegister` hook that calls `RunAll`, but only when its own `CentralMigrations` config flag is on. This forge change makes `PhaseAfterRegister` fire at the correct moment (after all `Register`, before any `Start`) so schema exists before extensions seed/query at `Start`.

## Why

Forge's `Start` interleaves `Register` then `Start` per extension in dependency order (`app_impl.go:532-560`). So today there is no "all registered, none started" point, and `PhaseAfterRegister` (its doc comment at `lifecycle.go:19` notwithstanding) actually fires *after* the whole loop. Grove's central-migration hook therefore runs after extensions have already started — too late for Start-time seeding. This change splits the loop, gated by an opt-in flag, so the default behavior is byte-for-byte unchanged.

## Change set

### 1. Add the config flag — `app.go`

In the `AppConfig` struct, next to `DisableMigrations` (around `app.go:105`):

```go
	// CentralMigrations runs all MigratableExtension migrations as a single
	// ordered pass (Register-all -> migrate -> Start-all) instead of letting
	// each extension migrate independently. Default: false. Also settable via
	// .forge.yaml database.central_migrations.
	CentralMigrations bool
```

Add the option next to `WithDisableMigrations` (around `app.go:332`):

```go
// WithCentralMigrations enables the single-pass, dependency-ordered migration
// lifecycle (Register-all -> migrate -> Start-all).
func WithCentralMigrations() AppOption {
	return func(c *AppConfig) { c.CentralMigrations = true }
}
```

Add to the `App` interface (near `MigrationsDisabled() bool` at `app.go:46`):

```go
	// CentralMigrationsEnabled reports whether the single-pass migration
	// lifecycle is enabled.
	CentralMigrationsEnabled() bool
```

### 2. Accessor — `app_impl.go`

Next to `MigrationsDisabled()` (around `app_impl.go:482`):

```go
// CentralMigrationsEnabled reports whether the single-pass migration lifecycle
// is enabled via config or .forge.yaml.
func (a *app) CentralMigrationsEnabled() bool {
	return a.config.CentralMigrations
}
```

### 3. `.forge.yaml` plumbing — `app_impl.go`

In the forge-config struct (around `app_impl.go:1456`, where `DisableMigrations bool \`yaml:"disable_migrations"\`` lives):

```go
		CentralMigrations bool `yaml:"central_migrations"`
```

And in the merge block (around `app_impl.go:1546`, mirroring the `DisableMigrations` merge):

```go
	// Set CentralMigrations from .forge.yaml if not already set programmatically.
	if !config.CentralMigrations && forgeConfig.Database.CentralMigrations {
		config.CentralMigrations = true
	}
```

### 4. The split-phase loop — `app_impl.go` (the core change)

Replace the single per-extension loop in `Start` (currently `app_impl.go:532-560`). Keep the existing interleaved loop verbatim for the default path; add the split path under the flag.

**Current (keep as the `else` branch):**

```go
	for _, name := range order {
		ext, ok := extMap[name]
		if !ok {
			continue
		}
		a.logger.Info("registering extension", F("extension", ext.Name()), F("version", ext.Version()))
		if err := ext.Register(a); err != nil {
			return fmt.Errorf("failed to register extension %s: %w", ext.Name(), err)
		}
		a.logger.Info("starting extension", F("extension", ext.Name()))
		if err := ext.Start(ctx); err != nil {
			return fmt.Errorf("failed to start extension %s: %w", ext.Name(), err)
		}
		a.logger.Info("extension ready", F("extension", ext.Name()))
	}
```

**New:**

```go
	if a.config.CentralMigrations {
		// Phase 1: Register ALL extensions (in dependency order). Each
		// MigratableExtension contributes its migration groups to the shared
		// registry here, but runs nothing yet.
		for _, name := range order {
			ext, ok := extMap[name]
			if !ok {
				continue
			}
			a.logger.Info("registering extension", F("extension", ext.Name()), F("version", ext.Version()))
			if err := ext.Register(a); err != nil {
				return fmt.Errorf("failed to register extension %s: %w", ext.Name(), err)
			}
		}

		// Phase 2: Run the single ordered migration pass before any Start, so
		// schema exists for extensions that seed/query during Start. The grove
		// MigrationRegistry registered a PhaseAfterRegister hook during Register.
		if err := a.lifecycleManager.ExecuteHooks(ctx, PhaseAfterRegister, a); err != nil {
			return fmt.Errorf("central migration phase failed: %w", err)
		}

		// Phase 3: Start ALL extensions (in dependency order).
		for _, name := range order {
			ext, ok := extMap[name]
			if !ok {
				continue
			}
			a.logger.Info("starting extension", F("extension", ext.Name()))
			if err := ext.Start(ctx); err != nil {
				return fmt.Errorf("failed to start extension %s: %w", ext.Name(), err)
			}
			a.logger.Info("extension ready", F("extension", ext.Name()))
		}
	} else {
		// Existing interleaved Register+Start loop (UNCHANGED).
		for _, name := range order {
			// ... current body verbatim ...
		}
	}
```

**Important:** in the central path, `PhaseAfterRegister` now fires *between* Register-all and Start-all. The post-loop `PhaseAfterRegister` execution that exists today (around `app_impl.go:563`) must NOT also fire in the central path — guard it so it only runs in the `else` (non-central) branch, otherwise the hook runs twice. Easiest: move the existing `ExecuteHooks(ctx, PhaseAfterRegister, a)` call into the `else` branch's tail.

### 5. (Optional follow-on) make rollback/status reachable centrally

Grove's central-mode `Rollback` deliberately errors loudly because there is no central rollback trigger yet (forward migration runs via the `PhaseAfterRegister` hook; rollback/status have no equivalent). To complete the story:

- Define `forge.CentralMigrator interface { RunMigrations(ctx) error; RollbackMigrations(ctx) error; MigrationStatus(ctx) (...) }` (or similar) and have the grove registry implement it / register itself in DI.
- In the forge CLI (`cli/app_runner_commands.go`), when `CentralMigrationsEnabled()`, route `migrate up/down/status` through the single `CentralMigrator` (resolved from the container) instead of looping `m.Migrate`/`m.Rollback` per extension. This also fixes the current quirk that `migrate down` calls `app.Start()` (which triggers the forward hook) — under central mode the CLI should bootstrap without auto-running the Start hook, or invoke the migrator explicitly.

This step is not required for the core "schema-before-Start central forward migration" goal, but it's needed before `migrate down`/`migrate status` behave correctly with `CentralMigrations` on.

## Tests (in forge)

- Two fake extensions recording call order. With `WithCentralMigrations()`: assert order is `[A.Register, B.Register, <PhaseAfterRegister hook>, A.Start, B.Start]`. Without it: `[A.Register, A.Start, B.Register, B.Start]` (unchanged).
- Assert `PhaseAfterRegister` fires exactly once in each path (no double-fire in central mode).
- `.forge.yaml` with `database.central_migrations: true` flips the flag.

## After forge release

Bump `extension/go.mod` in grove to the new forge version. Grove's wiring (Phase 2) is already forward-compatible — no grove change needed once forge fires `PhaseAfterRegister` at the right time.
