# Phase 4 Handoff — twinos adoption of central migrations

**Repo:** the twinos backend (NOT this repo).
**Prerequisites:** grove with Phases 1–2 (this branch) AND forge with Phase 3 (`CentralMigrations` split-phase). Do NOT enable the flag before the forge change ships — on forge ≤ 1.7.1 the central hook fires after extensions start, which is worse than the per-extension path for anything that seeds/queries at `Start`.

## What this fixes

The boot crash loop (`migrate: acquire lock: ... lock is held by another process`) was concurrent in-process migration runs racing for the global `pg_advisory_lock(1)` and the loser failing the boot after 30s. Phase 1 alone (configurable lock wait, default 5m) already stops the crash loop — runs now serialize and wait. Phases 2–4 additionally collapse N independent migration passes into one ordered pass and make cross-extension `DependsOn` actually work.

## Order of operations

1. **Ship Phase 1 first, verify the crash loop is gone.** Bump twinos to the grove version with the configurable lock wait. With `CentralMigrations` still OFF, boot twinos and confirm: no more `lock is held by another process` failures; concurrent extension init now serializes (you may see a brief wait in logs instead of a crash). This is the low-risk win — do it before anything else.

2. **Bump to the forge version with `CentralMigrations` (Phase 3).** Update twinos' forge dependency and the grove `extension` dependency to the releases that include Phase 3.

3. **Enable the flag.** Programmatically `forge.WithCentralMigrations()` when building the app, OR in `.forge.yaml`:
   ```yaml
   database:
     central_migrations: true
   ```
   Also set the grove extension's own flag (it has a matching `CentralMigrations` config / `extension.WithCentralMigrations()`), since the grove extension only contributes + registers the trigger hook when its flag is on.

4. **Switch extensions from self-migrate to contribute.** For every extension that currently runs grove migrations itself (trove, and any other that calls a grove orchestrator or `extension.Migrate` directly):
   - Resolve the shared registry from the forge container: `reg := vessel.MustInject[*extension.MigrationRegistry](app.Container())` (or let the grove extension contribute on its behalf if it owns the groups).
   - Contribute groups instead of running them: `reg.Contribute(dbKey, driver, groups...)` where `dbKey` is `""` for the default DB or the named DB. Stop calling the orchestrator directly.
   - Extensions built on the grove `extension` package get this automatically once `CentralMigrations` is on — they contribute `WithMigrations(...)`/`WithMigrationsFor(...)` groups and no-op their own `Migrate`. Only extensions that bypass the grove extension need manual `Contribute` calls.

5. **Declare the real cross-extension dependencies.** This is the payoff and was impossible before (each extension's orchestrator only saw its own groups, so a cross-extension `DependsOn` errored with "unknown group"). For each migration group that depends on another extension's schema:
   ```go
   var Migrations = migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
   ```
   Use the exact group name the owning extension registers (the `grove_migrations` table's `group` column shows existing names). A `DependsOn` on a group that no registered extension contributes will now fail loudly at migrate time — that is the intended signal for a misconfiguration.

6. **Boot and verify.**
   - Logs show a single ordered migration pass (one acquire/release of the migration lock), not one per extension.
   - No `lock is held by another process`.
   - Dependent migrations run after their dependencies (e.g. `identity/postgres` before `trove/postgres`).
   - Extensions that seed/query at `Start` find their schema present (this is why Phase 3's split-phase matters).

## Caveats / known limits at this stage

- **Rollback:** in central mode, the grove extension's per-extension `Rollback` returns a loud error (central rollback isn't wired until the forge `CentralMigrator` follow-on — see Phase 3 doc §5). If you need `migrate down` under central mode, implement that follow-on first, or temporarily disable `CentralMigrations` to roll back via the per-extension path.
- **Single default DB assumption:** the registry keys contributions by `dbKey`; for the default DB (`""`) it keeps the first contributed driver and appends later groups. If twinos has multiple *distinct* default-DB drivers contributing under `""`, give them explicit named `dbKey`s instead so groups don't get routed to the wrong driver.
- **Idempotency:** the `grove_migrations` table still keys on `(version, "group")`, so already-applied migrations are skipped. Switching to central mode does not re-run or duplicate applied migrations.

## Rollback plan if central mode misbehaves

Set `CentralMigrations` back to off. Behavior reverts to per-extension migration, now protected by Phase 1's blocking lock (so still no crash loop). No schema changes are needed to flip the flag either way.
