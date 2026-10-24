# Migration builders: design

Date: 2026-10-24. Status: draft for Rex's review.

## What you'll get

Write a grove migration the way you'd write one in bun. The function gets the database, you build tables, columns and indexes with chained builders, and the version comes from the file name `forge db create-go` generated:

```go
// database/migrations/20261011000001_create_projects.go
package migrations

func init() {
	App.Add(
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("loomplane_projects").IfNotExists().
				Column("id", migrate.Text, migrate.PrimaryKey()).
				Column("org_id", migrate.Text, migrate.NotNull()).
				Column("slug", migrate.Text, migrate.NotNull()).
				Column("created_at", migrate.Timestamp, migrate.NotNull()).
				Unique("org_id", "slug").Named("loomplane_projects_org_slug").
				Exec(ctx)
		},
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewDropTable("loomplane_projects").IfExists().Exec(ctx)
		},
	)
}
```

The same API works for MongoDB. Postgres and Mongo migrations stay in separate files, as every forgery store keeps them today, but you write both with the same builders and never cast the executor again.

Then every migration in loomplane's `database/migrations` gets rewritten this way.

---

## Why

Loomplane's migrations are SQL strings in `[]string` package variables. A 90-line list in `migrations.go` registers them by hand with versions somebody typed in, and those versions now run nine days ahead of the clock. Rex flagged them as wrong. The expectation was generated files, a real version per file, and grove APIs for creating resources.

Grove doesn't have those APIs yet. A migration receives a `migrate.Executor` with raw `Exec` and `Query` and nothing else. Postgres has `NewCreateTable` and `NewDropTable` on `*pgdriver.PgDB`, but a migration can't reach that handle, and those builders can't express CHECK, table-level UNIQUE, indexes or ALTER. Mongo has `CreateCollection`, `RefreshValidator` and `CreateIndexes`, but only behind `exec.(*mongomigrate.Executor)`. Every Mongo store in forgery repeats that cast in every migration.

---

## Decided by Rex

These came from the conversation on 2026-10-24. Don't reopen them.

| Decision | Detail |
|---|---|
| Builders, not raw exec | Migrations create resources through grove APIs. Raw SQL stays only where no builder makes sense. |
| The function receives the database | Like bun's `func(ctx context.Context, db *bun.DB) error`. |
| The executor form keeps working | About 1,000 existing migration functions across authsome, warden, chronicle, dispatch, vault, keysmith, trove, herald, relay and ctrlplane stay untouched. Those stores aren't converted in this project. |
| Driver agnostic | The builders live in grove's `migrate` package and each driver renders them. |
| First release: Postgres and MongoDB | Other drivers come later. |
| Separate files per driver | A migration is written for one database. Nobody expects one file to run on both. |
| Better DX than bun | Rex handed the ergonomics over. The calls below marked "our call" are ours, and Rex reviews them in this document. |
| Codex is paused | The Codex sessions stop committing to loomplane while the conversion runs. Their unfinished work stays in the tree. |

---

## Goals

1. A new migration form whose function receives `*migrate.DB`, a driver-agnostic handle.
2. Bun-style registration. `App.Add(up, down)` takes the version and name from the file name.
3. Builders for create table, drop table, alter table, create index and drop index. `db.Exec` covers everything else.
4. A Postgres renderer that covers everything loomplane's migrations use (see the coverage table).
5. A Mongo renderer for collections, `$jsonSchema` validators, indexes, and adding, dropping and renaming fields.
6. On Postgres, a new-form migration runs in a transaction by default and commits together with its applied record.
7. `forge db create-go` and `forge generate migration` emit the new form.
8. All of loomplane's migrations are converted, and a fresh database built from them is identical to one built today.

## Non-goals

- Renderers for MySQL, SQLite, Turso and ClickHouse. Until someone writes them, those drivers return `ErrSchemaUnsupported` naming the driver. The operation types are exported so anyone can add one.
- Portability of one migration file across Postgres and Mongo.
- Converting the other forgery stores, or removing or changing the executor form.
- Builders for functions, trigger bodies, DO blocks or data backfills. Those are code. `db.Exec` runs them.

## What would make this a failure even if it works

- Any loomplane migration changes its version or name. Every database that ran it recorded both, and grove would run it again.
- A fresh loomplane database differs from today's in any type, default, constraint name, index name, trigger or function.
- An existing grove consumer breaks, at compile time or in behavior. That includes a consumer that ends up with a newer grove core and an older driver module under Go's minimum version selection.
- A driver silently skips something you asked for.
- A simple migration gets more verbose than the SQL it replaces.

---

## Constraints

- Grove is v1, so `migrate.Executor` keeps its exact method set. Adding a method would break any driver module pinned at an older version. New capabilities are optional interfaces found by type assertion, which is how grove already handles `driver.StreamCapable` and friends.
- `migrate` may import `driver` and `schema`, nothing that depends on the root, and never the Mongo or pgx libraries. Driver-specific settings come from the driver's own migrate package.
- Loomplane's integration tests wrap the executor in a struct that embeds `migrate.Executor` and call `step.Up(ctx, wrapper)` directly. One of them counts statements through `Exec` and fails at each one in turn. Builders have to work through wrappers like that, so the dialect can't come from a type assertion on the executor alone. An optional `Unwrap() migrate.Executor` that the lookup follows would do it, with loomplane's test wrappers gaining the method, but the builder picks the mechanism.
- Grove releases all 16 tagged modules in lockstep by dispatching `release.yml`, and nobody pushes a tag by hand. Loomplane pins releases with no replaces, so its go.mod moves only after grove is out. Until then you develop against local grove with a `go.work` that lives outside every repo and is passed through `GOWORK`. Never commit one.
- Dispatching a release, pushing, and tagging each need Rex's go-ahead at the time.
- Two Codex sessions commit straight to loomplane's main and leave work in progress in the tree. Stage only your own paths.
- Grove's lint rules in `AGENTS.md` apply (errcheck with type-assertion checks, revive's stutter rule, three import groups) and so do Conventional Commits. Lint the way CI does, with `GOWORK=off`.

---

## Design

### Registration (our call)

```go
func (g *Group) Add(up, down Func, opts ...Option)

type Func func(ctx context.Context, db *DB) error
```

`Add` reads the caller's file name through `runtime.Caller` and expects `<14 digits>_<name>.go`. It panics on a malformed name or a duplicate version, the same way `MustRegister` panics. `down` may be nil. Options cover `NoTransaction()` and `Comment(string)`.

`Add` builds an ordinary `*Migration` whose `Up` and `Down` wrap the `Func`s. The orchestrator, `Status`, `Rollback`, `Group.Migrations()` and every test that lifts migrations into its own group keep seeing plain migrations. `Migration` gains one exported field for the transaction setting, which is additive. `MustRegister(&Migration{...})` keeps working.

### The handle (our call)

`*migrate.DB` is a struct in `migrate`:

- `Exec` and `Query` run raw statements through the executor, on the connection that holds the migration lock and inside the migration's transaction.
- `Dialect() string` returns the driver name, such as `pg` or `mongo`.
- `NewCreateTable`, `NewDropTable`, `NewAlterTable`, `NewCreateIndex` and `NewDropIndex` return builders, each finished by `Exec(ctx) error`.
- `Executor()` is the escape hatch. On Mongo, `mongomigrate.Native(db)` returns `*mongodriver.MongoDB` for data migrations no builder covers.

The `New` prefix matches grove's `NewSelect` and bun. `Exec` returns only an error, since DDL results carry nothing useful.

### Builders

The exact method set is the builder's call, inside these rules:

- `Column(name, type, opts...)` adds a column. Portable types are `Text`, `SmallInt`, `Integer`, `BigInt`, `BigSerial`, `Boolean`, `Timestamp`, `Date`, `JSON`, `UUID`, `Bytes`, `Float`, `Numeric(p, s)` and `ArrayOf(t)`, and `Type("INET")` passes anything else straight through.
- Column options are `NotNull()`, `Default(value)` for literals, `DefaultExpr(sql)` for expressions, `PrimaryKey()`, `Unique()`, `Check(sql)`, `References(table, columns...)` with `OnDelete(...)`, `Identity()` for `GENERATED ALWAYS AS IDENTITY`, and `GeneratedAs(sql)` for stored generated columns.
- Table constraints are `PrimaryKey(cols...)`, `Unique(cols...)`, `Check(sql)` and `ForeignKey(cols...).References(table, cols...)` with `OnDelete`. Each takes an optional name.
- Constraints render exactly as written: the same columns, the same expression, at the level you put them. Postgres derives an unnamed constraint's name from the columns involved (`t_col_key`, `t_col_check`, `t_check` when a CHECK touches several columns), and later migrations drop constraints by those generated names.
- Columns and constraints render in the order you wrote them. When two unnamed constraints would get the same name, Postgres adds a numeric suffix in creation order, so order is part of the name.
- One `NewAlterTable(table)` can chain several actions, and they render as one statement where Postgres allows it. The actions are `AddColumn` (with `IfNotExists`), `DropColumn` (with `IfExists`), `SetNotNull`, `DropNotNull`, `SetDefault`, `DropDefault`, `SetType`, `AddConstraint`, `DropConstraint` (with `IfExists`), `RenameColumn` and `RenameTo`. Renames that Postgres won't combine with other actions render as their own statements.
- `NewCreateIndex(name).On(table).Columns(...)` creates an index, with `Desc(col)` and `Expr(sql)` for sort order and expressions. It also takes `Unique()`, `IfNotExists()`, `Where(sql)` for SQL drivers, `Filter(doc any)` for Mongo, `Using(method)`, `Include(cols...)` and `Concurrently()`.
- `NewDropIndex(name)` takes `IfExists()`, `Concurrently()` and `Cascade()`. On Mongo it also needs `On(collection)`.
- `NewDropTable(table)` takes `IfExists()` and `Cascade()`.
- `NewCreateTable(...).Model((*T)(nil))` takes columns from a grove model, and on Mongo that's today's `NewCreateCollection` path. Inline columns are the default in migrations because they don't change when a model does.

### Rendering

`migrate` defines each operation as an exported value (create table, alter table and so on) and an optional interface a driver's migrate package implements to apply one. The Postgres renderer turns an operation into SQL and runs it through `exec.Exec`, so wrappers see every statement. The Mongo renderer runs commands through its native handle. A driver without a renderer, or one asked for something it can't do, returns `ErrSchemaUnsupported` wrapped with the driver and feature, for example `migrate: mongo does not support foreign keys`.

### Transactions (our call)

- A new-form migration on a driver that supports it runs inside a transaction on the locked connection. `RecordApplied` and `RemoveApplied` run inside the same transaction, so a migration and its record commit together or not at all.
- pgmigrate supports it. mongomigrate doesn't, since Mongo can't make collection and index changes atomic in a useful way, so Mongo migrations run as they do today.
- `NoTransaction()` opts a migration out. `Concurrently()` inside a transaction is an error, not a silent downgrade.
- Executor-form migrations behave exactly as they do today, with no transaction.
- The transaction belongs to the orchestrator. When a test calls `step.Up` or `step.Down` directly, nothing wraps it, the same as today. About 20 of loomplane's integration tests do exactly that and depend on it.

### Postgres coverage

Loomplane's migrations need all of these. Each must render to SQL that leaves the catalog exactly as the hand-written statement did.

| Feature | Uses in loomplane |
|---|---|
| CREATE TABLE IF NOT EXISTS with inline and table constraints | about 100 |
| ON DELETE CASCADE and RESTRICT, including composite foreign keys | over 120 |
| ADD CONSTRAINT, DROP CONSTRAINT | 30 and 22 |
| CREATE INDEX and CREATE UNIQUE INDEX, about 28 partial, 18 with DESC | 87 |
| SET NOT NULL, DROP NOT NULL | 1 and 3 |
| Identity, stored generated, BIGSERIAL and TEXT[] columns | 1, 1, 2 and 2 |
| DROP TABLE, DROP INDEX | about 85 and 20 |

Functions, CREATE TRIGGER, DO blocks, `LOCK TABLE` and the INSERT, UPDATE and DELETE backfills stay as `db.Exec`.

### Mongo coverage

- Creating a table makes the collection. Columns become `$jsonSchema` properties, with the type mapped to a `bsonType` and `NotNull` mapped to `required`. A `Model` builds the validator the way `CreateCollection` does now. `IfNotExists` is honored, and validation level and action come from mongomigrate options.
- Dropping a table drops the collection.
- Creating an index maps columns to keys, with `Desc` as -1, and carries over `Unique`, `Filter` (as `partialFilterExpression`) and the index name. Dropping one drops it by name on its collection.
- Adding a field updates the validator through `collMod`. A field with a default is backfilled with `$set` on documents that lack it.
- Dropping or renaming a field updates the validator, and the data gets `$unset` or `$rename`. Constraint actions return `ErrSchemaUnsupported`.
- `db.Exec` and `db.Query` return `ErrNotSupported`, as the executor does today.

### forge CLI

`writeGoMigrationFile` in `forge/cmd/forge/plugins/database.go` (used by `forge db create-go`) and `forge generate migration` emit:

```go
func init() {
	App.Add(
		func(ctx context.Context, db *migrate.DB) error {
			// Write your up migration here.
			return nil
		},
		func(ctx context.Context, db *migrate.DB) error {
			// Write your down migration here.
			return nil
		},
	)
}
```

The version lives only in the file name. The generated file needs the grove release that introduces `Add`. Whether the CLI checks the project's grove version or just says so is the builder's call.

### Loomplane conversion

- Every migration becomes `<version>_<name>.go` built on `App.Add`. That covers the hand-registered ones in `migrations.go`, the generated `20261101001200_frontend_capacity.go`, and the three built by constructors: `deployment_notifications`, `workload_controllers` and `workload_registrations`. `native_credential_fence` keeps its own group, `loomplane_native_credentials`, with the same `DependsOn`. The file name carries the version and name, so it must match today's exactly.
- Builders handle tables, columns, constraints and indexes. `db.Exec` handles the rest. Raw SQL that remains is rewritten in the house style of the October 11 migrations: uppercase keywords, one column or clause per line, aligned types, tab indentation.
- A DO block stays one DO block, DDL inside it included. About 25 files wrap a whole Up, or a guarded Down that locks a table, checks for retained rows and only then drops, in a single `DO $tag$ ... $tag$`. That one statement is what makes them atomic when a test calls them directly, and `TestNativeRetirementDowngradeRefusesConcurrentAdmission` even finds the blocked Down in `pg_stat_activity` by its dollar tag. Splitting one into builders would turn check-then-drop into a race. Reformat the SQL inside, but keep the statement whole and keep its tag.
- Every converted statement stays safe to re-run. Direct calls get no transaction, and the package tests replay migrations.
- `conduitpg.Migrations(...)` stays a live call. Inlining it would freeze the library's SQL.
- Some SQL is shared today. That covers `deploymentCapacityGuard`, `atomicFederationDDL`, `preparationDestinationFK`, `realtimeFinalizeUp` reusing `realtimeUp`, `serverCapacitySQL` used twice, `cloudCapacityIntentsDown` pulling a statement out of `cloudCompensationUp` by prefix, and `conduitpg.Migrations(...)`. Keep a shared unexported helper or inline it, whichever reads better. Behavior stays identical either way.
- Converted migrations run in a transaction by default. Any that can't opt out with `NoTransaction()`.
- `Group()` still returns a fresh group holding copies of App's migrations, because tests lift migrations into groups of their own. The hand-registered list and `statements()` go away.
- The comment on `App` about versions running ahead of the clock until 2026-11-02 stays. It's still true.
- Tests in `database/migrations` that run `[]string` slices, including the ones that replay every statement prefix, get rewritten to drive migrations through `Up` and `Down`. Integration tests that look migrations up by version keep working.

---

## Verification

### Grove

- Golden tests show the SQL each builder renders on Postgres, and the commands each produces on Mongo.
- Real-database tests run on Postgres through testcontainers, as pgmigrate's tests already do, and on Mongo through testcontainers, which is new for mongodriver.
- A transaction test fails a migration halfway. It checks that no partial schema and no applied record are left behind.
- An executor-form regression test shows existing migrate tests pass unchanged.
- A wrapper test runs `Up` through an executor wrapped by embedding.
- `make lint-all` passes with `GOWORK=off`.

### Loomplane

- Build two fresh databases, one from the old code and one from the new. `pg_dump --schema-only` must match, with function bodies compared after normalizing whitespace. The `grove_migrations` rows (group, version, name) must match too.
- Migrate a database with the old code, then start the new code. It must apply nothing.
- The full integration suite must give the same pass and fail results on the old and new code. Run it in shards by test name: one serial run got through only 43 tests in two hours on 2026-10-24. That run also showed three tests already failing on the old code, `TestDeploy_anAPIOnAzureContainerAppsThroughACloudTarget`, `TestCLI_planAndApproveADeploymentFromTheCommandLine` and `TestCloudAPIActualReleaseSwitchAndNextRelease`, so don't chase those as regressions.
- `golangci-lint` must be clean, run with `GOOS=linux` as CI does.

### Forge

The generator test is updated. `forge db create-go x` in a scratch project must compile against the new grove.

---

## Order of work

1. Grove: the `migrate` API, both renderers, tests, and the docs pages in `docs/content/docs/migrations/`.
2. Loomplane, as a rehearsal: convert against local grove through an out-of-repo `go.work` and run the full verification. Nothing gets committed. The point is to find API gaps before grove is tagged, and to fix them in grove.
3. With Rex's go-ahead, release grove through `release.yml`.
4. Loomplane for real: bump go.mod to the release, redo the conversion on current main, verify again, and commit straight away. The converted tree never sits uncommitted for long while the Codex sessions keep committing.
5. Forge: the CLI template, then its release with Rex's go-ahead.

The baseline for "identical to today" is the loomplane main commit that step 4 starts from. The comparison builds both databases from that same commit, once with the old migrations and once with the converted ones.

`20261101001200_frontend_capacity.go` is the exception (our call). It belongs to the Codex session's unfinished frontend capacity feature, which sits uncommitted in `internal/` and `tests/integration/`. Its target guard blocks changes to a cloud target while frontend service bindings on its connection name no target. Committed without the feature that sets those targets, it could block cloud-target changes main allows today. So convert the file in place to the builder form, leave it out of the commit, and leave the feature's other files alone. It ships when the feature does.

## Left to the builder

- Exact method names and signatures inside the rules above, and the option types.
- The shape of the renderer interface and the dialect lookup, within the wrapped-executor constraint.
- How the Mongo renderer reads a collection's current validator before changing it.
- Error wording, and file layout inside `migrate`.

## Our calls, for Rex to check

- `App.Add(up, down)` takes the version and name from the file name, as bun does.
- New-form migrations run in a transaction by default on Postgres.
- `Timestamp` maps to TIMESTAMPTZ and `JSON` maps to JSONB on Postgres.
- Builders end in `Exec(ctx) error`, not `(Result, error)`.
- `CREATE TRIGGER` stays raw SQL. There's no trigger builder.
- Loomplane's DO blocks stay whole, so about 25 files keep most of their SQL as `db.Exec`.
- Transactions wrap orchestrator runs only. Direct `Up` and `Down` calls in tests stay unwrapped.
- Grove is released before loomplane's conversion is committed, and the conversion is rehearsed against local grove first.
- On Mongo, adding, dropping and renaming a field also changes the data, not just the validator.
