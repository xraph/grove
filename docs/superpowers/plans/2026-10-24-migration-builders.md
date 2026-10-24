# Migration Builders Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Grove migrations that receive a `*migrate.DB` and build tables, columns, constraints and indexes through driver-agnostic builders, rendered by Postgres and MongoDB.

**Architecture:** `migrate` gains plain exported operation structs, fluent builders that produce them, a `*DB` handle, `Group.Add` (version and name from the file name) and orchestrator-level transactions behind an optional `TxExecutor`. Each driver's migrate package registers a `Renderer` by driver name: pgmigrate renders SQL and runs it through `exec.Exec`, mongomigrate runs commands through its native handle. Nothing in `migrate.Executor` changes.

**Tech Stack:** Go 1.25.7, pgx v5 (pgdriver), mongo-driver v2 (mongodriver), testify, testcontainers-go v0.42.0 (postgres module already in pgdriver; mongodb module new in mongodriver).

**Spec:** `docs/superpowers/specs/2026-10-24-migration-builders-design.md`. This plan covers the grove part (spec "Order of work" step 1). The loomplane conversion and the forge template get their own plans once this API exists.

## Global Constraints

- Run every Go command with `GOWORK=off`. `./...` stops at module boundaries: run tests in the root, `drivers/pgdriver` and `drivers/mongodriver` separately.
- `migrate.Executor` keeps its exact method set. New capabilities are optional interfaces found by type assertion.
- `migrate` imports only stdlib, `driver` and `schema`. Never pgx or the Mongo driver.
- Executor-form migrations (`MustRegister(&Migration{...})`) behave exactly as before: no transaction.
- Errors start with the package name (`migrate: ...`, `pgmigrate: ...`, `mongomigrate: ...`), wrap with `%w`, sentinels are `ErrXxx`.
- Lint rules from `AGENTS.md`: errcheck with `check-type-assertions` (comma-ok every assertion), govet `shadow`, revive stutter and lowercase error strings, three import groups (stdlib, third party, `github.com/xraph/grove/...`).
- Comments and docs: no em dashes or en dashes anywhere. Address the reader as "you".
- Commits: Conventional Commits (`feat(migrate): ...`), local only, no `Co-Authored-By` or any other trailer. Never push, tag or dispatch `release.yml`; Task 11 stops and asks Rex.
- Container tests skip without Docker. After any pgdriver or mongodriver change, read `-v` output for SKIP lines before calling it tested.

## Review Focus

- A table or column named with a reserved word (`group`, `order`, `user`) must work: every identifier is quoted. Pinned in Task 5 (golden) and Task 7 (live).
- A string default containing a single quote (`it's`) must render as `'it''s'`, never break the statement. Pinned in Task 5.
- A migration that panics inside a transaction must leave no partial schema, no applied record, and must not wedge the lock. Pinned in Task 4 (fake) and Task 7 (live).
- A wrapped executor without `Unwrap` must get an error naming its type, never a panic or a silent no-op. Pinned in Task 2.
- `Group.Add` called from a file not named `<14 digits>_<name>.go` must panic with a message naming the file and the expected pattern. Pinned in Task 3.

---

### Task 1: Operation types and builders

**Files:**
- Create: `migrate/schema_types.go`, `migrate/schema_ops.go`, `migrate/schema_builders.go`
- Test: `migrate/schema_builders_test.go`

**Interfaces:**
- Produces (all in package `migrate`):
  - `type TypeKind int` with constants `KindText` (= iota + 1), `KindSmallInt`, `KindInteger`, `KindBigInt`, `KindBigSerial`, `KindBoolean`, `KindTimestamp`, `KindDate`, `KindJSON`, `KindUUID`, `KindBytes`, `KindFloat`, `KindNumeric`, `KindArray`, `KindRaw`.
  - `type ColumnType struct { Kind TypeKind; Raw string; Precision, Scale int; Elem *ColumnType }`; vars `Text, SmallInt, Integer, BigInt, BigSerial, Boolean, Timestamp, Date, JSON, UUID, Bytes, Float ColumnType`; `func Numeric(precision, scale int) ColumnType`; `func ArrayOf(elem ColumnType) ColumnType`; `func Type(raw string) ColumnType`.
  - `type Action string`; constants `NoAction = "NO ACTION"`, `Restrict = "RESTRICT"`, `Cascade = "CASCADE"`, `SetNull = "SET NULL"`, `SetDefault = "SET DEFAULT"`.
  - `type Reference struct { Table string; Columns []string; OnDelete Action }`
  - `type DefaultValue struct { Value any; Expr string }` (Expr non-empty means an expression; otherwise Value is a literal, nil meaning NULL).
  - `type Column struct { Name string; Type ColumnType; NotNull bool; Default *DefaultValue; PrimaryKey, Unique bool; Check string; References *Reference; Identity bool; Generated string }`
  - `type ConstraintKind int` with `PrimaryKeyConstraint` (= iota + 1), `UniqueConstraint`, `CheckConstraint`, `ForeignKeyConstraint`; `type Constraint struct { Kind ConstraintKind; Name string; Columns []string; Check string; References *Reference }`
  - `type Operation interface{ schemaOperation() }` implemented by `*CreateTable`, `*DropTable`, `*AlterTable`, `*CreateIndex`, `*DropIndex`.
  - `type CreateTable struct { Table string; IfNotExists bool; Columns []Column; Constraints []Constraint; Model any; Options []any }`
  - `type DropTable struct { Table string; IfExists, Cascade bool }`
  - `type AlterKind int` with `AlterAddColumn` (= iota + 1), `AlterDropColumn`, `AlterSetNotNull`, `AlterDropNotNull`, `AlterSetDefault`, `AlterDropDefault`, `AlterSetType`, `AlterAddConstraint`, `AlterDropConstraint`, `AlterRenameColumn`, `AlterRenameTo`; `type AlterAction struct { Kind AlterKind; Column Column; Name, NewName string; Default *DefaultValue; Type ColumnType; Using string; Constraint Constraint; IfExists, IfNotExists, Cascade bool }`; `type AlterTable struct { Table string; Actions []AlterAction }`
  - `type IndexColumn struct { Name, Expr string; Desc bool }`; `type CreateIndex struct { Name, Table string; Columns []IndexColumn; Unique, IfNotExists, Concurrently bool; Using string; Include []string; Where string; Filter any; Options []any }`; `type DropIndex struct { Name, Table string; IfExists, Concurrently, Cascade bool }`
  - `type ColumnOption func(*Column) error`; `NotNull()`, `Default(value any)`, `DefaultExpr(sql string)`, `PrimaryKey()`, `Unique()`, `Check(sql string)`, `References(table string, columns ...string)`, `OnDelete(a Action)` (error if no `References` before it), `Identity()`, `GeneratedAs(sql string)`.
  - Builders, each holding a `*DB` (may be nil in tests), the op, and the first misuse error: `CreateTableQuery`, `DropTableQuery`, `AlterTableQuery`, `CreateIndexQuery`, `DropIndexQuery`. Each has `Operation() (<*Op>, error)` and `Exec(ctx context.Context) error` (Exec is wired in Task 2; here it may return the Operation error only).
  - `CreateTableQuery`: `IfNotExists()`, `Column(name string, typ ColumnType, opts ...ColumnOption)`, `PrimaryKey(cols ...string)`, `Unique(cols ...string)`, `Check(expr string)`, `ForeignKey(cols ...string)`, `References(table string, cols ...string)` and `OnDelete(a Action)` (apply to the last constraint, which must be a foreign key), `Named(name string)` (last constraint), `Model(model any)`, `With(opts ...any)`.
  - `AlterTableQuery`: `AddColumn(name string, typ ColumnType, opts ...ColumnOption)`, `DropColumn(name string)`, `SetNotNull(col string)`, `DropNotNull(col string)`, `SetDefault(col string, value any)`, `SetDefaultExpr(col, sql string)`, `DropDefault(col string)`, `SetType(col string, typ ColumnType)`, `Using(sql string)` (last action must be SetType), `AddPrimaryKey(cols ...string)`, `AddUnique(cols ...string)`, `AddCheck(sql string)`, `AddForeignKey(cols ...string)`, `References`, `OnDelete`, `Named` (last action must be AddConstraint; References and OnDelete need a foreign key), `DropConstraint(name string)`, `RenameColumn(from, to string)`, `RenameTo(name string)`, `IfExists()` (last action DropColumn or DropConstraint), `IfNotExists()` (last action AddColumn), `Cascade()` (last action DropColumn or DropConstraint).
  - `CreateIndexQuery`: `On(table string)`, `Columns(names ...string)`, `ColumnDesc(name string)`, `ColumnExpr(sql string)`, `Unique()`, `IfNotExists()`, `Concurrently()`, `Using(method string)`, `Include(cols ...string)`, `Where(sql string)`, `Filter(doc any)`, `With(opts ...any)`.
  - `DropIndexQuery`: `On(table string)`, `IfExists()`, `Concurrently()`, `Cascade()`. `DropTableQuery`: `IfExists()`, `Cascade()`.
  - `var ErrInvalidOperation = errors.New("migrate: invalid schema operation")`; every misuse and validation error wraps it.

- [ ] **Step 1: Write the failing tests** in `migrate/schema_builders_test.go`:
  - `TestCreateTableQuery_buildsOperation`: `NewDB(nil).NewCreateTable("t").IfNotExists().Column("id", Text, PrimaryKey()).Column("status", Text, NotNull(), Default("pending"), Check("status <> ''")).Column("project_id", Text, References("projects", "id"), OnDelete(Cascade)).Unique("org_id", "status").Named("t_org_status").ForeignKey("org_id").References("orgs", "id").OnDelete(Restrict).Operation()` returns no error and a `*CreateTable` equal (`assert.Equal`) to the literal struct with those three columns in order and constraints `[{UniqueConstraint, "t_org_status", ["org_id","status"]}, {ForeignKeyConstraint, "", ["org_id"], References: {orgs, [id], Restrict}}]`.
  - `TestCreateTableQuery_misuse` (table-driven): `Named` with no constraint; `References` after `Unique`; `OnDelete` before `References` as a column option; `Column` with `Type("")`; no columns and no model; `Model` plus columns. Each `Operation()` error satisfies `errors.Is(err, ErrInvalidOperation)`.
  - `TestAlterTableQuery_buildsOperation`: `AddColumn("tags", ArrayOf(Text), NotNull(), DefaultExpr("'{}'")).IfNotExists().DropColumn("old").IfExists().Cascade().DropNotNull("x").AddCheck("a > 0").Named("t_a_positive").DropConstraint("t_old").IfExists().RenameColumn("a", "b")` yields the six actions in order with the flags set on the right ones.
  - `TestAlterTableQuery_misuse`: `IfNotExists` after `DropColumn`; `Using` after `AddColumn`; `References` after `AddCheck`; empty table name.
  - `TestCreateIndexQuery_buildsOperation`: `NewCreateIndex("i").On("t").Columns("org_id").ColumnDesc("created_at").ColumnExpr("lower(name)").Unique().IfNotExists().Where("deleted_at IS NULL")` yields `Columns: [{Name:"org_id"},{Name:"created_at",Desc:true},{Expr:"lower(name)"}]`.
  - `TestCreateIndexQuery_misuse`: no `On`; no columns; empty name.

- [ ] **Step 2: Run to verify failure.** `GOWORK=off go test ./migrate/ -run 'Query_' -count=1`. Expected: build failure, undefined `NewDB`, `Text`, etc.

- [ ] **Step 3: Implement** the types in `schema_types.go`, the operations in `schema_ops.go`, and the builders in `schema_builders.go`. Add `type DB struct{ exec Executor }` and `func NewDB(exec Executor) *DB` with the five `New...` constructors here; Task 2 fills in the rest of `DB`. Builders record only the first misuse error and keep returning themselves, so a chain never panics.

- [ ] **Step 4: Run to verify pass.** Same command. Expected: PASS.

- [ ] **Step 5: Commit.** `git add migrate/schema_*.go && git commit -m "feat(migrate): add schema operations and builders"`

---

### Task 2: The DB handle, driver lookup and renderers

**Files:**
- Create: `migrate/db.go`, `migrate/render.go`
- Modify: `migrate/schema_builders.go` (wire every `Exec` to `db.Apply`)
- Test: `migrate/db_test.go`

**Interfaces:**
- Consumes: Task 1 operations and builders.
- Produces:
  - `func (db *DB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error)` and `Query(...) (driver.Rows, error)`, passed straight to the executor.
  - `func (db *DB) Executor() Executor`, `func (db *DB) Dialect() string` (driver name, or "" when unknown).
  - `func (db *DB) Apply(ctx context.Context, op Operation) error`.
  - `type DriverNamer interface{ DriverName() string }`, `type Unwrapper interface{ Unwrap() Executor }`.
  - `func DriverNameOf(exec Executor) (string, bool)`: follows `Unwrap` until something implements `DriverNamer`.
  - `func ExecutorAs[T any](exec Executor) (T, bool)`: follows `Unwrap` until the executor is a `T`.
  - `type Renderer interface{ Apply(ctx context.Context, exec Executor, op Operation) error }`, `func RegisterRenderer(driverName string, r Renderer)` (same locking style as `RegisterExecutor`).
  - `var ErrSchemaUnsupported = errors.New("migrate: schema operation not supported")`, `var ErrUnknownDriver = errors.New("migrate: cannot tell which driver this executor runs on")`, `func Unsupported(driverName, feature string) error` returning `fmt.Errorf("%w: %s does not support %s", ErrSchemaUnsupported, driverName, feature)`.
- Behavior: `Apply` validates nothing itself (builders did). Unknown driver returns `fmt.Errorf("%w: %T has no DriverName and no Unwrap() migrate.Executor", ErrUnknownDriver, exec)`. A driver with no registered renderer returns `Unsupported(name, "schema builders (import its migrate package)")`.

- [ ] **Step 1: Write the failing tests** in `migrate/db_test.go`, using a `fakeExecutor` that implements `Executor` plus `DriverName() string { return "fake" }` and records `Exec` queries, and a `recordingRenderer` registered as `"fake"`:
  - `TestDB_ApplyRoutesToRenderer`: `NewDB(exec).NewDropTable("t").IfExists().Exec(ctx)` hands the renderer `&DropTable{Table:"t", IfExists:true}` and the same executor.
  - `TestDB_ApplyThroughUnwrap`: an `unwrapping{inner}` wrapper with `Unwrap()` reaches the renderer, and the renderer receives the wrapper, not the inner executor.
  - `TestDB_ApplyUnknownDriver`: a struct embedding `Executor` with no `Unwrap` gives an error with `errors.Is(err, ErrUnknownDriver)` whose message contains the wrapper's type name.
  - `TestDB_ApplyNoRenderer`: an executor named `"nobody"` gives `errors.Is(err, ErrSchemaUnsupported)` and the message contains `nobody`.
  - `TestDB_ExecPassesThrough`: `db.Exec(ctx, "SELECT 1")` reaches the fake's `Exec`.
  - `TestExecutorAs_followsUnwrap`: `ExecutorAs[*fakeExecutor](unwrapping{fake})` returns the fake and true.
  - `TestDB_BuilderMisuseSkipsRenderer`: a builder with a misuse error returns it from `Exec` and the renderer records nothing.

- [ ] **Step 2: Run to verify failure.** `GOWORK=off go test ./migrate/ -run 'TestDB_|TestExecutorAs' -count=1`. Expected: build failure.

- [ ] **Step 3: Implement** `db.go` and `render.go`; replace each builder's `Exec` body with `op, err := q.Operation(); if err != nil { return err }; return q.db.Apply(ctx, op)`.

- [ ] **Step 4: Run to verify pass**, then the whole package: `GOWORK=off go test -race -count=1 ./migrate/`. Expected: PASS, including every pre-existing test.

- [ ] **Step 5: Commit.** `git commit -m "feat(migrate): add the DB handle and schema renderer registry"`

---

### Task 3: Group.Add and Define

**Files:**
- Create: `migrate/add.go`, `migrate/internal/addtest/20240102030405_create_widgets.go`
- Modify: `migrate/migration.go` (add the `Transactional` field)
- Test: `migrate/add_test.go` (package `migrate`), `migrate/add_external_test.go` (package `migrate_test`, imports `addtest`)

**Interfaces:**
- Produces:
  - `type Func func(ctx context.Context, db *DB) error`
  - `type Option func(*Migration)`, `func NoTransaction() Option`, `func Comment(text string) Option`
  - `func Define(version, name string, up, down Func, opts ...Option) *Migration`: sets `Transactional: true` before applying options; `Up` wraps `up` as `func(ctx, exec) error { return up(ctx, NewDB(exec)) }`; `Down` is nil when `down` is nil.
  - `func (g *Group) Add(up, down Func, opts ...Option)`: `runtime.Caller(1)`, then `g.MustRegister(Define(version, name, ...))`.
  - Unexported `parseMigrationFile(path string) (version, name string, err error)`: base name must match `^(\d{14})_(.+)\.go$`. The panic message reads `migrate: Add called from <base>; name the file <14 digit version>_<name>.go`.
  - `Migration.Transactional bool`, documented: runs Up or Down and its applied record in one transaction when the executor supports it; Add and Define set it, struct-registered migrations don't.
  - `addtest` package: `var Group = migrate.NewGroup("addtest")` and an `init()` calling `Group.Add(up, nil)`.

- [ ] **Step 1: Write the failing tests:**
  - `TestAdd_takesVersionAndNameFromFile` (external): `addtest.Group.Migrations()` has one migration with `Version "20240102030405"`, `Name "create_widgets"`, `Group "addtest"`, `Transactional true`, `Down nil`.
  - `TestParseMigrationFile` (table-driven): `/x/20240102030405_create_widgets.go` gives the version and name; `migrations.go`, `2024_create.go` and `20240102030405_.go` give errors.
  - `TestDefine_noTransaction`: `Define(v, n, up, down, NoTransaction()).Transactional` is false; `Comment("x")` sets `Comment`.
  - `TestDefine_upReceivesDB`: calling `m.Up(ctx, fake)` runs `up` with a `*DB` whose `Executor()` is `fake`.
  - `TestAdd_badFileNamePanics`: call the unexported `addFromFile(g, "/x/migrations.go", up, nil)` that `Add` delegates to, and `assert.PanicsWithValue` the message above.
  - `TestAdd_duplicateVersionPanics`: registering `Define` twice with one version into one group panics with `migrate: duplicate version`.

- [ ] **Step 2: Run to verify failure.** `GOWORK=off go test ./migrate/... -run 'Add|Define|ParseMigrationFile' -count=1`. Expected: build failure.

- [ ] **Step 3: Implement** `add.go` and the `addtest` file. `Add` calls `addFromFile(g, file, up, down, opts...)`.

- [ ] **Step 4: Run to verify pass.** Same command, then `GOWORK=off go test -race -count=1 ./migrate/...`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "feat(migrate): register migrations from their file names with Group.Add"`

---

### Task 4: Transactions in the orchestrator

**Files:**
- Create: `migrate/tx.go`
- Modify: `migrate/migrator.go` (`Migrate` and `Rollback` loops)
- Test: `migrate/tx_test.go`

**Interfaces:**
- Consumes: `Migration.Transactional` (Task 3).
- Produces: `type TxExecutor interface { BeginTx(ctx context.Context) error; CommitTx(ctx context.Context) error; RollbackTx(ctx context.Context) error }`, `var ErrTxUnsupported = errors.New("migrate: executor cannot run this migration in a transaction")`.
- Behavior: one unexported helper runs a step and its record. When `m.Transactional` and the executor is a `TxExecutor`, it calls `BeginTx`; `ErrTxUnsupported` from `BeginTx` means run without a transaction. On error or panic it calls `RollbackTx` with `context.WithoutCancel(ctx)`, then returns the error or re-panics. `Rollback` uses the same helper with `Down` (nil Down still records the removal inside the transaction).

- [ ] **Step 1: Write the failing tests** with a `txFake` that implements `Executor` and `TxExecutor` and appends every call to a log (`BEGIN`, `exec:<q>`, `record:<version>`, `remove:<version>`, `COMMIT`, `ROLLBACK`):
  - `TestMigrate_transactionalCommitsWithRecord`: one `Define`d migration whose up runs `db.Exec("X")` logs `[BEGIN exec:X record:v1 COMMIT]` (lock calls filtered out).
  - `TestMigrate_transactionalRollsBackOnError`: up returns an error; log ends `[BEGIN ROLLBACK]`, nothing recorded, `Migrate` returns the error.
  - `TestMigrate_transactionalRollsBackOnPanic`: up panics; `assert.Panics`; log contains `ROLLBACK` and no `record`; `ReleaseLock` was still called.
  - `TestMigrate_structMigrationStaysPlain`: a `&Migration{Up: ...}` (Transactional false) on `txFake` logs no `BEGIN`.
  - `TestMigrate_beginUnsupportedRunsPlain`: `BeginTx` returns `ErrTxUnsupported`; the migration runs and records with no `COMMIT`.
  - `TestRollback_transactional`: logs `[BEGIN exec:D remove:v1 COMMIT]`.
  - `TestMigrate_executorWithoutTx`: the existing plain fake runs a `Define`d migration fine.

- [ ] **Step 2: Run to verify failure.** `GOWORK=off go test ./migrate/ -run 'TestMigrate_|TestRollback_' -count=1`. Expected: FAIL or build failure on `TxExecutor`.

- [ ] **Step 3: Implement** `tx.go` and the helper, and call it from both loops.

- [ ] **Step 4: Run to verify pass.** `GOWORK=off go test -race -count=1 ./migrate/...`. Expected: PASS, every pre-existing migrator test included.

- [ ] **Step 5: Commit.** `git commit -m "feat(migrate): run Add migrations and their records in one transaction"`

---

### Task 5: Postgres rendering for tables

**Files:**
- Create: `drivers/pgdriver/pgmigrate/schema.go`
- Test: `drivers/pgdriver/pgmigrate/schema_table_test.go` (no database)

**Interfaces:**
- Consumes: Task 1 operations.
- Produces: `func Render(op migrate.Operation) ([]string, error)`, and `type Renderer struct{}` with `Apply(ctx context.Context, exec migrate.Executor, op migrate.Operation) error` (Render, then `exec.Exec` each statement in order, wrapping errors as `pgmigrate: <statement kind> <name>: %w`).
- Rules: every identifier through `(&pgdriver.PgDialect{}).Quote`. Types: Text `TEXT`, SmallInt `SMALLINT`, Integer `INTEGER`, BigInt `BIGINT`, BigSerial `BIGSERIAL`, Boolean `BOOLEAN`, Timestamp `TIMESTAMPTZ`, Date `DATE`, JSON `JSONB`, UUID `UUID`, Bytes `BYTEA`, Float `DOUBLE PRECISION`, Numeric `NUMERIC(p, s)` (or `NUMERIC` when both are 0), ArrayOf `<elem>[]`, Raw verbatim. Column clause order: type, `GENERATED ALWAYS AS (<expr>) STORED` or `GENERATED ALWAYS AS IDENTITY`, `NOT NULL`, `DEFAULT <v>`, `PRIMARY KEY`, `UNIQUE`, `CHECK (<expr>)`, `REFERENCES <t> (<cols>)` and ` ON DELETE <a>`. Literal defaults: strings single-quoted with `'` doubled, bools `TRUE`/`FALSE`, integers and floats with `strconv`, nil `NULL`; any other type is an `ErrInvalidOperation`. `Model` without columns renders through `pgdriver.New().NewCreateTable(model)` (plus `IfNotExists()`), then `Build()`. Driver `Options` on any operation are an error (`pgmigrate: postgres takes no driver options`).

- [ ] **Step 1: Write the failing golden tests:**
  - `TestRender_createTable`: the Task 1 `TestCreateTableQuery_buildsOperation` chain (plus `Column("evidence", migrate.JSON, migrate.NotNull(), migrate.DefaultExpr("'{}'"))` and `Column("seq", migrate.BigInt, migrate.Identity())`) renders exactly:

    ```
    CREATE TABLE IF NOT EXISTS "t" (
    	"id" TEXT PRIMARY KEY,
    	"status" TEXT NOT NULL DEFAULT 'pending' CHECK (status <> ''),
    	"project_id" TEXT REFERENCES "projects" ("id") ON DELETE CASCADE,
    	"evidence" JSONB NOT NULL DEFAULT '{}',
    	"seq" BIGINT GENERATED ALWAYS AS IDENTITY,
    	CONSTRAINT "t_org_status" UNIQUE ("org_id", "status"),
    	FOREIGN KEY ("org_id") REFERENCES "orgs" ("id") ON DELETE RESTRICT
    )
    ```
  - `TestRender_types` (table-driven over every kind, `Numeric(10, 2)`, `ArrayOf(Text)` and `Type("INET")`).
  - `TestRender_defaultQuoting`: `Default("it's")` gives `DEFAULT 'it''s'`; `Default(true)` gives `DEFAULT TRUE`; `Default(nil)` gives `DEFAULT NULL`; `Default(struct{}{})` errors.
  - `TestRender_reservedWords`: table `"order"` with column `"group"` renders both quoted.
  - `TestRender_generatedColumn`: `GeneratedAs("lower(name)")` renders `GENERATED ALWAYS AS (lower(name)) STORED`.
  - `TestRender_dropTable`: `DROP TABLE IF EXISTS "t" CASCADE`.
  - `TestRender_model`: a model struct with `grove:"table:widgets"` and a pk field renders the same SQL `pgdriver.New().NewCreateTable(model).IfNotExists().Build()` returns.
  - `TestRender_rejectsOptions`: `.With("x")` errors.

- [ ] **Step 2: Run to verify failure.** `cd drivers/pgdriver && GOWORK=off go test ./pgmigrate/ -run 'TestRender_' -count=1`. Expected: build failure.

- [ ] **Step 3: Implement** `Render` for `*CreateTable` and `*DropTable` (other operations return `migrate.Unsupported("pg", "<op> (not yet)")` until Task 6) and `Renderer.Apply`.

- [ ] **Step 4: Run to verify pass.** Same command. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "feat(pgmigrate): render table operations"`

---

### Task 6: Postgres rendering for ALTER and indexes

**Files:**
- Modify: `drivers/pgdriver/pgmigrate/schema.go`
- Test: `drivers/pgdriver/pgmigrate/schema_alter_test.go`

**Interfaces:**
- Consumes: Task 5's `Render` and helpers.
- Rules: non-rename actions join into one `ALTER TABLE "t" <a>, <b>` statement, in order. Each `RenameColumn` and `RenameTo` is its own statement, emitted where it sits in the sequence, and it splits the joined runs around it. `CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] "i" ON "t" [USING m] (<cols>) [INCLUDE (<cols>)] [WHERE <sql>]`: a named column is quoted, `Desc` appends ` DESC`, an `Expr` renders as `(<expr>)`. `DROP INDEX [CONCURRENTLY] [IF EXISTS] "i" [CASCADE]`. A `Filter` on a pg index is `migrate.Unsupported("pg", "index filter documents")`.

- [ ] **Step 1: Write the failing golden tests:**
  - `TestRender_alterTable`: the Task 1 `TestAlterTableQuery_buildsOperation` chain renders exactly two statements:

    ```
    ALTER TABLE "t" ADD COLUMN IF NOT EXISTS "tags" TEXT[] NOT NULL DEFAULT '{}', DROP COLUMN IF EXISTS "old" CASCADE, ALTER COLUMN "x" DROP NOT NULL, ADD CONSTRAINT "t_a_positive" CHECK (a > 0), DROP CONSTRAINT IF EXISTS "t_old"
    ALTER TABLE "t" RENAME COLUMN "a" TO "b"
    ```
  - `TestRender_alterColumnActions`: `SetNotNull`, `SetDefault("x")`, `SetDefaultExpr("now()")`, `DropDefault`, `SetType(...).Using("x::bigint")` and `AddForeignKey("a","b").References("p","a","b").OnDelete(migrate.Cascade)` each render their exact clause.
  - `TestRender_createIndex`: the Task 1 index chain renders `CREATE UNIQUE INDEX IF NOT EXISTS "i" ON "t" ("org_id", "created_at" DESC, (lower(name))) WHERE deleted_at IS NULL`; `Using("gin")` and `Include("a")` place their clauses as above; `Concurrently()` puts `CONCURRENTLY` before `IF NOT EXISTS`.
  - `TestRender_dropIndex`: `DROP INDEX CONCURRENTLY IF EXISTS "i" CASCADE`.
  - `TestRender_renameSplits`: `AddColumn(a)`, `RenameColumn(b, c)`, `DropColumn(d)` renders three statements in that order.

- [ ] **Step 2: Run to verify failure.** `cd drivers/pgdriver && GOWORK=off go test ./pgmigrate/ -run 'TestRender_' -count=1`. Expected: FAIL on the new tests.

- [ ] **Step 3: Implement** the alter and index rendering.

- [ ] **Step 4: Run to verify pass.** Same command. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "feat(pgmigrate): render ALTER TABLE and index operations"`

---

### Task 7: pgmigrate executor support, proven on a live Postgres

**Files:**
- Modify: `drivers/pgdriver/pgmigrate/executor.go` (add `DriverName`, the `TxExecutor` methods, and `migrate.RegisterRenderer("pg", Renderer{})` in the existing `init`)
- Test: `drivers/pgdriver/pgmigrate/schema_live_test.go` (reuses `startPostgres` from `executor_test.go`)

**Interfaces:**
- Produces: `func (e *Executor) DriverName() string` returning `"pg"`. `BeginTx`, `CommitTx` and `RollbackTx` run `BEGIN`, `COMMIT` and `ROLLBACK` on `e.dedicated`, and `BeginTx` returns `migrate.ErrTxUnsupported` when `e.dedicated` is nil. Compile-time assertions `var _ migrate.TxExecutor = (*Executor)(nil)` and `var _ migrate.DriverNamer = (*Executor)(nil)`.

- [ ] **Step 1: Write the failing live tests.** Each opens a fresh container DB, builds a `migrate.Group` from `migrate.Define`, and runs `migrate.NewOrchestrator(New(db), g).Migrate(ctx)`.
  - `TestLive_builderMatchesHandWrittenSQL`: a builder migration creates table `b` with the Task 5 column and constraint mix, without names except one. A raw `db.Exec` creates the same table as `h` from hand-written SQL (constraints inline exactly as the builder renders them). Then `SELECT conname, pg_get_constraintdef(oid)` for each table, with the table prefix stripped from `conname`, is equal for `b` and `h`. The same holds for `pg_get_indexdef` of an index created both ways.
  - `TestLive_transactionLeavesNothingOnFailure`: a migration creates a table, then `db.Exec("SELECT 1/0")`. Afterwards `to_regclass('x')` is NULL, `grove_migrations` has no row for the version, and a second `Migrate` with the failing statement removed succeeds. That shows the lock was released.
  - `TestLive_panicLeavesNothing`: the same with a panic after the CREATE. Recover it in the test, then check as above.
  - `TestLive_noTransactionAllowsConcurrently`: `Define(..., migrate.NoTransaction())` with `NewCreateIndex(...).Concurrently()` succeeds; the same index under a transactional migration fails with the Postgres message containing `cannot run inside a transaction block`.
  - `TestLive_reservedWords`: `NewCreateTable("order").Column("group", migrate.Text)` applies and `SELECT "group" FROM "order"` works.
  - `TestLive_wrapperCountsStatements`: `step.Up(ctx, counting{inner: New(db)})`, where `counting` embeds `migrate.Executor`, counts `Exec` and has `Unwrap`. The count equals `len(Render(op))`, run outside any orchestrator.

- [ ] **Step 2: Run to verify failure.** `cd drivers/pgdriver && GOWORK=off go test -v -count=1 ./pgmigrate/ -run 'TestLive_'`. Expected: FAIL (renderer not registered, no `DriverName`). Check the output has no SKIP lines; Docker must be running.

- [ ] **Step 3: Implement** the executor methods and the registration.

- [ ] **Step 4: Run to verify pass**, then the whole module: `cd drivers/pgdriver && GOWORK=off go test -race -count=1 -v ./... 2>&1 | grep -E '^(--- |ok|FAIL|PASS)|SKIP'`. Expected: all PASS, no SKIP in pgmigrate.

- [ ] **Step 5: Commit.** `git commit -m "feat(pgmigrate): register the renderer and run migrations in transactions"`

---

### Task 8: Mongo rendering for collections and indexes

**Files:**
- Create: `drivers/mongodriver/mongomigrate/schema.go`, `drivers/mongodriver/mongomigrate/live_test.go`
- Modify: `drivers/mongodriver/mongomigrate/executor.go` (`DriverName`, `RegisterRenderer("mongo", Renderer{})` in the existing `init`), `drivers/mongodriver/go.mod` (add `github.com/testcontainers/testcontainers-go v0.42.0` and `.../modules/mongodb v0.42.0`)
- Test: `drivers/mongodriver/mongomigrate/schema_test.go` (no server), `live_test.go`

**Interfaces:**
- Produces: `func (e *Executor) DriverName() string` returning `"mongo"`; `type Renderer struct{}` with `Apply`; `func Native(db *migrate.DB) (*mongodriver.MongoDB, error)` (via `migrate.ExecutorAs[*Executor]`, with error `mongomigrate: executor %T is not a mongomigrate executor`).
- Rules: Apply reaches the native DB through `migrate.ExecutorAs[*Executor](exec)`. CreateTable with columns builds `{bsonType: "object", properties, required}`. bsonTypes: Text `string`, SmallInt and Integer `int`, BigInt and BigSerial `long`, Boolean `bool`, Timestamp and Date `date`, JSON `object`, UUID `string`, Bytes `binData`, Float `double`, Numeric `decimal`, ArrayOf `{bsonType: "array", items: {bsonType: <elem>}}`, and Raw passes through as the bsonType. A column without `NotNull` gets `[<type>, "null"]`, matching `jsonschema.go` for pointers, and `NotNull` columns go in `required`. These return `migrate.Unsupported("mongo", <feature>)`: defaults, PrimaryKey, Unique, Check, References, Identity, Generated, any table constraint, `Cascade` on drops, `Where`, `Using`, `Include`, `Concurrently` and `ColumnExpr`. `Model` goes through `db.NewCreateCollection(model)`, as `CreateCollection` does. `With` accepts `CollectionOption` values (validation level and action, additional properties) and errors on anything else. `IfNotExists` skips an existing collection. CreateIndex maps keys in order (`Desc` as -1, otherwise 1), `Unique`, the name, and `Filter` as `partialFilterExpression`. DropIndex needs `On`, and with `IfExists` it ignores `IndexNotFound`. DropTable drops the collection.

- [ ] **Step 1: Write the failing tests.**
  - `schema_test.go`:
    - `TestJSONSchema_columns`: `Column("email", migrate.Text, migrate.NotNull())`, `Column("bio", migrate.Text)` and `Column("tags", migrate.ArrayOf(migrate.Text), migrate.NotNull())` give `properties.email.bsonType "string"`, `properties.bio.bsonType ["string","null"]`, a tags array of strings, and `required ["email","tags"]`.
    - `TestIndexModel_keysAndOptions`: `Columns("app_id").ColumnDesc("created_at").Unique().Filter(bson.M{"username": bson.M{"$gt": ""}})` gives keys `{app_id: 1, created_at: -1}`, unique, the name, and the partial filter.
    - `TestRender_unsupported` (table-driven over each unsupported feature): `errors.Is(err, migrate.ErrSchemaUnsupported)` and the message names the feature.
  - `live_test.go`: `startMongo(t)` (testcontainers `mongodb.Run(ctx, "mongo:7")`, skipping like `startPostgres`), then:
    - `TestLive_createCollectionValidates`: a migration run through `migrate.NewOrchestrator(New(mdb), g)` creates `users` with a NotNull `email`. Inserting a doc without `email` fails validation, and one with it succeeds.
    - `TestLive_indexes`: a unique index rejects a duplicate. The partial unique index from the authsome example allows two docs with `username: ""`.
    - `TestLive_dropIndexIfExists`: dropping a missing index with `IfExists` succeeds, and without it fails.
    - `TestLive_native`: `Native(db)` returns the driver handle inside an `Add`-style migration.

- [ ] **Step 2: Run to verify failure.** `cd drivers/mongodriver && GOWORK=off go test -v -count=1 ./mongomigrate/`. Expected: build failure. Once it builds, check for SKIP lines.

- [ ] **Step 3: Implement** `schema.go`, the executor additions and the go.mod dependency (`GOWORK=off go get` both testcontainers modules, then `GOWORK=off go mod tidy` in `drivers/mongodriver`).

- [ ] **Step 4: Run to verify pass.** `cd drivers/mongodriver && GOWORK=off go test -race -count=1 -v ./... 2>&1 | grep -E '^(--- |ok|FAIL|PASS)|SKIP'`. Expected: all PASS, no SKIP in mongomigrate.

- [ ] **Step 5: Commit.** `git commit -m "feat(mongomigrate): render collections, validators and indexes"`

---

### Task 9: Mongo field changes

**Files:**
- Modify: `drivers/mongodriver/mongomigrate/schema.go`
- Test: additions to `schema_test.go` and `live_test.go`

**Interfaces:**
- Rules for `*migrate.AlterTable` on Mongo. Validator edits apply only when the collection already has a `$jsonSchema` validator, read through `Database().ListCollectionSpecifications` and written with `collMod`. Data changes go through `UpdateMany`.
  - `AddColumn`: with a literal `Default`, first `$set` the value on documents where the field is missing, then add the property (and `required` when NotNull). NotNull without a default fails with `mongomigrate: cannot add required field <f> to <coll>: <n> documents lack it` when any document lacks the field, mirroring Postgres. `IfNotExists` skips the action when the property already exists. `DefaultExpr` is unsupported.
  - `DropColumn`: `$unset` from every document, then remove it from `properties` and `required`. `IfExists` makes a missing property a no-op.
  - `RenameColumn`: `$rename` in every document, then rename it in `properties` and `required`.
  - Every other action returns `migrate.Unsupported("mongo", ...)`.
- Unexported pure helpers `addProperty`, `dropProperty` and `renameProperty` take and return `bson.M` schemas, so they test without a server.

- [ ] **Step 1: Write the failing tests:**
  - `TestValidatorEdits` (table-driven over the three helpers): `required` and `properties` before and after.
  - `TestLive_addFieldBackfills`: collection with docs, `AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default("active"))`; every doc has `status: "active"` and a new doc without `status` fails validation.
  - `TestLive_addRequiredFieldWithoutDefaultFails`: the error message names the count.
  - `TestLive_dropAndRenameField`: the field disappears from docs and the validator, and a renamed field carries its values.
  - `TestLive_alterWithoutValidator`: on a collection with no validator, `AddColumn` with a default backfills and creates no validator.

- [ ] **Step 2: Run to verify failure.** `cd drivers/mongodriver && GOWORK=off go test -v -count=1 ./mongomigrate/ -run 'Validator|TestLive_(add|drop|alter)'`. Expected: FAIL.

- [ ] **Step 3: Implement** the alter rendering.

- [ ] **Step 4: Run to verify pass.** Same command, then the whole module with `-race`. Expected: PASS, no SKIP.

- [ ] **Step 5: Commit.** `git commit -m "feat(mongomigrate): add, drop and rename fields with their validators"`

---

### Task 10: Docs and full checks

**Files:**
- Create: `docs/content/docs/migrations/builders.mdx`
- Modify: `docs/content/docs/migrations/meta.json` (pages `["overview", "builders", "multi-module"]`), `docs/content/docs/migrations/overview.mdx` (Defining Migrations shows `App.Add` first and the struct form after it; replace the sentence "It has no query builder, so migrations are written in SQL"), and the migration sections of `docs/content/docs/drivers/postgres.mdx` and `mongodb.mdx`

**Interfaces:**
- Consumes: the final API from Tasks 1 to 9. Every example in the docs must compile against it.

- [ ] **Step 1: Write `builders.mdx`.** Start with a full `App.Add` migration for Postgres, then the same table for Mongo in its own file. Follow with the builder reference (tables, columns, types, constraints, alter actions, indexes), the transaction rules including `NoTransaction` and `Concurrently`, the unsupported-feature errors, and `Unwrap` for executor wrappers. Use rex-voice: you-address, no em dashes, practical first.
- [ ] **Step 2: Check the prose.** `grep -rn '—\|–' docs/content/docs/migrations docs/content/docs/drivers/postgres.mdx docs/content/docs/drivers/mongodb.mdx`. Expected: no output.
- [ ] **Step 3: Build the docs.** `make docs-build`. Expected: success.
- [ ] **Step 4: Run the full checks.** `GOWORK=off make test-all`, `make lint-all`, `goimports -l -local github.com/xraph/grove .`, then `GOWORK=off go mod tidy` in the root, `drivers/pgdriver` and `drivers/mongodriver`, followed by `git diff --exit-code -- '*.mod' '*.sum'`. Expected: tests pass, lint clean, goimports silent, and tidy changes nothing beyond Task 8's committed go.mod.
- [ ] **Step 5: Commit.** `git commit -m "docs(migrate): document the migration builders"`

---

### Task 11: Release preparation, then stop

**Files:**
- Modify: every go.mod that requires `github.com/xraph/grove` (via `./bumpmod.sh`)

- [ ] **Step 1: Bump the labels.** `./bumpmod.sh v1.10.0 github.com/xraph/grove`, so pgdriver and mongodriver require the root version that has the new API. Then `GOWORK=off ./updatemod.sh`.
- [ ] **Step 2: Build every module.** `GOWORK=off make test-all`. Expected: PASS.
- [ ] **Step 3: Commit.** `git commit -m "chore: bump mods to v1.10.0"`
- [ ] **Step 4: Stop.** Report to Rex the commits on local main, the test and lint results, and that `v1.10.0` is ready. Ask for the go-ahead to push main and run `gh workflow run release.yml --ref main -f tag=v1.10.0`. Do nothing further without it.
