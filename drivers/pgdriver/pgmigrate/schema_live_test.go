package pgmigrate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

// liveLockWait is how long a second Migrate waits for the migration lock. A
// lock a failed migration leaked would otherwise stall the test for the
// orchestrator's default budget.
const liveLockWait = 5 * time.Second

// migrateLive runs ms as one group through a new executor on db, the way an
// application would on boot.
func migrateLive(ctx context.Context, db *pgdriver.PgDB, ms ...*migrate.Migration) error {
	g := migrate.NewGroup("app")
	g.MustRegister(ms...)
	_, err := migrate.NewOrchestrator(New(db), g).SetLockTimeout(liveLockWait).Migrate(ctx)
	return err
}

// rollbackLive rolls back the last migration of the group, through a new
// executor on db.
func rollbackLive(ctx context.Context, db *pgdriver.PgDB, ms ...*migrate.Migration) error {
	g := migrate.NewGroup("app")
	g.MustRegister(ms...)
	_, err := migrate.NewOrchestrator(New(db), g).SetLockTimeout(liveLockWait).Rollback(ctx)
	return err
}

// liveValue runs a query that returns one row with one column. A T of *string
// reads a NULL as nil.
func liveValue[T any](t *testing.T, db *pgdriver.PgDB, query string, args ...any) T {
	t.Helper()
	var v T
	require.NoError(t, db.QueryRow(context.Background(), query, args...).Scan(&v))
	return v
}

// liveRows runs a query whose columns are all text and returns each row as
// its columns joined with " | ", so a test compares whole result sets.
func liveRows(t *testing.T, db *pgdriver.PgDB, columns int, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(context.Background(), query, args...)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var out []string
	for rows.Next() {
		cells := make([]string, columns)
		dest := make([]any, columns)
		for i := range cells {
			dest[i] = &cells[i]
		}
		require.NoError(t, rows.Scan(dest...))
		out = append(out, strings.Join(cells, " | "))
	}
	require.NoError(t, rows.Err())
	return out
}

// tableExists reports whether name resolves to a relation in the database.
func tableExists(t *testing.T, db *pgdriver.PgDB, name string) bool {
	t.Helper()
	return liveValue[*string](t, db, "SELECT to_regclass($1)::text", name) != nil
}

// recordedVersions counts the grove_migrations rows for version.
func recordedVersions(t *testing.T, db *pgdriver.PgDB, version string) int {
	t.Helper()
	return liveValue[int](t, db, "SELECT count(*) FROM grove_migrations WHERE version = $1", version)
}

// advisoryLocks counts the advisory locks held in this test's database. It is
// the proof a migration released the lock, which a second Migrate can't give:
// advisory locks are reentrant, so a pool that hands the same session to the
// second run would take a leaked lock without noticing.
func advisoryLocks(t *testing.T, db *pgdriver.PgDB) int {
	t.Helper()
	return liveValue[int](t, db, `SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory'
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`)
}

func TestLive_builderMatchesHandWrittenSQL(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	_, err := db.Exec(ctx, `CREATE TABLE parents (id TEXT PRIMARY KEY); CREATE TABLE orgs (id TEXT PRIMARY KEY)`)
	require.NoError(t, err)

	up := func(ctx context.Context, mdb *migrate.DB) error {
		createErr := mdb.NewCreateTable("b").
			Column("id", migrate.BigInt, migrate.Identity(), migrate.PrimaryKey()).
			Column("status", migrate.Text, migrate.NotNull(), migrate.Default("pending"), migrate.Check("status <> ''")).
			Column("email", migrate.Text, migrate.Unique()).
			Column("parent_id", migrate.Text, migrate.References("parents", "id"), migrate.OnDelete(migrate.Cascade)).
			Column("payload", migrate.JSON, migrate.NotNull(), migrate.DefaultExpr("'{}'")).
			Column("org_id", migrate.Text).
			Column("qty", migrate.Integer).
			Column("total", migrate.Integer, migrate.GeneratedAs("qty * 2")).
			Unique("org_id", "status").Named("b_org_status").
			Check("qty >= 0").
			ForeignKey("org_id").References("orgs", "id").OnDelete(migrate.Restrict).
			Exec(ctx)
		if createErr != nil {
			return createErr
		}
		return mdb.NewCreateIndex("b_org_qty_idx").On("b").
			Columns("org_id").ColumnDesc("qty").ColumnExpr("lower(email)").
			Unique().Using("btree").Include("email").Where("status <> 'closed'").
			Exec(ctx)
	}
	require.NoError(t, migrateLive(ctx, db, migrate.Define("20260101000001", "create_b", up, nil)))

	// The same schema in the SQL a person would write, laid out the way the
	// builder renders it. Only the constraint named in the builder keeps a
	// name of its own, and it carries the table's prefix so the two tables'
	// names line up once the prefix comes off.
	_, err = db.Exec(ctx, `CREATE TABLE "h" (
		"id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		"status" TEXT NOT NULL DEFAULT 'pending' CHECK (status <> ''),
		"email" TEXT UNIQUE,
		"parent_id" TEXT REFERENCES "parents" ("id") ON DELETE CASCADE,
		"payload" JSONB NOT NULL DEFAULT '{}',
		"org_id" TEXT,
		"qty" INTEGER,
		"total" INTEGER GENERATED ALWAYS AS (qty * 2) STORED,
		CONSTRAINT "h_org_status" UNIQUE ("org_id", "status"),
		CHECK (qty >= 0),
		FOREIGN KEY ("org_id") REFERENCES "orgs" ("id") ON DELETE RESTRICT
	)`)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `CREATE UNIQUE INDEX "h_org_qty_idx" ON "h" USING btree
		("org_id", "qty" DESC, (lower(email))) INCLUDE ("email") WHERE status <> 'closed'`)
	require.NoError(t, err)

	constraints := func(table string) []string {
		return liveRows(t, db, 2, `SELECT substr(c.conname, length(t.relname) + 1), pg_get_constraintdef(c.oid)
			FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname = $1 ORDER BY 1`, table)
	}
	columns := func(table string) []string {
		return liveRows(t, db, 7, `SELECT column_name, data_type, is_nullable, coalesce(column_default, ''),
				is_identity, is_generated, coalesce(generation_expression, '')
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, table)
	}
	index := func(table string) string {
		name := table + "_org_qty_idx"
		def := liveValue[string](t, db, "SELECT indexdef FROM pg_indexes WHERE tablename = $1 AND indexname = $2", table, name)
		return strings.NewReplacer(name, "IDX", " public."+table+" ", " public.TBL ").Replace(def)
	}

	built := constraints("b")
	// Seven: the primary key, the column unique and check, the column foreign
	// key, and the table's unique, check and foreign key. A bare equality
	// would pass for two empty lists.
	require.Len(t, built, 7)
	assert.Contains(t, built, `_org_status | UNIQUE (org_id, status)`)
	assert.Equal(t, built, constraints("h"))
	assert.Equal(t, columns("b"), columns("h"))
	assert.Len(t, columns("b"), 8)
	assert.Equal(t, index("b"), index("h"))
	assert.Contains(t, index("b"), "DESC")
}

func TestLive_transactionLeavesNothingOnFailure(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	const version = "20260101000002"
	failing := migrate.Define(version, "create_x", func(ctx context.Context, mdb *migrate.DB) error {
		if err := mdb.NewCreateTable("x").Column("id", migrate.Text).Exec(ctx); err != nil {
			return err
		}
		_, err := mdb.Exec(ctx, "SELECT 1/0")
		return err
	}, nil)

	err := migrateLive(ctx, db, failing)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "division by zero")

	assert.False(t, tableExists(t, db, "x"), "the table the failed migration created must be rolled back")
	assert.Zero(t, recordedVersions(t, db, version))
	assert.Zero(t, advisoryLocks(t, db), "the failed run must release the migration lock")

	fixed := migrate.Define(version, "create_x", func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewCreateTable("x").Column("id", migrate.Text).Exec(ctx)
	}, nil)
	require.NoError(t, migrateLive(ctx, db, fixed))
	assert.True(t, tableExists(t, db, "x"))
	assert.Equal(t, 1, recordedVersions(t, db, version))
	assert.Zero(t, advisoryLocks(t, db))
}

func TestLive_panicLeavesNothing(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	const version = "20260101000003"
	panicking := migrate.Define(version, "create_x", func(ctx context.Context, mdb *migrate.DB) error {
		if err := mdb.NewCreateTable("x").Column("id", migrate.Text).Exec(ctx); err != nil {
			return err
		}
		panic("boom")
	}, nil)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = migrateLive(ctx, db, panicking)
	}()
	require.Equal(t, "boom", recovered, "the panic must keep unwinding with its own value")

	assert.False(t, tableExists(t, db, "x"), "the table created before the panic must be rolled back")
	assert.Zero(t, recordedVersions(t, db, version))
	assert.Zero(t, advisoryLocks(t, db), "a panic must still release the migration lock")

	fixed := migrate.Define(version, "create_x", func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewCreateTable("x").Column("id", migrate.Text).Exec(ctx)
	}, nil)
	require.NoError(t, migrateLive(ctx, db, fixed))
	assert.True(t, tableExists(t, db, "x"))
}

func TestLive_noTransactionAllowsConcurrently(t *testing.T) {
	concurrentIndex := func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewCreateIndex("c_name_idx").On("c").Columns("name").Concurrently().Exec(ctx)
	}
	setup := func(t *testing.T) *pgdriver.PgDB {
		t.Helper()
		db := openLive(t)
		_, err := db.Exec(context.Background(), `CREATE TABLE c (name TEXT)`)
		require.NoError(t, err)
		return db
	}

	t.Run("outside a transaction", func(t *testing.T) {
		db := setup(t)
		m := migrate.Define("20260101000004", "index_c", concurrentIndex, nil, migrate.NoTransaction())
		require.NoError(t, migrateLive(context.Background(), db, m))
		assert.True(t, tableExists(t, db, "c_name_idx"))
		assert.Equal(t, 1, recordedVersions(t, db, "20260101000004"))
	})

	t.Run("inside a transaction", func(t *testing.T) {
		db := setup(t)
		m := migrate.Define("20260101000004", "index_c", concurrentIndex, nil)
		err := migrateLive(context.Background(), db, m)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot run inside a transaction block")
		assert.False(t, tableExists(t, db, "c_name_idx"))
		assert.Zero(t, recordedVersions(t, db, "20260101000004"))
		assert.Zero(t, advisoryLocks(t, db))
	})
}

func TestLive_reservedWords(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	up := func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewCreateTable("order").Column("group", migrate.Text).Exec(ctx)
	}
	require.NoError(t, migrateLive(ctx, db, migrate.Define("20260101000005", "create_order", up, nil)))

	_, err := db.Exec(ctx, `INSERT INTO "order" ("group") VALUES ('admins')`)
	require.NoError(t, err)
	assert.Equal(t, "admins", liveValue[string](t, db, `SELECT "group" FROM "order"`))
}

func TestLive_rollbackIsTransactional(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	const version = "20260101000006"
	create := func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewCreateTable("r").Column("id", migrate.Text).Exec(ctx)
	}
	drop := func(ctx context.Context, mdb *migrate.DB) error {
		return mdb.NewDropTable("r").Exec(ctx)
	}
	dropThenFail := func(ctx context.Context, mdb *migrate.DB) error {
		if err := drop(ctx, mdb); err != nil {
			return err
		}
		_, err := mdb.Exec(ctx, "SELECT 1/0")
		return err
	}

	require.NoError(t, migrateLive(ctx, db, migrate.Define(version, "create_r", create, dropThenFail)))

	err := rollbackLive(ctx, db, migrate.Define(version, "create_r", create, dropThenFail))
	require.Error(t, err)
	assert.True(t, tableExists(t, db, "r"), "a failed down must put back the table it dropped")
	assert.Equal(t, 1, recordedVersions(t, db, version), "a failed down must keep the applied record")
	assert.Zero(t, advisoryLocks(t, db))

	require.NoError(t, rollbackLive(ctx, db, migrate.Define(version, "create_r", create, drop)))
	assert.False(t, tableExists(t, db, "r"))
	assert.Zero(t, recordedVersions(t, db, version))
}

// counting stands in for a test double that counts statements. It embeds the
// interface, so it passes every call through, and unwraps so the builders can
// still find the driver behind it.
type counting struct {
	migrate.Executor

	execs int
}

func (c *counting) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	c.execs++
	return c.Executor.Exec(ctx, query, args...)
}

func (c *counting) Unwrap() migrate.Executor { return c.Executor }

func TestLive_wrapperCountsStatements(t *testing.T) {
	ctx := context.Background()
	db := openLive(t)

	_, err := db.Exec(ctx, `CREATE TABLE w (b TEXT, d TEXT)`)
	require.NoError(t, err)

	// A rename splits the alter, so one builder call is several statements.
	alter := func(mdb *migrate.DB) *migrate.AlterTableQuery {
		return mdb.NewAlterTable("w").AddColumn("a", migrate.Text).RenameColumn("b", "c").DropColumn("d")
	}
	op, err := alter(migrate.NewDB(nil)).Operation()
	require.NoError(t, err)
	stmts, err := Render(op)
	require.NoError(t, err)
	require.Greater(t, len(stmts), 1)

	step := migrate.Define("20260101000007", "alter_w", func(ctx context.Context, mdb *migrate.DB) error {
		return alter(mdb).Exec(ctx)
	}, nil)

	wrapper := &counting{Executor: New(db)}
	require.NoError(t, step.Up(ctx, wrapper))

	assert.Equal(t, len(stmts), wrapper.execs, "the renderer must run every statement through the executor it was handed")
	assert.Equal(t, []string{"a", "c"}, liveColumns(t, db, "w"))
}

// liveColumns lists a table's column names in alphabetical order.
func liveColumns(t *testing.T, db *pgdriver.PgDB, table string) []string {
	t.Helper()
	return liveRows(t, db, 1, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 ORDER BY column_name`, table)
}

func TestExecutor_DriverName(t *testing.T) {
	var e migrate.Executor = New(nil)
	name, ok := migrate.DriverNameOf(e)
	require.True(t, ok)
	assert.Equal(t, "pg", name)

	wrapped := &counting{Executor: e}
	name, ok = migrate.DriverNameOf(wrapped)
	require.True(t, ok)
	assert.Equal(t, "pg", name, "a wrapper must reach the driver name through Unwrap")
}

func TestExecutor_registersTheRenderer(t *testing.T) {
	// A builder on a pg executor with no connection has to get as far as the
	// executor, which proves the renderer was found. A missing renderer
	// would stop earlier, with ErrSchemaUnsupported.
	exec := &recordingExecutor{}
	db := migrate.NewDB(exec)
	require.NoError(t, db.NewDropTable("t").Exec(context.Background()))
	assert.Equal(t, []string{`DROP TABLE "t"`}, exec.stmts)
}

// recordingExecutor is a pg executor that records statements instead of
// running them.
type recordingExecutor struct {
	*Executor

	stmts []string
}

func (r *recordingExecutor) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	r.stmts = append(r.stmts, query)
	return nil, nil
}
