package pgmigrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/migrate"
)

// alterOp builds an AlterTable on "t" from a builder chain without an executor.
func alterOp(t *testing.T, build func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery) *migrate.AlterTable {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewAlterTable("t")).Operation()
	require.NoError(t, err)
	return op
}

// indexOp builds a CreateIndex named "i" on "t" from a builder chain.
func indexOp(t *testing.T, build func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery) *migrate.CreateIndex {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewCreateIndex("i").On("t")).Operation()
	require.NoError(t, err)
	return op
}

// renderAll renders op and returns every statement it produced.
func renderAll(t *testing.T, op migrate.Operation) []string {
	t.Helper()
	stmts, err := Render(op)
	require.NoError(t, err)
	return stmts
}

func TestRender_alterTable(t *testing.T) {
	op, err := migrate.NewDB(nil).NewAlterTable("t").
		AddColumn("tags", migrate.ArrayOf(migrate.Text), migrate.NotNull(), migrate.DefaultExpr("'{}'")).IfNotExists().
		DropColumn("old").IfExists().Cascade().
		DropNotNull("x").
		AddCheck("a > 0").Named("t_a_positive").
		DropConstraint("t_old").IfExists().
		RenameColumn("a", "b").
		Operation()
	require.NoError(t, err)

	want := []string{
		`ALTER TABLE "t" ADD COLUMN IF NOT EXISTS "tags" TEXT[] NOT NULL DEFAULT '{}', DROP COLUMN IF EXISTS "old" CASCADE, ALTER COLUMN "x" DROP NOT NULL, ADD CONSTRAINT "t_a_positive" CHECK (a > 0), DROP CONSTRAINT IF EXISTS "t_old"`,
		`ALTER TABLE "t" RENAME COLUMN "a" TO "b"`,
	}
	assert.Equal(t, want, renderAll(t, op))
}

func TestRender_alterColumnActions(t *testing.T) {
	tests := []struct {
		name  string
		build func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery
		want  string
	}{
		{
			"add column",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("a", migrate.Text)
			},
			`ADD COLUMN "a" TEXT`,
		},
		{
			"add column with a reference",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("org_id", migrate.Text, migrate.NotNull(), migrate.References("orgs", "id"), migrate.OnDelete(migrate.Cascade))
			},
			`ADD COLUMN "org_id" TEXT NOT NULL REFERENCES "orgs" ("id") ON DELETE CASCADE`,
		},
		{
			"drop column",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropColumn("a")
			},
			`DROP COLUMN "a"`,
		},
		{
			"drop column if exists",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropColumn("a").IfExists()
			},
			`DROP COLUMN IF EXISTS "a"`,
		},
		{
			"drop column cascade",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropColumn("a").Cascade()
			},
			`DROP COLUMN "a" CASCADE`,
		},
		{
			"set not null",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetNotNull("x")
			},
			`ALTER COLUMN "x" SET NOT NULL`,
		},
		{
			"drop not null",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropNotNull("x")
			},
			`ALTER COLUMN "x" DROP NOT NULL`,
		},
		{
			"set default string",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetDefault("x", "it's")
			},
			`ALTER COLUMN "x" SET DEFAULT 'it''s'`,
		},
		{
			"set default number",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetDefault("x", 5)
			},
			`ALTER COLUMN "x" SET DEFAULT 5`,
		},
		{
			"set default nil",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetDefault("x", nil)
			},
			`ALTER COLUMN "x" SET DEFAULT NULL`,
		},
		{
			"set default expression",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetDefaultExpr("x", "now()")
			},
			`ALTER COLUMN "x" SET DEFAULT now()`,
		},
		{
			"drop default",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropDefault("x")
			},
			`ALTER COLUMN "x" DROP DEFAULT`,
		},
		{
			"set type",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetType("x", migrate.Numeric(10, 2))
			},
			`ALTER COLUMN "x" TYPE NUMERIC(10, 2)`,
		},
		{
			"set type using",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetType("x", migrate.BigInt).Using("x::bigint")
			},
			`ALTER COLUMN "x" TYPE BIGINT USING x::bigint`,
		},
		{
			"add primary key",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddPrimaryKey("a", "b")
			},
			`ADD PRIMARY KEY ("a", "b")`,
		},
		{
			"add named unique",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddUnique("a").Named("t_a_key")
			},
			`ADD CONSTRAINT "t_a_key" UNIQUE ("a")`,
		},
		{
			"add unnamed check",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddCheck("a > 0")
			},
			`ADD CHECK (a > 0)`,
		},
		{
			"add foreign key",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddForeignKey("a", "b").References("p", "a", "b").OnDelete(migrate.Cascade)
			},
			`ADD FOREIGN KEY ("a", "b") REFERENCES "p" ("a", "b") ON DELETE CASCADE`,
		},
		{
			"add named foreign key without an action",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddForeignKey("a").References("p", "id").Named("t_p_fk")
			},
			`ADD CONSTRAINT "t_p_fk" FOREIGN KEY ("a") REFERENCES "p" ("id")`,
		},
		{
			"drop constraint",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropConstraint("c")
			},
			`DROP CONSTRAINT "c"`,
		},
		{
			"drop constraint if exists cascade",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.DropConstraint("c").IfExists().Cascade()
			},
			`DROP CONSTRAINT IF EXISTS "c" CASCADE`,
		},
		{
			"rename column",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameColumn("a", "b")
			},
			`RENAME COLUMN "a" TO "b"`,
		},
		{
			"rename table",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameTo("u")
			},
			`RENAME TO "u"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, `ALTER TABLE "t" `+tt.want, renderOne(t, alterOp(t, tt.build)))
		})
	}
}

func TestRender_alterTable_quotesIdentifiersAndKeepsExpressionsVerbatim(t *testing.T) {
	op, err := migrate.NewDB(nil).NewAlterTable(`we"ird`).
		DropColumn(`co"l`).
		SetType("order", migrate.Text).Using(`"order"::text`).
		SetDefaultExpr("group", "now() - '1 day'::interval").
		AddCheck(`"a" > 0 AND b <> 'x'`).Named(`n"m`).
		Operation()
	require.NoError(t, err)

	want := `ALTER TABLE "we""ird" DROP COLUMN "co""l", ALTER COLUMN "order" TYPE TEXT USING "order"::text, ` +
		`ALTER COLUMN "group" SET DEFAULT now() - '1 day'::interval, ADD CONSTRAINT "n""m" CHECK ("a" > 0 AND b <> 'x')`
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_renameSplits(t *testing.T) {
	op := alterOp(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
		return q.
			AddColumn("a", migrate.Text).
			RenameColumn("b", "c").
			DropColumn("d")
	})

	want := []string{
		`ALTER TABLE "t" ADD COLUMN "a" TEXT`,
		`ALTER TABLE "t" RENAME COLUMN "b" TO "c"`,
		`ALTER TABLE "t" DROP COLUMN "d"`,
	}
	assert.Equal(t, want, renderAll(t, op))
}

func TestRender_renameOrdering(t *testing.T) {
	tests := []struct {
		name  string
		build func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery
		want  []string
	}{
		{
			"rename alone",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameColumn("a", "b")
			},
			[]string{`ALTER TABLE "t" RENAME COLUMN "a" TO "b"`},
		},
		{
			"rename first",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameColumn("a", "b").SetNotNull("b").DropDefault("b")
			},
			[]string{
				`ALTER TABLE "t" RENAME COLUMN "a" TO "b"`,
				`ALTER TABLE "t" ALTER COLUMN "b" SET NOT NULL, ALTER COLUMN "b" DROP DEFAULT`,
			},
		},
		{
			"back to back renames",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameColumn("a", "b").RenameColumn("c", "d")
			},
			[]string{
				`ALTER TABLE "t" RENAME COLUMN "a" TO "b"`,
				`ALTER TABLE "t" RENAME COLUMN "c" TO "d"`,
			},
		},
		{
			"runs on both sides of two renames",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.
					SetNotNull("a").DropNotNull("b").
					RenameColumn("a", "c").
					DropDefault("c").
					RenameColumn("b", "d").
					DropColumn("e").DropColumn("f")
			},
			[]string{
				`ALTER TABLE "t" ALTER COLUMN "a" SET NOT NULL, ALTER COLUMN "b" DROP NOT NULL`,
				`ALTER TABLE "t" RENAME COLUMN "a" TO "c"`,
				`ALTER TABLE "t" ALTER COLUMN "c" DROP DEFAULT`,
				`ALTER TABLE "t" RENAME COLUMN "b" TO "d"`,
				`ALTER TABLE "t" DROP COLUMN "e", DROP COLUMN "f"`,
			},
		},
		{
			"rename table splits the run before it",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.SetNotNull("a").RenameTo("u")
			},
			[]string{
				`ALTER TABLE "t" ALTER COLUMN "a" SET NOT NULL`,
				`ALTER TABLE "t" RENAME TO "u"`,
			},
		},
		{
			// The table has a new name from the rename on, so a later action
			// aimed at "t" would fail with a missing relation.
			"actions after a table rename use the new name",
			func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.RenameTo("u").AddColumn("a", migrate.Text).RenameColumn("a", "b").RenameTo("v").DropColumn("b")
			},
			[]string{
				`ALTER TABLE "t" RENAME TO "u"`,
				`ALTER TABLE "u" ADD COLUMN "a" TEXT`,
				`ALTER TABLE "u" RENAME COLUMN "a" TO "b"`,
				`ALTER TABLE "u" RENAME TO "v"`,
				`ALTER TABLE "v" DROP COLUMN "b"`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, renderAll(t, alterOp(t, tt.build)))
		})
	}
}

func TestRender_alterTable_rejectsMalformedOperations(t *testing.T) {
	col := func(c migrate.Column) migrate.AlterAction {
		return migrate.AlterAction{Kind: migrate.AlterAddColumn, Column: c}
	}
	tests := []struct {
		name    string
		actions []migrate.AlterAction
	}{
		{"no actions", nil},
		{"unset kind", []migrate.AlterAction{{Name: "a"}}},
		{"unknown kind", []migrate.AlterAction{{Kind: 99, Name: "a"}}},
		{"drop column without a name", []migrate.AlterAction{{Kind: migrate.AlterDropColumn}}},
		{"set not null without a name", []migrate.AlterAction{{Kind: migrate.AlterSetNotNull}}},
		{"drop constraint without a name", []migrate.AlterAction{{Kind: migrate.AlterDropConstraint}}},
		{"rename column without a new name", []migrate.AlterAction{{Kind: migrate.AlterRenameColumn, Name: "a"}}},
		{"rename column without an old name", []migrate.AlterAction{{Kind: migrate.AlterRenameColumn, NewName: "b"}}},
		{"rename table without a name", []migrate.AlterAction{{Kind: migrate.AlterRenameTo}}},
		{"set default without a default", []migrate.AlterAction{{Kind: migrate.AlterSetDefault, Name: "a"}}},
		{"set default with a literal that has no SQL form", []migrate.AlterAction{{
			Kind: migrate.AlterSetDefault, Name: "a", Default: &migrate.DefaultValue{Value: []string{"x"}},
		}}},
		{"set type with no type", []migrate.AlterAction{{Kind: migrate.AlterSetType, Name: "a"}}},
		{"set type with an unnamed raw type", []migrate.AlterAction{{
			Kind: migrate.AlterSetType, Name: "a", Type: migrate.ColumnType{Kind: migrate.KindRaw},
		}}},
		{"add column with no type", []migrate.AlterAction{col(migrate.Column{Name: "a"})}},
		{"add column both identity and generated", []migrate.AlterAction{col(migrate.Column{
			Name: "a", Type: migrate.BigInt, Identity: true, Generated: "1",
		})}},
		{"add constraint of an unknown kind", []migrate.AlterAction{{
			Kind: migrate.AlterAddConstraint, Constraint: migrate.Constraint{Kind: 99},
		}}},
		{"add foreign key without references", []migrate.AlterAction{{
			Kind:       migrate.AlterAddConstraint,
			Constraint: migrate.Constraint{Kind: migrate.ForeignKeyConstraint, Columns: []string{"a"}},
		}}},
		{"add foreign key with a made-up action", []migrate.AlterAction{{
			Kind: migrate.AlterAddConstraint,
			Constraint: migrate.Constraint{
				Kind:       migrate.ForeignKeyConstraint,
				Columns:    []string{"a"},
				References: &migrate.Reference{Table: "o", Columns: []string{"id"}, OnDelete: "CASCADE; DROP TABLE x"},
			},
		}}},
		{"a bad action after a good one", []migrate.AlterAction{
			{Kind: migrate.AlterDropColumn, Name: "a"},
			{Kind: migrate.AlterSetNotNull},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := Render(&migrate.AlterTable{Table: "t", Actions: tt.actions})
			require.Error(t, err)
			assert.Nil(t, stmts)
			assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
		})
	}
}

func TestRender_alterTable_errorNamesTheAction(t *testing.T) {
	_, err := Render(&migrate.AlterTable{Table: "t", Actions: []migrate.AlterAction{
		{Kind: migrate.AlterDropColumn, Name: "a"},
		{Kind: migrate.AlterSetNotNull},
	}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `alter table "t"`)
	assert.Contains(t, err.Error(), "action 2")
}

func TestRender_createIndex(t *testing.T) {
	op, err := migrate.NewDB(nil).NewCreateIndex("i").
		On("t").
		Columns("org_id").
		ColumnDesc("created_at").
		ColumnExpr("lower(name)").
		Unique().
		IfNotExists().
		Where("deleted_at IS NULL").
		Operation()
	require.NoError(t, err)

	want := `CREATE UNIQUE INDEX IF NOT EXISTS "i" ON "t" ("org_id", "created_at" DESC, (lower(name))) WHERE deleted_at IS NULL`
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_createIndexClauses(t *testing.T) {
	tests := []struct {
		name  string
		build func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery
		want  string
	}{
		{
			"plain",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery { return q.Columns("a") },
			`CREATE INDEX "i" ON "t" ("a")`,
		},
		{
			"unique",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery { return q.Columns("a").Unique() },
			`CREATE UNIQUE INDEX "i" ON "t" ("a")`,
		},
		{
			"if not exists",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery { return q.Columns("a").IfNotExists() },
			`CREATE INDEX IF NOT EXISTS "i" ON "t" ("a")`,
		},
		{
			"using",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery { return q.Columns("tags").Using("gin") },
			`CREATE INDEX "i" ON "t" USING gin ("tags")`,
		},
		{
			"include",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery { return q.Columns("a").Include("b") },
			`CREATE INDEX "i" ON "t" ("a") INCLUDE ("b")`,
		},
		{
			"include several",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.Columns("a").Include("b", "c").Include("d")
			},
			`CREATE INDEX "i" ON "t" ("a") INCLUDE ("b", "c", "d")`,
		},
		{
			"where",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.Columns("a").Where("a > 0 AND b <> 'x'")
			},
			`CREATE INDEX "i" ON "t" ("a") WHERE a > 0 AND b <> 'x'`,
		},
		{
			"concurrently comes before if not exists",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.Columns("a").Concurrently().IfNotExists()
			},
			`CREATE INDEX CONCURRENTLY IF NOT EXISTS "i" ON "t" ("a")`,
		},
		{
			"concurrently and unique",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.Columns("a").Unique().Concurrently()
			},
			`CREATE UNIQUE INDEX CONCURRENTLY "i" ON "t" ("a")`,
		},
		{
			"descending column",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.ColumnDesc("a")
			},
			`CREATE INDEX "i" ON "t" ("a" DESC)`,
		},
		{
			"expression key",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.ColumnExpr("lower(a) || b")
			},
			`CREATE INDEX "i" ON "t" ((lower(a) || b))`,
		},
		{
			"every clause in order",
			func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
				return q.Columns("a").ColumnDesc("b").Unique().IfNotExists().Concurrently().
					Using("btree").Include("c").Where("c IS NOT NULL")
			},
			`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "i" ON "t" USING btree ("a", "b" DESC) INCLUDE ("c") WHERE c IS NOT NULL`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, renderOne(t, indexOp(t, tt.build)))
		})
	}
}

func TestRender_createIndex_quotesIdentifiers(t *testing.T) {
	op, err := migrate.NewDB(nil).NewCreateIndex(`i"x`).
		On("order").
		Columns("group", `co"l`).
		Include("select").
		Operation()
	require.NoError(t, err)

	want := `CREATE INDEX "i""x" ON "order" ("group", "co""l") INCLUDE ("select")`
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_createIndex_hasNoPostgresFilterForm(t *testing.T) {
	op := &migrate.CreateIndex{
		Name:    "i",
		Table:   "t",
		Columns: []migrate.IndexColumn{{Name: "a"}},
		Filter:  map[string]any{"deleted_at": nil},
	}

	stmts, err := Render(op)
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	assert.Equal(t, migrate.Unsupported("pg", "index filter documents").Error(), err.Error())
}

func TestRender_createIndex_rejectsMalformedOperations(t *testing.T) {
	cols := []migrate.IndexColumn{{Name: "a"}}
	tests := []struct {
		name string
		op   *migrate.CreateIndex
	}{
		{"driver options", &migrate.CreateIndex{Name: "i", Table: "t", Columns: cols, Options: []any{"x"}}},
		{"no name", &migrate.CreateIndex{Table: "t", Columns: cols}},
		{"no table", &migrate.CreateIndex{Name: "i", Columns: cols}},
		{"no columns", &migrate.CreateIndex{Name: "i", Table: "t"}},
		{"column with neither name nor expression", &migrate.CreateIndex{
			Name: "i", Table: "t", Columns: []migrate.IndexColumn{{Desc: true}},
		}},
		{"column with both name and expression", &migrate.CreateIndex{
			Name: "i", Table: "t", Columns: []migrate.IndexColumn{{Name: "a", Expr: "lower(a)"}},
		}},
		{"empty include column", &migrate.CreateIndex{Name: "i", Table: "t", Columns: cols, Include: []string{"b", ""}}},
		{"access method with a space", &migrate.CreateIndex{Name: "i", Table: "t", Columns: cols, Using: "gin (a); DROP TABLE x; --"}},
		{"access method with a quote", &migrate.CreateIndex{Name: "i", Table: "t", Columns: cols, Using: `"gin"`}},
		{"access method starting with a digit", &migrate.CreateIndex{Name: "i", Table: "t", Columns: cols, Using: "2gin"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := Render(tt.op)
			require.Error(t, err)
			assert.Nil(t, stmts)
			assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
		})
	}
}

func TestRender_createIndex_acceptsExtensionAccessMethods(t *testing.T) {
	op := indexOp(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
		return q.Columns("embedding").Using("ivfflat")
	})
	assert.Equal(t, `CREATE INDEX "i" ON "t" USING ivfflat ("embedding")`, renderOne(t, op))
}

func TestRender_dropIndex(t *testing.T) {
	tests := []struct {
		name string
		op   *migrate.DropIndex
		want string
	}{
		{"plain", &migrate.DropIndex{Name: "i"}, `DROP INDEX "i"`},
		{"if exists", &migrate.DropIndex{Name: "i", IfExists: true}, `DROP INDEX IF EXISTS "i"`},
		{"concurrently", &migrate.DropIndex{Name: "i", Concurrently: true}, `DROP INDEX CONCURRENTLY "i"`},
		{"cascade", &migrate.DropIndex{Name: "i", Cascade: true}, `DROP INDEX "i" CASCADE`},
		{
			"concurrently if exists",
			&migrate.DropIndex{Name: "i", IfExists: true, Concurrently: true},
			`DROP INDEX CONCURRENTLY IF EXISTS "i"`,
		},
		{
			"if exists cascade",
			&migrate.DropIndex{Name: "i", IfExists: true, Cascade: true},
			`DROP INDEX IF EXISTS "i" CASCADE`,
		},
		{"table is not needed", &migrate.DropIndex{Name: "i", Table: "t"}, `DROP INDEX "i"`},
		{"reserved word", &migrate.DropIndex{Name: "order"}, `DROP INDEX "order"`},
		{"embedded quote", &migrate.DropIndex{Name: `i"x`}, `DROP INDEX "i""x"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, renderOne(t, tt.op))
		})
	}
}

func TestRender_dropIndex_fromBuilder(t *testing.T) {
	op, err := migrate.NewDB(nil).NewDropIndex("i").On("t").IfExists().Concurrently().Operation()
	require.NoError(t, err)
	assert.Equal(t, `DROP INDEX CONCURRENTLY IF EXISTS "i"`, renderOne(t, op))
}

// Postgres refuses CASCADE together with CONCURRENTLY, so the pair is caught
// here instead of failing partway through a migration.
func TestRender_dropIndex_rejectsConcurrentlyWithCascade(t *testing.T) {
	built, err := migrate.NewDB(nil).NewDropIndex("i").IfExists().Concurrently().Cascade().Operation()
	require.NoError(t, err)

	tests := []struct {
		name string
		op   *migrate.DropIndex
	}{
		{"hand-built", &migrate.DropIndex{Name: "i", Concurrently: true, Cascade: true}},
		{"with if exists", &migrate.DropIndex{Name: "i", IfExists: true, Concurrently: true, Cascade: true}},
		{"from the builder", built},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := Render(tt.op)
			require.Error(t, err)
			assert.Nil(t, stmts)
			assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
			assert.Contains(t, err.Error(), "CASCADE with CONCURRENTLY")
		})
	}
}

func TestRenderer_Apply_concurrentlyWithCascadeDoesNotExecute(t *testing.T) {
	exec := &recordingExec{}
	err := Renderer{}.Apply(context.Background(), exec, &migrate.DropIndex{Name: "i", Concurrently: true, Cascade: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
	assert.Empty(t, exec.stmts)
}

func TestRender_dropIndex_needsAName(t *testing.T) {
	stmts, err := Render(&migrate.DropIndex{})
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
	assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
}

func TestRender_unknownOperation(t *testing.T) {
	stmts, err := Render(nil)
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
}

func TestRenderer_Apply_runsEveryStatementInOrder(t *testing.T) {
	exec := &recordingExec{}
	op := alterOp(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
		return q.AddColumn("a", migrate.Text).RenameColumn("b", "c").DropColumn("d")
	})

	require.NoError(t, Renderer{}.Apply(context.Background(), exec, op))
	assert.Equal(t, []string{
		`ALTER TABLE "t" ADD COLUMN "a" TEXT`,
		`ALTER TABLE "t" RENAME COLUMN "b" TO "c"`,
		`ALTER TABLE "t" DROP COLUMN "d"`,
	}, exec.stmts)
}

func TestRenderer_Apply_stopsAtTheFailingStatement(t *testing.T) {
	boom := errors.New("column \"b\" does not exist")
	exec := &recordingExec{failAt: 2, err: boom}
	op := alterOp(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
		return q.AddColumn("a", migrate.Text).RenameColumn("b", "c").DropColumn("d")
	})

	err := Renderer{}.Apply(context.Background(), exec, op)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)

	// The first statement ran, the second was tried and failed, and the third
	// never started.
	assert.Equal(t, []string{
		`ALTER TABLE "t" ADD COLUMN "a" TEXT`,
		`ALTER TABLE "t" RENAME COLUMN "b" TO "c"`,
	}, exec.stmts)
	assert.Equal(t, `pgmigrate: alter table t (statement 2 of 3): column "b" does not exist`, err.Error())
}

func TestRenderer_Apply_wrapsExecErrorsForAlterAndIndexes(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		op   migrate.Operation
		want string
	}{
		{
			"alter table",
			&migrate.AlterTable{Table: "t", Actions: []migrate.AlterAction{{Kind: migrate.AlterDropColumn, Name: "a"}}},
			"pgmigrate: alter table t: boom",
		},
		{
			"create index",
			&migrate.CreateIndex{Name: "i", Table: "t", Columns: []migrate.IndexColumn{{Name: "a"}}},
			"pgmigrate: create index i: boom",
		},
		{
			"drop index",
			&migrate.DropIndex{Name: "i"},
			"pgmigrate: drop index i: boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &recordingExec{failAt: 1, err: boom}
			err := Renderer{}.Apply(context.Background(), exec, tt.op)
			require.Error(t, err)
			assert.ErrorIs(t, err, boom)
			assert.Equal(t, tt.want, err.Error())
		})
	}
}

func TestRenderer_Apply_runsIndexStatements(t *testing.T) {
	exec := &recordingExec{}
	ctx := context.Background()

	create := indexOp(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
		return q.Columns("a").Unique()
	})
	require.NoError(t, Renderer{}.Apply(ctx, exec, create))
	require.NoError(t, Renderer{}.Apply(ctx, exec, &migrate.DropIndex{Name: "i", IfExists: true}))

	assert.Equal(t, []string{
		`CREATE UNIQUE INDEX "i" ON "t" ("a")`,
		`DROP INDEX IF EXISTS "i"`,
	}, exec.stmts)
}

func TestRenderer_Apply_unsupportedIndexFilterDoesNotExecute(t *testing.T) {
	exec := &recordingExec{}
	op := &migrate.CreateIndex{
		Name:    "i",
		Table:   "t",
		Columns: []migrate.IndexColumn{{Name: "a"}},
		Filter:  map[string]any{"x": 1},
	}

	err := Renderer{}.Apply(context.Background(), exec, op)
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	assert.Empty(t, exec.stmts)
}

func TestRenderer_Apply_rejectedAlterDoesNotExecute(t *testing.T) {
	exec := &recordingExec{}
	// The second action is malformed, so the first must not run on its own.
	op := &migrate.AlterTable{Table: "t", Actions: []migrate.AlterAction{
		{Kind: migrate.AlterDropColumn, Name: "a"},
		{Kind: migrate.AlterSetNotNull},
	}}

	err := Renderer{}.Apply(context.Background(), exec, op)
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
	assert.Empty(t, exec.stmts)
}
