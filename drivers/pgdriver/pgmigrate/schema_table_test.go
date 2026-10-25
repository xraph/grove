package pgmigrate

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

// lines joins statement lines the way the renderer lays out a table: one
// column or constraint per line, indented with a tab.
func lines(parts ...string) string {
	return strings.Join(parts, "\n")
}

// renderOne renders op and insists on exactly one statement.
func renderOne(t *testing.T, op migrate.Operation) string {
	t.Helper()
	stmts, err := Render(op)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return stmts[0]
}

// createOp builds a CreateTable from a builder chain without touching an
// executor, so no database or driver is involved.
func createOp(t *testing.T, build func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery) *migrate.CreateTable {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewCreateTable("t")).Operation()
	require.NoError(t, err)
	return op
}

func TestRender_createTable(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.
			IfNotExists().
			Column("id", migrate.Text, migrate.PrimaryKey()).
			Column("status", migrate.Text, migrate.NotNull(), migrate.Default("pending"), migrate.Check("status <> ''")).
			Column("project_id", migrate.Text, migrate.References("projects", "id"), migrate.OnDelete(migrate.Cascade)).
			Column("evidence", migrate.JSON, migrate.NotNull(), migrate.DefaultExpr("'{}'")).
			Column("seq", migrate.BigInt, migrate.Identity()).
			Unique("org_id", "status").Named("t_org_status").
			ForeignKey("org_id").References("orgs", "id").OnDelete(migrate.Restrict)
	})

	want := lines(
		`CREATE TABLE IF NOT EXISTS "t" (`,
		"\t"+`"id" TEXT PRIMARY KEY,`,
		"\t"+`"status" TEXT NOT NULL DEFAULT 'pending' CHECK (status <> ''),`,
		"\t"+`"project_id" TEXT REFERENCES "projects" ("id") ON DELETE CASCADE,`,
		"\t"+`"evidence" JSONB NOT NULL DEFAULT '{}',`,
		"\t"+`"seq" BIGINT GENERATED ALWAYS AS IDENTITY,`,
		"\t"+`CONSTRAINT "t_org_status" UNIQUE ("org_id", "status"),`,
		"\t"+`FOREIGN KEY ("org_id") REFERENCES "orgs" ("id") ON DELETE RESTRICT`,
		`)`,
	)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_createTable_withoutIfNotExists(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("id", migrate.Text)
	})
	assert.Equal(t, lines(`CREATE TABLE "t" (`, "\t"+`"id" TEXT`, `)`), renderOne(t, op))
}

func TestRender_columnClauseOrder(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("a", migrate.Text,
			migrate.References("o", "id"), migrate.OnDelete(migrate.SetNull),
			migrate.Check("a <> ''"), migrate.Unique(), migrate.PrimaryKey(),
			migrate.Default("x"), migrate.NotNull())
	})
	// Option order in the chain must not matter: the clause order is fixed.
	want := lines(
		`CREATE TABLE "t" (`,
		"\t"+`"a" TEXT NOT NULL DEFAULT 'x' PRIMARY KEY UNIQUE CHECK (a <> '') REFERENCES "o" ("id") ON DELETE SET NULL`,
		`)`,
	)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_tableConstraints(t *testing.T) {
	tests := []struct {
		name  string
		build func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery
		want  string
	}{
		{
			name: "composite primary key",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).Column("b", migrate.Text).PrimaryKey("a", "b")
			},
			want: `PRIMARY KEY ("a", "b")`,
		},
		{
			name: "named primary key",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).PrimaryKey("a").Named("t_pk")
			},
			want: `CONSTRAINT "t_pk" PRIMARY KEY ("a")`,
		},
		{
			name: "unnamed unique",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).Unique("a")
			},
			want: `UNIQUE ("a")`,
		},
		{
			name: "unnamed check",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Integer).Check("a > 0")
			},
			want: `CHECK (a > 0)`,
		},
		{
			name: "named check",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Integer).Check("a > 0").Named("t_positive")
			},
			want: `CONSTRAINT "t_positive" CHECK (a > 0)`,
		},
		{
			name: "foreign key without an action",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).ForeignKey("a").References("o", "id")
			},
			want: `FOREIGN KEY ("a") REFERENCES "o" ("id")`,
		},
		{
			name: "composite named foreign key",
			build: func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).Column("b", migrate.Text).
					ForeignKey("a", "b").References("o", "x", "y").OnDelete(migrate.NoAction).Named("t_o_fk")
			},
			want: `CONSTRAINT "t_o_fk" FOREIGN KEY ("a", "b") REFERENCES "o" ("x", "y") ON DELETE NO ACTION`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := renderOne(t, createOp(t, tt.build))
			lastLine := got[strings.LastIndex(got, "\n\t")+2 : strings.LastIndex(got, "\n)")]
			assert.Equal(t, tt.want, lastLine)
		})
	}
}

func TestRender_types(t *testing.T) {
	tests := []struct {
		name string
		typ  migrate.ColumnType
		want string
	}{
		{"text", migrate.Text, "TEXT"},
		{"smallint", migrate.SmallInt, "SMALLINT"},
		{"integer", migrate.Integer, "INTEGER"},
		{"bigint", migrate.BigInt, "BIGINT"},
		{"bigserial", migrate.BigSerial, "BIGSERIAL"},
		{"boolean", migrate.Boolean, "BOOLEAN"},
		{"timestamp", migrate.Timestamp, "TIMESTAMPTZ"},
		{"date", migrate.Date, "DATE"},
		{"json", migrate.JSON, "JSONB"},
		{"uuid", migrate.UUID, "UUID"},
		{"bytes", migrate.Bytes, "BYTEA"},
		{"float", migrate.Float, "DOUBLE PRECISION"},
		{"numeric with limits", migrate.Numeric(10, 2), "NUMERIC(10, 2)"},
		{"numeric precision only", migrate.Numeric(10, 0), "NUMERIC(10, 0)"},
		{"numeric unbounded", migrate.Numeric(0, 0), "NUMERIC"},
		{"array of text", migrate.ArrayOf(migrate.Text), "TEXT[]"},
		{"array of numeric", migrate.ArrayOf(migrate.Numeric(10, 2)), "NUMERIC(10, 2)[]"},
		{"array of array", migrate.ArrayOf(migrate.ArrayOf(migrate.Integer)), "INTEGER[][]"},
		{"raw", migrate.Type("INET"), "INET"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("c", tt.typ)
			})
			assert.Equal(t, lines(`CREATE TABLE "t" (`, "\t"+`"c" `+tt.want, `)`), renderOne(t, op))
		})
	}
}

func TestRender_rejectsBadTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  migrate.ColumnType
	}{
		{"zero value", migrate.ColumnType{}},
		{"unknown kind", migrate.ColumnType{Kind: 99}},
		{"array without element", migrate.ColumnType{Kind: migrate.KindArray}},
		{"array of unset element", migrate.ArrayOf(migrate.ColumnType{})},
		{"raw without name", migrate.ColumnType{Kind: migrate.KindRaw}},
		{"negative precision", migrate.Numeric(-1, 0)},
		{"scale without precision", migrate.Numeric(0, 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Hand-built: the builders refuse these, but Render is public.
			op := &migrate.CreateTable{
				Table:   "t",
				Columns: []migrate.Column{{Name: "c", Type: tt.typ}},
			}
			_, err := Render(op)
			require.Error(t, err)
			assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
		})
	}
}

type status string

func TestRender_defaultQuoting(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "pending", `'pending'`},
		{"string with apostrophe", "it's", `'it''s'`},
		{"empty string", "", `''`},
		{"named string type", status("open"), `'open'`},
		{"true", true, "TRUE"},
		{"false", false, "FALSE"},
		{"nil", nil, "NULL"},
		{"int", 42, "42"},
		{"negative int64", int64(-7), "-7"},
		{"int8", int8(3), "3"},
		{"uint16", uint16(9), "9"},
		{"float64", 1.5, "1.5"},
		{"float32 keeps its own precision", float32(0.1), "0.1"},
		{"negative float", -0.25, "-0.25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("c", migrate.Text, migrate.Default(tt.value))
			})
			want := lines(`CREATE TABLE "t" (`, "\t"+`"c" TEXT DEFAULT `+tt.want, `)`)
			assert.Equal(t, want, renderOne(t, op))
		})
	}
}

func TestRender_defaultExpressionIsVerbatim(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("c", migrate.Timestamp, migrate.NotNull(), migrate.DefaultExpr("now()"))
	})
	want := lines(`CREATE TABLE "t" (`, "\t"+`"c" TIMESTAMPTZ NOT NULL DEFAULT now()`, `)`)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_defaultRejectsUnsupportedValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"struct", struct{}{}},
		{"slice", []string{"a"}},
		{"map", map[string]any{}},
		{"pointer", new(int)},
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("c", migrate.Text, migrate.Default(tt.value))
			})
			_, err := Render(op)
			require.Error(t, err)
			assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.Contains(t, err.Error(), `"c"`)
		})
	}
}

func TestRender_reservedWords(t *testing.T) {
	op, err := migrate.NewDB(nil).NewCreateTable("order").
		Column("group", migrate.Text).
		Operation()
	require.NoError(t, err)
	assert.Equal(t, lines(`CREATE TABLE "order" (`, "\t"+`"group" TEXT`, `)`), renderOne(t, op))
}

func TestRender_quotesEmbeddedQuotes(t *testing.T) {
	op, err := migrate.NewDB(nil).NewCreateTable(`we"ird`).
		Column(`co"l`, migrate.Text, migrate.References(`o"t`, `i"d`)).
		Unique(`co"l`).Named(`n"m`).
		Operation()
	require.NoError(t, err)
	want := lines(
		`CREATE TABLE "we""ird" (`,
		"\t"+`"co""l" TEXT REFERENCES "o""t" ("i""d"),`,
		"\t"+`CONSTRAINT "n""m" UNIQUE ("co""l")`,
		`)`,
	)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_generatedColumn(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.
			Column("name", migrate.Text).
			Column("name_lower", migrate.Text, migrate.GeneratedAs("lower(name)"), migrate.NotNull())
	})
	want := lines(
		`CREATE TABLE "t" (`,
		"\t"+`"name" TEXT,`,
		"\t"+`"name_lower" TEXT GENERATED ALWAYS AS (lower(name)) STORED NOT NULL`,
		`)`,
	)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_rejectsIdentityWithGenerated(t *testing.T) {
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("c", migrate.BigInt, migrate.Identity(), migrate.GeneratedAs("1"))
	})
	_, err := Render(op)
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
}

func TestRender_dropTable(t *testing.T) {
	tests := []struct {
		name string
		op   *migrate.DropTable
		want string
	}{
		{"plain", &migrate.DropTable{Table: "t"}, `DROP TABLE "t"`},
		{"if exists", &migrate.DropTable{Table: "t", IfExists: true}, `DROP TABLE IF EXISTS "t"`},
		{"cascade", &migrate.DropTable{Table: "t", Cascade: true}, `DROP TABLE "t" CASCADE`},
		{"if exists and cascade", &migrate.DropTable{Table: "t", IfExists: true, Cascade: true}, `DROP TABLE IF EXISTS "t" CASCADE`},
		{"reserved word", &migrate.DropTable{Table: "order"}, `DROP TABLE "order"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, renderOne(t, tt.op))
		})
	}
}

func TestRender_dropTable_fromBuilder(t *testing.T) {
	op, err := migrate.NewDB(nil).NewDropTable("t").IfExists().Cascade().Operation()
	require.NoError(t, err)
	assert.Equal(t, `DROP TABLE IF EXISTS "t" CASCADE`, renderOne(t, op))
}

type widget struct {
	grove.BaseModel `grove:"table:widgets"`

	ID   int64  `grove:"id,pk,autoincrement"`
	Name string `grove:"name,notnull"`
}

func TestRender_model(t *testing.T) {
	want, _, err := pgdriver.New().NewCreateTable((*widget)(nil)).IfNotExists().Build()
	require.NoError(t, err)
	require.Contains(t, want, `"widgets"`, "the model's table tag should reach the SQL")

	op, err := migrate.NewDB(nil).NewCreateTable("widgets").IfNotExists().Model((*widget)(nil)).Operation()
	require.NoError(t, err)
	assert.Equal(t, want, renderOne(t, op))
}

func TestRender_model_withoutIfNotExists(t *testing.T) {
	want, _, err := pgdriver.New().NewCreateTable((*widget)(nil)).Build()
	require.NoError(t, err)

	op, err := migrate.NewDB(nil).NewCreateTable("widgets").Model((*widget)(nil)).Operation()
	require.NoError(t, err)
	got := renderOne(t, op)
	assert.Equal(t, want, got)
	assert.NotContains(t, got, "IF NOT EXISTS")
}

func TestRender_model_notAStruct(t *testing.T) {
	_, err := Render(&migrate.CreateTable{Table: "t", Model: 42})
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "pgmigrate: "), err.Error())
}

func TestRender_rejectsModelWithColumns(t *testing.T) {
	_, err := Render(&migrate.CreateTable{
		Table:   "widgets",
		Model:   (*widget)(nil),
		Columns: []migrate.Column{{Name: "id", Type: migrate.Text}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
}

func TestRender_rejectsOptions(t *testing.T) {
	op, err := migrate.NewDB(nil).NewCreateTable("t").Column("id", migrate.Text).With("x").Operation()
	require.NoError(t, err)

	_, err = Render(op)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgmigrate: postgres takes no driver options")
}

func TestRender_rejectsMalformedOperations(t *testing.T) {
	tests := []struct {
		name string
		op   migrate.Operation
	}{
		{"no columns and no model", &migrate.CreateTable{Table: "t"}},
		{"foreign key without references", &migrate.CreateTable{
			Table:       "t",
			Columns:     []migrate.Column{{Name: "a", Type: migrate.Text}},
			Constraints: []migrate.Constraint{{Kind: migrate.ForeignKeyConstraint, Columns: []string{"a"}}},
		}},
		{"foreign key column count mismatch", &migrate.CreateTable{
			Table:   "t",
			Columns: []migrate.Column{{Name: "a", Type: migrate.Text}},
			Constraints: []migrate.Constraint{{
				Kind:       migrate.ForeignKeyConstraint,
				Columns:    []string{"a"},
				References: &migrate.Reference{Table: "o", Columns: []string{"x", "y"}},
			}},
		}},
		{"unique without columns", &migrate.CreateTable{
			Table:       "t",
			Columns:     []migrate.Column{{Name: "a", Type: migrate.Text}},
			Constraints: []migrate.Constraint{{Kind: migrate.UniqueConstraint}},
		}},
		{"check without expression", &migrate.CreateTable{
			Table:       "t",
			Columns:     []migrate.Column{{Name: "a", Type: migrate.Text}},
			Constraints: []migrate.Constraint{{Kind: migrate.CheckConstraint}},
		}},
		{"unknown constraint kind", &migrate.CreateTable{
			Table:       "t",
			Columns:     []migrate.Column{{Name: "a", Type: migrate.Text}},
			Constraints: []migrate.Constraint{{Kind: 99, Columns: []string{"a"}}},
		}},
		{"column reference without a table", &migrate.CreateTable{
			Table: "t",
			Columns: []migrate.Column{{
				Name: "a", Type: migrate.Text, References: &migrate.Reference{Columns: []string{"id"}},
			}},
		}},
		{"column reference without columns", &migrate.CreateTable{
			Table: "t",
			Columns: []migrate.Column{{
				Name: "a", Type: migrate.Text, References: &migrate.Reference{Table: "o"},
			}},
		}},
		{"made-up referential action", &migrate.CreateTable{
			Table: "t",
			Columns: []migrate.Column{{
				Name: "a", Type: migrate.Text,
				References: &migrate.Reference{Table: "o", Columns: []string{"id"}, OnDelete: "CASCADE; DROP TABLE x"},
			}},
		}},
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

func TestRender_otherOperationsAreNotYetSupported(t *testing.T) {
	tests := []struct {
		name string
		op   migrate.Operation
	}{
		{"alter table", &migrate.AlterTable{Table: "t"}},
		{"create index", &migrate.CreateIndex{Name: "i", Table: "t"}},
		{"drop index", &migrate.DropIndex{Name: "i"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Render(tt.op)
			require.Error(t, err)
			assert.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
			assert.Contains(t, err.Error(), "pg")
		})
	}
}

// recordingExec is a migrate.Executor that only knows Exec. The embedded
// interface is nil, so a call to any other method is a bug in the code under
// test.
type recordingExec struct {
	migrate.Executor

	stmts  []string
	failAt int // 1-based statement to fail, 0 for never
	err    error
}

func (r *recordingExec) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	r.stmts = append(r.stmts, query)
	if r.failAt != 0 && len(r.stmts) == r.failAt {
		return nil, r.err
	}
	return nil, nil
}

func TestRenderer_Apply_runsRenderedStatements(t *testing.T) {
	exec := &recordingExec{}
	op := &migrate.DropTable{Table: "t", IfExists: true}

	require.NoError(t, Renderer{}.Apply(context.Background(), exec, op))
	assert.Equal(t, []string{`DROP TABLE IF EXISTS "t"`}, exec.stmts)
}

func TestRenderer_Apply_createTable(t *testing.T) {
	exec := &recordingExec{}
	op := createOp(t, func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("id", migrate.Text, migrate.PrimaryKey())
	})

	require.NoError(t, Renderer{}.Apply(context.Background(), exec, op))
	assert.Equal(t, []string{lines(`CREATE TABLE "t" (`, "\t"+`"id" TEXT PRIMARY KEY`, `)`)}, exec.stmts)
}

func TestRenderer_Apply_wrapsExecErrors(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		op   migrate.Operation
		want string
	}{
		{
			"drop table",
			&migrate.DropTable{Table: "t"},
			"pgmigrate: drop table t: boom",
		},
		{
			"create table",
			&migrate.CreateTable{Table: "t", Columns: []migrate.Column{{Name: "id", Type: migrate.Text}}},
			"pgmigrate: create table t: boom",
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

func TestRenderer_Apply_returnsRenderErrorsWithoutExecuting(t *testing.T) {
	exec := &recordingExec{}
	err := Renderer{}.Apply(context.Background(), exec, &migrate.CreateTable{Table: "t"})
	require.Error(t, err)
	assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
	assert.Empty(t, exec.stmts)
}
