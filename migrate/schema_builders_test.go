package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type widget struct{}

func TestCreateTableQuery_buildsOperation(t *testing.T) {
	op, err := NewDB(nil).NewCreateTable("t").
		IfNotExists().
		Column("id", Text, PrimaryKey()).
		Column("status", Text, NotNull(), Default("pending"), Check("status <> ''")).
		Column("project_id", Text, References("projects", "id"), OnDelete(Cascade)).
		Unique("org_id", "status").Named("t_org_status").
		ForeignKey("org_id").References("orgs", "id").OnDelete(Restrict).
		Operation()
	require.NoError(t, err)

	assert.Equal(t, &CreateTable{
		Table:       "t",
		IfNotExists: true,
		Columns: []Column{
			{Name: "id", Type: Text, PrimaryKey: true},
			{
				Name:    "status",
				Type:    Text,
				NotNull: true,
				Default: &DefaultValue{Value: "pending"},
				Check:   "status <> ''",
			},
			{
				Name:       "project_id",
				Type:       Text,
				References: &Reference{Table: "projects", Columns: []string{"id"}, OnDelete: Cascade},
			},
		},
		Constraints: []Constraint{
			{Kind: UniqueConstraint, Name: "t_org_status", Columns: []string{"org_id", "status"}},
			{
				Kind:       ForeignKeyConstraint,
				Columns:    []string{"org_id"},
				References: &Reference{Table: "orgs", Columns: []string{"id"}, OnDelete: Restrict},
			},
		},
	}, op)
}

func TestCreateTableQuery_columnOptions(t *testing.T) {
	op, err := NewDB(nil).NewCreateTable("t").
		Column("a", BigSerial, PrimaryKey(), Unique()).
		Column("b", Integer, Identity()).
		Column("c", Integer, GeneratedAs("a * 2")).
		Column("d", Timestamp, NotNull(), DefaultExpr("now()")).
		Column("e", Text, Default(nil)).
		Operation()
	require.NoError(t, err)

	assert.Equal(t, []Column{
		{Name: "a", Type: BigSerial, PrimaryKey: true, Unique: true},
		{Name: "b", Type: Integer, Identity: true},
		{Name: "c", Type: Integer, Generated: "a * 2"},
		{Name: "d", Type: Timestamp, NotNull: true, Default: &DefaultValue{Expr: "now()"}},
		{Name: "e", Type: Text, Default: &DefaultValue{}},
	}, op.Columns)
}

func TestCreateTableQuery_tableConstraints(t *testing.T) {
	op, err := NewDB(nil).NewCreateTable("t").
		Column("a", Text).
		PrimaryKey("a", "b").
		Check("a <> b").Named("t_a_b").
		Operation()
	require.NoError(t, err)

	assert.Equal(t, []Constraint{
		{Kind: PrimaryKeyConstraint, Columns: []string{"a", "b"}},
		{Kind: CheckConstraint, Name: "t_a_b", Check: "a <> b"},
	}, op.Constraints)
}

func TestCreateTableQuery_modelAndOptions(t *testing.T) {
	op, err := NewDB(nil).NewCreateTable("t").
		Model((*widget)(nil)).
		With("a", 1).With("b").
		Operation()
	require.NoError(t, err)

	assert.Equal(t, (*widget)(nil), op.Model)
	assert.Equal(t, []any{"a", 1, "b"}, op.Options)
	assert.Empty(t, op.Columns)
}

func TestCreateTableQuery_copiesColumnSlices(t *testing.T) {
	cols := []string{"a", "b"}
	op, err := NewDB(nil).NewCreateTable("t").Column("a", Text).Unique(cols...).Operation()
	require.NoError(t, err)

	cols[0] = "changed"
	assert.Equal(t, []string{"a", "b"}, op.Constraints[0].Columns)
}

func TestCreateTableQuery_misuse(t *testing.T) {
	newT := func() *CreateTableQuery { return NewDB(nil).NewCreateTable("t") }

	tests := []struct {
		name  string
		build func() *CreateTableQuery
	}{
		{"named with no constraint", func() *CreateTableQuery {
			return newT().Column("id", Text).Named("x")
		}},
		{"empty constraint name", func() *CreateTableQuery {
			return newT().Column("id", Text).Unique("id").Named("")
		}},
		{"references after unique", func() *CreateTableQuery {
			return newT().Column("id", Text).Unique("id").References("p", "id")
		}},
		{"references with no constraint", func() *CreateTableQuery {
			return newT().Column("id", Text).References("p", "id")
		}},
		{"references twice", func() *CreateTableQuery {
			return newT().Column("id", Text).ForeignKey("id").References("p", "id").References("q", "id")
		}},
		{"on delete after unique", func() *CreateTableQuery {
			return newT().Column("id", Text).Unique("id").OnDelete(Cascade)
		}},
		{"on delete before references", func() *CreateTableQuery {
			return newT().Column("id", Text).ForeignKey("id").OnDelete(Cascade)
		}},
		{"on delete with an unknown action", func() *CreateTableQuery {
			return newT().Column("id", Text).ForeignKey("id").References("p", "id").OnDelete("EXPLODE")
		}},
		{"on delete column option before references", func() *CreateTableQuery {
			return newT().Column("id", Text, OnDelete(Cascade))
		}},
		{"column option references twice", func() *CreateTableQuery {
			return newT().Column("id", Text, References("p", "id"), References("q", "id"))
		}},
		{"column references naming two columns", func() *CreateTableQuery {
			return newT().Column("id", Text, References("p", "a", "b"))
		}},
		{"column references with no table", func() *CreateTableQuery {
			return newT().Column("id", Text, References("", "id"))
		}},
		{"column with an empty raw type", func() *CreateTableQuery {
			return newT().Column("id", Type(""))
		}},
		{"column with the zero type", func() *CreateTableQuery {
			return newT().Column("id", ColumnType{})
		}},
		{"array of nothing", func() *CreateTableQuery {
			return newT().Column("id", ColumnType{Kind: KindArray})
		}},
		{"array of a bad element", func() *CreateTableQuery {
			return newT().Column("id", ArrayOf(Type("")))
		}},
		{"numeric with a negative precision", func() *CreateTableQuery {
			return newT().Column("id", Numeric(-1, 0))
		}},
		{"numeric with a scale and no precision", func() *CreateTableQuery {
			return newT().Column("id", Numeric(0, 2))
		}},
		{"column with an empty name", func() *CreateTableQuery {
			return newT().Column("", Text)
		}},
		{"empty default expression", func() *CreateTableQuery {
			return newT().Column("id", Text, DefaultExpr(""))
		}},
		{"empty column check", func() *CreateTableQuery {
			return newT().Column("id", Text, Check(""))
		}},
		{"empty generated expression", func() *CreateTableQuery {
			return newT().Column("id", Text, GeneratedAs(""))
		}},
		{"primary key with no columns", func() *CreateTableQuery {
			return newT().Column("id", Text).PrimaryKey()
		}},
		{"unique with an empty column name", func() *CreateTableQuery {
			return newT().Column("id", Text).Unique("id", "")
		}},
		{"empty table check", func() *CreateTableQuery {
			return newT().Column("id", Text).Check("")
		}},
		{"foreign key with no references", func() *CreateTableQuery {
			return newT().Column("id", Text).ForeignKey("id")
		}},
		{"foreign key column count differs", func() *CreateTableQuery {
			return newT().Column("id", Text).ForeignKey("a", "b").References("p", "id")
		}},
		{"no columns and no model", newT},
		{"model plus columns", func() *CreateTableQuery {
			return newT().Model((*widget)(nil)).Column("id", Text)
		}},
		{"columns plus model", func() *CreateTableQuery {
			return newT().Column("id", Text).Model((*widget)(nil))
		}},
		{"model plus constraints", func() *CreateTableQuery {
			return newT().Model((*widget)(nil)).Unique("id")
		}},
		{"nil model", func() *CreateTableQuery {
			return newT().Model(nil)
		}},
		{"empty table name", func() *CreateTableQuery {
			return NewDB(nil).NewCreateTable("").Column("id", Text)
		}},
		{"column after a table constraint", func() *CreateTableQuery {
			return newT().
				Column("org_id", Text).Column("slug", Text).
				Unique("org_id", "slug").
				Column("email", Text, Unique()).Named("t_email_key")
		}},
		{"column after a table check", func() *CreateTableQuery {
			return newT().
				Column("a", Integer).
				Check("a < 10").
				Column("b", Integer, Check("a > 0"))
		}},
		{"column after a foreign key", func() *CreateTableQuery {
			return newT().
				Column("a", Text).
				ForeignKey("a").References("p", "id").
				Column("b", Text)
		}},
		{"column check twice", func() *CreateTableQuery {
			return newT().Column("x", Integer, Check("x > 0"), Check("x < 10"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := tt.build().Operation()

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOperation)
			assert.Nil(t, op)
		})
	}
}

func TestCreateTableQuery_columnAfterConstraintExplainsTheOrder(t *testing.T) {
	_, err := NewDB(nil).NewCreateTable("t").
		Column("org_id", Text).Column("slug", Text).
		Unique("org_id", "slug").
		Column("email", Text, Unique()).Named("t_email_key").
		Operation()

	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Contains(t, err.Error(), `column "email" declared after a table constraint`)
	assert.Contains(t, err.Error(), "declare columns before table constraints")
}

func TestCreateTableQuery_columnCheckTwiceSaysSo(t *testing.T) {
	_, err := NewDB(nil).NewCreateTable("t").
		Column("x", Integer, Check("x > 0"), Check("x < 10")).
		Operation()

	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Contains(t, err.Error(), `column "x"`)
	assert.Contains(t, err.Error(), "already has a check")
}

func TestCreateTableQuery_keepsTheFirstError(t *testing.T) {
	_, err := NewDB(nil).NewCreateTable("t").
		Named("first").
		Column("", Text).
		Unique().
		Operation()

	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Contains(t, err.Error(), "Named")
	assert.NotContains(t, err.Error(), "column name")
}

func TestCreateTableQuery_columnOptionErrorNamesTheColumn(t *testing.T) {
	_, err := NewDB(nil).NewCreateTable("t").
		Column("project_id", Text, OnDelete(Cascade)).
		Operation()

	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Contains(t, err.Error(), `"project_id"`)
	assert.Contains(t, err.Error(), "migrate: ")
}

func TestColumnOption_customErrorKeepsItsChain(t *testing.T) {
	boom := errors.New("boom")
	_, err := NewDB(nil).NewCreateTable("t").
		Column("id", Text, func(*Column) error { return boom }).
		Operation()

	assert.ErrorIs(t, err, ErrInvalidOperation)
	assert.ErrorIs(t, err, boom)
}

func TestColumnOption_nilIsIgnored(t *testing.T) {
	op, err := NewDB(nil).NewCreateTable("t").Column("id", Text, nil, NotNull()).Operation()
	require.NoError(t, err)

	assert.Equal(t, []Column{{Name: "id", Type: Text, NotNull: true}}, op.Columns)
}

func TestAlterTableQuery_buildsOperation(t *testing.T) {
	op, err := NewDB(nil).NewAlterTable("t").
		AddColumn("tags", ArrayOf(Text), NotNull(), DefaultExpr("'{}'")).IfNotExists().
		DropColumn("old").IfExists().Cascade().
		DropNotNull("x").
		AddCheck("a > 0").Named("t_a_positive").
		DropConstraint("t_old").IfExists().
		RenameColumn("a", "b").
		Operation()
	require.NoError(t, err)

	assert.Equal(t, &AlterTable{
		Table: "t",
		Actions: []AlterAction{
			{
				Kind: AlterAddColumn,
				Column: Column{
					Name:    "tags",
					Type:    ArrayOf(Text),
					NotNull: true,
					Default: &DefaultValue{Expr: "'{}'"},
				},
				IfNotExists: true,
			},
			{Kind: AlterDropColumn, Name: "old", IfExists: true, Cascade: true},
			{Kind: AlterDropNotNull, Name: "x"},
			{
				Kind:       AlterAddConstraint,
				Constraint: Constraint{Kind: CheckConstraint, Name: "t_a_positive", Check: "a > 0"},
			},
			{Kind: AlterDropConstraint, Name: "t_old", IfExists: true},
			{Kind: AlterRenameColumn, Name: "a", NewName: "b"},
		},
	}, op)
}

func TestAlterTableQuery_actionShapes(t *testing.T) {
	tests := []struct {
		name  string
		build func(*AlterTableQuery) *AlterTableQuery
		want  AlterAction
	}{
		{
			"set not null",
			func(q *AlterTableQuery) *AlterTableQuery { return q.SetNotNull("c") },
			AlterAction{Kind: AlterSetNotNull, Name: "c"},
		},
		{
			"set default literal",
			func(q *AlterTableQuery) *AlterTableQuery { return q.SetDefault("c", 5) },
			AlterAction{Kind: AlterSetDefault, Name: "c", Default: &DefaultValue{Value: 5}},
		},
		{
			"set default expression",
			func(q *AlterTableQuery) *AlterTableQuery { return q.SetDefaultExpr("c", "now()") },
			AlterAction{Kind: AlterSetDefault, Name: "c", Default: &DefaultValue{Expr: "now()"}},
		},
		{
			"drop default",
			func(q *AlterTableQuery) *AlterTableQuery { return q.DropDefault("c") },
			AlterAction{Kind: AlterDropDefault, Name: "c"},
		},
		{
			"set type using",
			func(q *AlterTableQuery) *AlterTableQuery { return q.SetType("c", BigInt).Using("c::bigint") },
			AlterAction{Kind: AlterSetType, Name: "c", Type: BigInt, Using: "c::bigint"},
		},
		{
			"add primary key",
			func(q *AlterTableQuery) *AlterTableQuery { return q.AddPrimaryKey("a", "b").Named("pk") },
			AlterAction{
				Kind:       AlterAddConstraint,
				Constraint: Constraint{Kind: PrimaryKeyConstraint, Name: "pk", Columns: []string{"a", "b"}},
			},
		},
		{
			"add unique",
			func(q *AlterTableQuery) *AlterTableQuery { return q.AddUnique("a") },
			AlterAction{
				Kind:       AlterAddConstraint,
				Constraint: Constraint{Kind: UniqueConstraint, Columns: []string{"a"}},
			},
		},
		{
			"add foreign key",
			func(q *AlterTableQuery) *AlterTableQuery {
				return q.AddForeignKey("a", "b").References("p", "a", "b").OnDelete(Cascade)
			},
			AlterAction{
				Kind: AlterAddConstraint,
				Constraint: Constraint{
					Kind:       ForeignKeyConstraint,
					Columns:    []string{"a", "b"},
					References: &Reference{Table: "p", Columns: []string{"a", "b"}, OnDelete: Cascade},
				},
			},
		},
		{
			"drop constraint cascade",
			func(q *AlterTableQuery) *AlterTableQuery { return q.DropConstraint("c").Cascade() },
			AlterAction{Kind: AlterDropConstraint, Name: "c", Cascade: true},
		},
		{
			"rename table",
			func(q *AlterTableQuery) *AlterTableQuery { return q.RenameTo("u") },
			AlterAction{Kind: AlterRenameTo, NewName: "u"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := tt.build(NewDB(nil).NewAlterTable("t")).Operation()
			require.NoError(t, err)

			assert.Equal(t, []AlterAction{tt.want}, op.Actions)
		})
	}
}

func TestAlterTableQuery_misuse(t *testing.T) {
	newT := func() *AlterTableQuery { return NewDB(nil).NewAlterTable("t") }

	tests := []struct {
		name  string
		build func() *AlterTableQuery
	}{
		{"if not exists after drop column", func() *AlterTableQuery {
			return newT().DropColumn("a").IfNotExists()
		}},
		{"if not exists with no action", func() *AlterTableQuery {
			return newT().IfNotExists()
		}},
		{"if exists after add column", func() *AlterTableQuery {
			return newT().AddColumn("a", Text).IfExists()
		}},
		{"cascade after add column", func() *AlterTableQuery {
			return newT().AddColumn("a", Text).Cascade()
		}},
		{"using after add column", func() *AlterTableQuery {
			return newT().AddColumn("a", Text).Using("a::text")
		}},
		{"empty using", func() *AlterTableQuery {
			return newT().SetType("a", Text).Using("")
		}},
		{"references after add check", func() *AlterTableQuery {
			return newT().AddCheck("a > 0").References("p", "id")
		}},
		{"on delete after add unique", func() *AlterTableQuery {
			return newT().AddUnique("a").OnDelete(Cascade)
		}},
		{"on delete before references", func() *AlterTableQuery {
			return newT().AddForeignKey("a").OnDelete(Cascade)
		}},
		{"named after drop column", func() *AlterTableQuery {
			return newT().DropColumn("a").Named("x")
		}},
		{"add foreign key with no references", func() *AlterTableQuery {
			return newT().AddForeignKey("a")
		}},
		{"add foreign key column count differs", func() *AlterTableQuery {
			return newT().AddForeignKey("a", "b").References("p", "id")
		}},
		{"add primary key with no columns", func() *AlterTableQuery {
			return newT().AddPrimaryKey()
		}},
		{"add check with no expression", func() *AlterTableQuery {
			return newT().AddCheck("")
		}},
		{"add column with an empty raw type", func() *AlterTableQuery {
			return newT().AddColumn("a", Type(""))
		}},
		{"set type with the zero type", func() *AlterTableQuery {
			return newT().SetType("a", ColumnType{})
		}},
		{"drop column with an empty name", func() *AlterTableQuery {
			return newT().DropColumn("")
		}},
		{"set not null with an empty name", func() *AlterTableQuery {
			return newT().SetNotNull("")
		}},
		{"set default expression with no expression", func() *AlterTableQuery {
			return newT().SetDefaultExpr("a", "")
		}},
		{"drop constraint with an empty name", func() *AlterTableQuery {
			return newT().DropConstraint("")
		}},
		{"rename column to an empty name", func() *AlterTableQuery {
			return newT().RenameColumn("a", "")
		}},
		{"rename table to an empty name", func() *AlterTableQuery {
			return newT().RenameTo("")
		}},
		{"no actions", newT},
		{"empty table name", func() *AlterTableQuery {
			return NewDB(nil).NewAlterTable("").DropColumn("a")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := tt.build().Operation()

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOperation)
			assert.Nil(t, op)
		})
	}
}

func TestCreateIndexQuery_buildsOperation(t *testing.T) {
	op, err := NewDB(nil).NewCreateIndex("i").
		On("t").
		Columns("org_id").
		ColumnDesc("created_at").
		ColumnExpr("lower(name)").
		Unique().
		IfNotExists().
		Where("deleted_at IS NULL").
		Operation()
	require.NoError(t, err)

	assert.Equal(t, &CreateIndex{
		Name:  "i",
		Table: "t",
		Columns: []IndexColumn{
			{Name: "org_id"},
			{Name: "created_at", Desc: true},
			{Expr: "lower(name)"},
		},
		Unique:      true,
		IfNotExists: true,
		Where:       "deleted_at IS NULL",
	}, op)
}

func TestCreateIndexQuery_everyOption(t *testing.T) {
	filter := map[string]any{"deleted_at": nil}
	op, err := NewDB(nil).NewCreateIndex("i").
		On("t").
		Columns("a", "b").
		Concurrently().
		Using("gin").
		Include("c", "d").
		Filter(filter).
		With("opt").
		Operation()
	require.NoError(t, err)

	assert.Equal(t, &CreateIndex{
		Name:         "i",
		Table:        "t",
		Columns:      []IndexColumn{{Name: "a"}, {Name: "b"}},
		Concurrently: true,
		Using:        "gin",
		Include:      []string{"c", "d"},
		Filter:       filter,
		Options:      []any{"opt"},
	}, op)
}

func TestCreateIndexQuery_misuse(t *testing.T) {
	newI := func() *CreateIndexQuery { return NewDB(nil).NewCreateIndex("i") }

	tests := []struct {
		name  string
		build func() *CreateIndexQuery
	}{
		{"no on", func() *CreateIndexQuery { return newI().Columns("a") }},
		{"no columns", func() *CreateIndexQuery { return newI().On("t") }},
		{"empty name", func() *CreateIndexQuery {
			return NewDB(nil).NewCreateIndex("").On("t").Columns("a")
		}},
		{"empty on", func() *CreateIndexQuery { return newI().On("").Columns("a") }},
		{"columns with no names", func() *CreateIndexQuery { return newI().On("t").Columns() }},
		{"columns with an empty name", func() *CreateIndexQuery { return newI().On("t").Columns("a", "") }},
		{"empty column desc", func() *CreateIndexQuery { return newI().On("t").ColumnDesc("") }},
		{"empty column expression", func() *CreateIndexQuery { return newI().On("t").ColumnExpr("") }},
		{"empty using", func() *CreateIndexQuery { return newI().On("t").Columns("a").Using("") }},
		{"include with no names", func() *CreateIndexQuery { return newI().On("t").Columns("a").Include() }},
		{"empty where", func() *CreateIndexQuery { return newI().On("t").Columns("a").Where("") }},
		{"nil filter", func() *CreateIndexQuery { return newI().On("t").Columns("a").Filter(nil) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := tt.build().Operation()

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOperation)
			assert.Nil(t, op)
		})
	}
}

func TestDropTableQuery_buildsOperation(t *testing.T) {
	op, err := NewDB(nil).NewDropTable("t").IfExists().Cascade().Operation()
	require.NoError(t, err)

	assert.Equal(t, &DropTable{Table: "t", IfExists: true, Cascade: true}, op)
}

func TestDropTableQuery_misuse(t *testing.T) {
	op, err := NewDB(nil).NewDropTable("").IfExists().Operation()

	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Nil(t, op)
}

func TestDropIndexQuery_buildsOperation(t *testing.T) {
	op, err := NewDB(nil).NewDropIndex("i").On("t").IfExists().Concurrently().Cascade().Operation()
	require.NoError(t, err)

	assert.Equal(t, &DropIndex{
		Name:         "i",
		Table:        "t",
		IfExists:     true,
		Concurrently: true,
		Cascade:      true,
	}, op)
}

func TestDropIndexQuery_misuse(t *testing.T) {
	tests := []struct {
		name  string
		build func() *DropIndexQuery
	}{
		{"empty name", func() *DropIndexQuery { return NewDB(nil).NewDropIndex("") }},
		{"empty on", func() *DropIndexQuery { return NewDB(nil).NewDropIndex("i").On("") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := tt.build().Operation()

			require.ErrorIs(t, err, ErrInvalidOperation)
			assert.Nil(t, op)
		})
	}
}

func TestQuery_execReportsMisuseBeforeLookingAtTheExecutor(t *testing.T) {
	ctx := context.Background()
	db := NewDB(nil)

	for _, err := range []error{
		db.NewDropTable("").Exec(ctx),
		db.NewCreateTable("t").Exec(ctx),
		db.NewAlterTable("t").Exec(ctx),
		db.NewCreateIndex("i").Exec(ctx),
		db.NewDropIndex("").Exec(ctx),
	} {
		require.ErrorIs(t, err, ErrInvalidOperation)
		assert.NotErrorIs(t, err, ErrUnknownDriver)
	}
}

func TestQuery_execOfAValidOperationNeedsADriver(t *testing.T) {
	ctx := context.Background()
	db := NewDB(nil)

	for _, err := range []error{
		db.NewDropTable("t").Exec(ctx),
		db.NewCreateTable("t").Column("id", Text).Exec(ctx),
		db.NewAlterTable("t").DropColumn("a").Exec(ctx),
		db.NewCreateIndex("i").On("t").Columns("a").Exec(ctx),
		db.NewDropIndex("i").Exec(ctx),
	} {
		assert.ErrorIs(t, err, ErrUnknownDriver)
	}
}
