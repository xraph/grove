package mongomigrate

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/drivers/mongodriver"
	"github.com/xraph/grove/migrate"
)

// offlineExecutor is a mongomigrate executor on a driver that was never
// opened. Rendering has to settle every error it can before it touches the
// database, so a test that only expects an error never needs a server.
func offlineExecutor() *Executor {
	return New(mongodriver.New())
}

// mismatchedDoc maps to the collection "mismatched_docs".
type mismatchedDoc struct {
	ID   bson.ObjectID `grove:"id,pk"`
	Name string        `grove:"name,notnull"`
}

func createTable(t *testing.T, table string, build func(*migrate.CreateTableQuery) *migrate.CreateTableQuery) *migrate.CreateTable {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewCreateTable(table)).Operation()
	require.NoError(t, err)
	return op
}

func alterTable(t *testing.T, build func(*migrate.AlterTableQuery) *migrate.AlterTableQuery) *migrate.AlterTable {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewAlterTable("c")).Operation()
	require.NoError(t, err)
	return op
}

func createIndex(t *testing.T, build func(*migrate.CreateIndexQuery) *migrate.CreateIndexQuery) *migrate.CreateIndex {
	t.Helper()
	op, err := build(migrate.NewDB(nil).NewCreateIndex("idx")).Operation()
	require.NoError(t, err)
	return op
}

func TestJSONSchema_columns(t *testing.T) {
	op := createTable(t, "users", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.
			Column("email", migrate.Text, migrate.NotNull()).
			Column("bio", migrate.Text).
			Column("tags", migrate.ArrayOf(migrate.Text), migrate.NotNull())
	})

	got, err := jsonSchema(op, nil)
	require.NoError(t, err)

	assert.Equal(t, bson.M{
		"bsonType": "object",
		"properties": bson.M{
			"email": bson.M{"bsonType": "string"},
			"bio":   bson.M{"bsonType": bson.A{"string", "null"}},
			"tags": bson.M{
				"bsonType": "array",
				"items":    bson.M{"bsonType": "string"},
			},
		},
		"required": []string{"email", "tags"},
	}, got)
}

func TestJSONSchema_bsonTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  migrate.ColumnType
		want bson.M
	}{
		{"text", migrate.Text, bson.M{"bsonType": "string"}},
		{"smallint", migrate.SmallInt, bson.M{"bsonType": "int"}},
		{"integer", migrate.Integer, bson.M{"bsonType": "int"}},
		{"bigint", migrate.BigInt, bson.M{"bsonType": "long"}},
		{"bigserial", migrate.BigSerial, bson.M{"bsonType": "long"}},
		{"boolean", migrate.Boolean, bson.M{"bsonType": "bool"}},
		{"timestamp", migrate.Timestamp, bson.M{"bsonType": "date"}},
		{"date", migrate.Date, bson.M{"bsonType": "date"}},
		{"json", migrate.JSON, bson.M{"bsonType": "object"}},
		{"uuid", migrate.UUID, bson.M{"bsonType": "string"}},
		{"bytes", migrate.Bytes, bson.M{"bsonType": "binData"}},
		{"float", migrate.Float, bson.M{"bsonType": "double"}},
		{"numeric ignores precision and scale", migrate.Numeric(10, 2), bson.M{"bsonType": "decimal"}},
		{"raw passes through", migrate.Type("objectId"), bson.M{"bsonType": "objectId"}},
		{
			"array of integers",
			migrate.ArrayOf(migrate.Integer),
			bson.M{"bsonType": "array", "items": bson.M{"bsonType": "int"}},
		},
		{
			"array of arrays",
			migrate.ArrayOf(migrate.ArrayOf(migrate.Text)),
			bson.M{
				"bsonType": "array",
				"items": bson.M{
					"bsonType": "array",
					"items":    bson.M{"bsonType": "string"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("f", tt.typ, migrate.NotNull())
			})
			got, err := jsonSchema(op, nil)
			require.NoError(t, err)
			props, ok := got["properties"].(bson.M)
			require.True(t, ok, "properties is %T", got["properties"])
			assert.Equal(t, tt.want, props["f"])
		})
	}
}

func TestJSONSchema_nullableTypes(t *testing.T) {
	op := createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.
			Column("raw", migrate.Type("objectId")).
			Column("list", migrate.ArrayOf(migrate.Integer)).
			Column("amount", migrate.Numeric(0, 0))
	})
	got, err := jsonSchema(op, nil)
	require.NoError(t, err)

	assert.Equal(t, bson.M{
		"raw":    bson.M{"bsonType": bson.A{"objectId", "null"}},
		"list":   bson.M{"bsonType": bson.A{"array", "null"}, "items": bson.M{"bsonType": "int"}},
		"amount": bson.M{"bsonType": bson.A{"decimal", "null"}},
	}, got["properties"])
	// Mongo rejects an empty required list, so none of these may produce one.
	assert.NotContains(t, got, "required")
}

func TestJSONSchema_additionalProperties(t *testing.T) {
	op := createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("f", migrate.Text)
	})

	got, err := jsonSchema(op, nil)
	require.NoError(t, err)
	assert.NotContains(t, got, "additionalProperties", "unset leaves Mongo's own default")

	strict := false
	got, err = jsonSchema(op, &strict)
	require.NoError(t, err)
	assert.Equal(t, false, got["additionalProperties"])
}

func TestJSONSchema_brokenTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  migrate.ColumnType
	}{
		{"unset type", migrate.ColumnType{}},
		{"array without an element", migrate.ColumnType{Kind: migrate.KindArray}},
		{"empty raw type", migrate.ColumnType{Kind: migrate.KindRaw}},
		{"array of an unset type", migrate.ArrayOf(migrate.ColumnType{})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Builders refuse these, so a hand-built operation is the only way in.
			op := &migrate.CreateTable{Table: "c", Columns: []migrate.Column{{Name: "f", Type: tt.typ}}}
			_, err := jsonSchema(op, nil)
			require.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.ErrorContains(t, err, `column "f"`)
		})
	}
}

func TestIndexModel_keysAndOptions(t *testing.T) {
	filter := bson.M{"username": bson.M{"$gt": ""}}
	op := createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
		return q.On("sessions").Columns("app_id").ColumnDesc("created_at").Unique().Filter(filter)
	})

	model, err := indexModel(op)
	require.NoError(t, err)

	assert.Equal(t, bson.D{{Key: "app_id", Value: 1}, {Key: "created_at", Value: -1}}, model.Keys)
	opts := indexOptions(t, model)
	require.NotNil(t, opts.Name)
	assert.Equal(t, "idx", *opts.Name)
	require.NotNil(t, opts.Unique)
	assert.True(t, *opts.Unique)
	assert.Equal(t, filter, opts.PartialFilterExpression)
}

func TestIndexModel_plain(t *testing.T) {
	op := createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
		return q.On("sessions").Columns("a", "b")
	})

	model, err := indexModel(op)
	require.NoError(t, err)

	assert.Equal(t, bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 1}}, model.Keys)
	opts := indexOptions(t, model)
	require.NotNil(t, opts.Name)
	assert.Equal(t, "idx", *opts.Name)
	assert.Nil(t, opts.Unique, "an index that isn't unique says nothing about it")
	assert.Nil(t, opts.PartialFilterExpression)
}

func TestIndexModel_invalid(t *testing.T) {
	tests := []struct {
		name string
		op   *migrate.CreateIndex
		want string
	}{
		{"no name", &migrate.CreateIndex{Table: "c", Columns: []migrate.IndexColumn{{Name: "a"}}}, "needs a name"},
		{"no collection", &migrate.CreateIndex{Name: "i", Columns: []migrate.IndexColumn{{Name: "a"}}}, "needs a collection"},
		{"no keys", &migrate.CreateIndex{Name: "i", Table: "c"}, "has no columns"},
		{"nameless key", &migrate.CreateIndex{Name: "i", Table: "c", Columns: []migrate.IndexColumn{{}}}, "key 1"},
		{
			"driver options",
			&migrate.CreateIndex{Name: "i", Table: "c", Columns: []migrate.IndexColumn{{Name: "a"}}, Options: []any{"x"}},
			"takes no index options",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := indexModel(tt.op)
			require.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func indexOptions(t *testing.T, model mongo.IndexModel) options.IndexOptions {
	t.Helper()
	var opts options.IndexOptions
	require.NotNil(t, model.Options)
	for _, set := range model.Options.List() {
		require.NoError(t, set(&opts))
	}
	return opts
}

func TestRender_unsupported(t *testing.T) {
	tests := []struct {
		name    string
		op      migrate.Operation
		feature string
	}{
		{"column literal default", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text, migrate.Default("x"))
		}), `defaults on column "a"`},
		{"column default expression", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Timestamp, migrate.DefaultExpr("now()"))
		}), `defaults on column "a"`},
		{"column primary key", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text, migrate.PrimaryKey())
		}), `primary keys on column "a"`},
		{"column unique", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text, migrate.Unique())
		}), `unique constraints on column "a"`},
		{"column check", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Integer, migrate.Check("a > 0"))
		}), `check constraints on column "a"`},
		{"column references", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text, migrate.References("other", "id"))
		}), `foreign keys on column "a"`},
		{"column identity", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.BigInt, migrate.Identity())
		}), `identity column "a"`},
		{"column generated", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Integer, migrate.GeneratedAs("b * 2"))
		}), `generated column "a"`},
		{"table primary key", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text).PrimaryKey("a")
		}), "primary key constraints"},
		{"table unique", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text).Unique("a")
		}), "unique constraints"},
		{"table check", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Integer).Check("a > 0")
		}), "check constraints"},
		{"table foreign key", createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
			return q.Column("a", migrate.Text).ForeignKey("a").References("other", "id")
		}), "foreign key constraints"},
		{"drop table cascade", &migrate.DropTable{Table: "c", Cascade: true}, "CASCADE on drop table"},
		{"drop index cascade", &migrate.DropIndex{Name: "i", Table: "c", Cascade: true}, "CASCADE on drop index"},
		{"drop index concurrently", &migrate.DropIndex{Name: "i", Table: "c", Concurrently: true}, "concurrent index drops"},
		{"index where", createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
			return q.On("c").Columns("a").Where("a > 0")
		}), "Where predicates (use Filter)"},
		{"index using", createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
			return q.On("c").Columns("a").Using("gin")
		}), "index access methods"},
		{"index include", createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
			return q.On("c").Columns("a").Include("b")
		}), "included columns"},
		{"index concurrently", createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
			return q.On("c").Columns("a").Concurrently()
		}), "concurrent index builds"},
		{"index expression key", createIndex(t, func(q *migrate.CreateIndexQuery) *migrate.CreateIndexQuery {
			return q.On("c").Columns("a").ColumnExpr("lower(b)")
		}), "expression index keys"},
		{"alter set not null", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.SetNotNull("a")
		}), `set not null on column "a"`},
		{"alter drop not null", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.DropNotNull("a")
		}), `drop not null on column "a"`},
		{"alter set default", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.SetDefault("a", 1)
		}), `set default on column "a"`},
		{"alter set default expression", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.SetDefaultExpr("a", "now()")
		}), `set default on column "a"`},
		{"alter drop default", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.DropDefault("a")
		}), `drop default on column "a"`},
		{"alter set type", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.SetType("a", migrate.BigInt)
		}), `set type on column "a"`},
		{"alter add primary key", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddPrimaryKey("a")
		}), "primary key constraints"},
		{"alter add unique", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddUnique("a")
		}), "unique constraints"},
		{"alter add check", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddCheck("a > 0")
		}), "check constraints"},
		{"alter add foreign key", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddForeignKey("a").References("other", "id")
		}), "foreign key constraints"},
		{"alter drop constraint", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.DropConstraint("c_pkey")
		}), `drop constraint "c_pkey"`},
		{"alter rename table", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.RenameTo("d")
		}), `rename table to "d"`},
		{"alter drop column cascade", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.DropColumn("a").Cascade()
		}), "CASCADE on drop column"},
		{"alter add column default expression", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddColumn("a", migrate.Timestamp, migrate.DefaultExpr("now()"))
		}), `default expressions on column "a"`},
		{"alter add column primary key", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddColumn("a", migrate.Text, migrate.PrimaryKey())
		}), `primary keys on column "a"`},
		{"alter add column unique", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddColumn("a", migrate.Text, migrate.Unique())
		}), `unique constraints on column "a"`},
		{"alter add column references", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddColumn("a", migrate.Text, migrate.References("other", "id"))
		}), `foreign keys on column "a"`},
		{"alter add column identity", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			return q.AddColumn("a", migrate.BigInt, migrate.Identity())
		}), `identity column "a"`},
		{"alter unsupported action after supported ones", alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
			// Settled before anything runs, so the first action is never applied.
			return q.AddColumn("a", migrate.Text).SetNotNull("a")
		}), `set not null on column "a"`},
		{"alter unknown action", &migrate.AlterTable{
			Table:   "c",
			Actions: []migrate.AlterAction{{Kind: migrate.AlterKind(99), Name: "a"}},
		}, "alter action (kind 99)"},
	}

	exec := offlineExecutor()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Renderer{}.Apply(context.Background(), exec, tt.op)
			require.Error(t, err)
			assert.True(t, errors.Is(err, migrate.ErrSchemaUnsupported), "got %v", err)
			assert.Contains(t, err.Error(), "mongo does not support "+tt.feature)
		})
	}
}

func TestRender_invalid(t *testing.T) {
	tests := []struct {
		name string
		op   migrate.Operation
		want string
	}{
		{
			"model that maps to another collection",
			createTable(t, "accounts", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Model((*mismatchedDoc)(nil))
			}),
			`create table "accounts": model *mongomigrate.mismatchedDoc maps to collection "mismatched_docs"`,
		},
		{
			"model that is not a struct",
			&migrate.CreateTable{Table: "c", Model: 42},
			`create table "c" from model`,
		},
		{
			"model mixed with columns",
			&migrate.CreateTable{
				Table:   "mismatched_docs",
				Model:   (*mismatchedDoc)(nil),
				Columns: []migrate.Column{{Name: "a", Type: migrate.Text}},
			},
			"mixes a model with columns or constraints",
		},
		{
			"neither columns nor a model",
			&migrate.CreateTable{Table: "c"},
			`create table "c" has no columns and no model`,
		},
		{
			"WithCollection through With",
			createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).With(WithCollection("other"))
			}),
			"name the collection in NewCreateTable, not WithCollection",
		},
		{
			"WithIfNotExists that disagrees with the builder",
			createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).With(WithIfNotExists(true))
			}),
			"IfNotExists on the builder, not WithIfNotExists",
		},
		{
			"an option that is not a CollectionOption",
			createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).With("strict")
			}),
			"option 1 (string) is not a mongomigrate.CollectionOption",
		},
		{
			"a nil option",
			createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).With(nil)
			}),
			"option 1 (<nil>) is not a mongomigrate.CollectionOption",
		},
		{
			"a nil CollectionOption",
			createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
				return q.Column("a", migrate.Text).With(CollectionOption(nil))
			}),
			"is not a mongomigrate.CollectionOption",
		},
		{"drop index without a collection", &migrate.DropIndex{Name: "i"}, `drop index "i" needs On with a collection`},
		{"drop table without a name", &migrate.DropTable{}, "drop table needs a name"},
		{"alter table without a name", &migrate.AlterTable{
			Actions: []migrate.AlterAction{{Kind: migrate.AlterDropColumn, Name: "a"}},
		}, "alter table needs a name"},
		{"alter table without actions", &migrate.AlterTable{Table: "c"}, `alter table "c" has no actions`},
		{
			"rename a column to itself",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery { return q.RenameColumn("a", "a") }),
			`rename column "a" to itself`,
		},
		{
			"add a column with a dotted name",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("a.b", migrate.Text)
			}),
			`column name "a.b" must not contain "." or start with "$"`,
		},
		{
			"drop a column with a dollar name",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery { return q.DropColumn("$a") }),
			`column name "$a" must not contain "." or start with "$"`,
		},
		{
			"rename onto a dotted name",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery { return q.RenameColumn("a", "b.c") }),
			`column name "b.c" must not contain "." or start with "$"`,
		},
		{
			"add a column without a type",
			&migrate.AlterTable{
				Table:   "c",
				Actions: []migrate.AlterAction{{Kind: migrate.AlterAddColumn, Column: migrate.Column{Name: "a"}}},
			},
			`column "a": column type is not set`,
		},
		{
			"add a column whose default is a typed nil",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("a", migrate.Text, migrate.NotNull(), migrate.Default((*string)(nil)))
			}),
			`column "a": default <nil> (*string) does not match bsonType "string"`,
		},
		{
			"add a column whose default has the wrong type",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("a", migrate.Text, migrate.Default(5))
			}),
			`column "a": default 5 (int) does not match bsonType "string"`,
		},
		{
			"add a column whose default is out of range",
			alterTable(t, func(q *migrate.AlterTableQuery) *migrate.AlterTableQuery {
				return q.AddColumn("a", migrate.Integer, migrate.Default(int64(3_000_000_000)))
			}),
			`column "a": default 3000000000 (int64) is out of range for bsonType "int"`,
		},
		{
			"add a column without a name",
			&migrate.AlterTable{
				Table:   "c",
				Actions: []migrate.AlterAction{{Kind: migrate.AlterAddColumn, Column: migrate.Column{Type: migrate.Text}}},
			},
			"column name is empty",
		},
	}

	exec := offlineExecutor()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Renderer{}.Apply(context.Background(), exec, tt.op)
			require.ErrorIs(t, err, migrate.ErrInvalidOperation)
			assert.ErrorContains(t, err, tt.want)
			assert.Contains(t, err.Error(), "mongomigrate: ")
		})
	}
}

func TestRender_acceptsCollectionOptions(t *testing.T) {
	op := createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.IfNotExists().Column("a", migrate.Text).With(
			WithValidationLevel("moderate"),
			WithValidationAction("warn"),
			WithAdditionalProperties(false),
			WithIfNotExists(true), // agrees with the builder, so it is not a conflict
		)
	})

	got, err := collectionOptions(op)
	require.NoError(t, err)

	assert.Equal(t, "moderate", got.validationLevel)
	assert.Equal(t, "warn", got.validationAction)
	require.NotNil(t, got.additionalProps)
	assert.False(t, *got.additionalProps)
	assert.True(t, got.ifNotExists)
}

func TestCollectionOptions_ifNotExistsComesFromTheOperation(t *testing.T) {
	// helpers.go defaults this to true for CreateCollection. A migration that
	// didn't ask for IfNotExists must not inherit that.
	op := createTable(t, "c", func(q *migrate.CreateTableQuery) *migrate.CreateTableQuery {
		return q.Column("a", migrate.Text)
	})

	got, err := collectionOptions(op)
	require.NoError(t, err)

	assert.False(t, got.ifNotExists)
	assert.Equal(t, "strict", got.validationLevel)
	assert.Equal(t, "error", got.validationAction)
}

func TestRenderer_foreignExecutor(t *testing.T) {
	foreign := unusableExecutor{err: errors.New("not used")}

	err := Renderer{}.Apply(context.Background(), foreign, &migrate.DropTable{Table: "c"})

	require.Error(t, err)
	assert.EqualError(t, err, "mongomigrate: executor mongomigrate.unusableExecutor is not a mongomigrate executor")
}

func TestRenderer_executorWithoutADatabase(t *testing.T) {
	err := Renderer{}.Apply(context.Background(), New(nil), &migrate.DropTable{Table: "c"})

	require.Error(t, err)
	assert.EqualError(t, err, "mongomigrate: executor has no database")
}

// wrapper is the kind of test double loomplane puts in front of an executor.
type wrapper struct{ migrate.Executor }

func (w wrapper) Unwrap() migrate.Executor { return w.Executor }

func TestRenderer_seesThroughWrappers(t *testing.T) {
	err := Renderer{}.Apply(context.Background(), wrapper{offlineExecutor()}, &migrate.DropTable{Table: "c", Cascade: true})

	require.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
}

func TestExecutor_DriverName(t *testing.T) {
	exec := offlineExecutor()
	assert.Equal(t, "mongo", exec.DriverName())

	name, ok := migrate.DriverNameOf(wrapper{exec})
	assert.True(t, ok)
	assert.Equal(t, "mongo", name)
}

func TestExecutor_registersTheRenderer(t *testing.T) {
	// The builder finds our renderer by the executor's driver name. Without
	// one registered it would say "import its migrate package" instead.
	db := migrate.NewDB(offlineExecutor())

	err := db.NewDropTable("c").Cascade().Exec(context.Background())

	require.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	assert.ErrorContains(t, err, "CASCADE on drop table")
}

func TestNative(t *testing.T) {
	driver := mongodriver.New()

	t.Run("returns the driver handle", func(t *testing.T) {
		got, err := Native(migrate.NewDB(New(driver)))
		require.NoError(t, err)
		assert.Same(t, driver, got)
	})

	t.Run("sees through a wrapper", func(t *testing.T) {
		got, err := Native(migrate.NewDB(wrapper{New(driver)}))
		require.NoError(t, err)
		assert.Same(t, driver, got)
	})

	t.Run("rejects another executor", func(t *testing.T) {
		_, err := Native(migrate.NewDB(unusableExecutor{err: errors.New("x")}))
		assert.EqualError(t, err, "mongomigrate: executor mongomigrate.unusableExecutor is not a mongomigrate executor")
	})

	t.Run("rejects a nil handle", func(t *testing.T) {
		_, err := Native(nil)
		assert.EqualError(t, err, "mongomigrate: no migrate.DB")
	})
}

// notADriver answers to the name "mongo" without being a *mongodriver.MongoDB,
// the shape a mistaken registration or a wrapper that lost its methods has.
type notADriver struct{}

func (notADriver) Name() string { return "mongo" }

func TestRegisteredFactory(t *testing.T) {
	t.Run("builds an executor on the driver", func(t *testing.T) {
		driver := mongodriver.New()
		exec, err := migrate.NewExecutorFor(driver)
		require.NoError(t, err)

		mexec, ok := exec.(*Executor)
		require.True(t, ok, "got %T", exec)
		assert.Same(t, driver, mexec.DB())
	})

	t.Run("rejects a foreign driver at the first step", func(t *testing.T) {
		exec, err := migrate.NewExecutorFor(notADriver{})
		require.NoError(t, err)
		// A nil executor would be dereferenced by the orchestrator.
		require.NotNil(t, exec)

		_, err = migrate.NewOrchestrator(exec).Migrate(context.Background())
		require.Error(t, err)
		assert.ErrorContains(t, err, "mongomigrate: mongomigrate.notADriver is not a *mongodriver.MongoDB")
	})
}

func TestValidatorEdits(t *testing.T) {
	// before is a function so each case gets two copies: one to edit and one to
	// prove the edit left its input alone.
	tests := []struct {
		name   string
		before func() bson.M
		edit   func(bson.M) bson.M
		want   bson.M
	}{
		{
			name: "add an optional property",
			before: func() bson.M {
				return bson.M{
					"bsonType":   "object",
					"properties": bson.M{"email": bson.M{"bsonType": "string"}},
					"required":   []string{"email"},
				}
			},
			edit: func(s bson.M) bson.M {
				return addProperty(s, "bio", bson.M{"bsonType": bson.A{"string", "null"}}, false)
			},
			want: bson.M{
				"bsonType": "object",
				"properties": bson.M{
					"email": bson.M{"bsonType": "string"},
					"bio":   bson.M{"bsonType": bson.A{"string", "null"}},
				},
				"required": []string{"email"},
			},
		},
		{
			name: "add a required property after the others",
			before: func() bson.M {
				return bson.M{
					"bsonType":   "object",
					"properties": bson.M{"email": bson.M{"bsonType": "string"}},
					"required":   bson.A{"email"}, // the shape the server reports
				}
			},
			edit: func(s bson.M) bson.M {
				return addProperty(s, "status", bson.M{"bsonType": "string"}, true)
			},
			want: bson.M{
				"bsonType": "object",
				"properties": bson.M{
					"email":  bson.M{"bsonType": "string"},
					"status": bson.M{"bsonType": "string"},
				},
				"required": []string{"email", "status"},
			},
		},
		{
			name: "add a required property to a schema that requires nothing yet",
			before: func() bson.M {
				return bson.M{"bsonType": "object", "properties": bson.M{"a": bson.M{"bsonType": "int"}}}
			},
			edit: func(s bson.M) bson.M {
				return addProperty(s, "b", bson.M{"bsonType": "int"}, true)
			},
			want: bson.M{
				"bsonType": "object",
				"properties": bson.M{
					"a": bson.M{"bsonType": "int"},
					"b": bson.M{"bsonType": "int"},
				},
				"required": []string{"b"},
			},
		},
		{
			name:   "add a property to a schema with no properties",
			before: func() bson.M { return bson.M{"bsonType": "object"} },
			edit: func(s bson.M) bson.M {
				return addProperty(s, "a", bson.M{"bsonType": "int"}, false)
			},
			want: bson.M{
				"bsonType":   "object",
				"properties": bson.M{"a": bson.M{"bsonType": "int"}},
			},
		},
		{
			name: "add a required property that is already required",
			before: func() bson.M {
				return bson.M{"properties": bson.M{}, "required": []string{"a"}}
			},
			edit: func(s bson.M) bson.M {
				return addProperty(s, "a", bson.M{"bsonType": "int"}, true)
			},
			want: bson.M{
				"properties": bson.M{"a": bson.M{"bsonType": "int"}},
				"required":   []string{"a"},
			},
		},
		{
			name: "add keeps the other keywords",
			before: func() bson.M {
				return bson.M{"bsonType": "object", "properties": bson.M{}, "additionalProperties": false}
			},
			edit: func(s bson.M) bson.M {
				return addProperty(s, "a", bson.M{"bsonType": "int"}, false)
			},
			want: bson.M{
				"bsonType":             "object",
				"properties":           bson.M{"a": bson.M{"bsonType": "int"}},
				"additionalProperties": false,
			},
		},
		{
			name: "drop an optional property",
			before: func() bson.M {
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "int"}},
					"required":   []string{"a"},
				}
			},
			edit: func(s bson.M) bson.M { return dropProperty(s, "b") },
			want: bson.M{
				"properties": bson.M{"a": bson.M{"bsonType": "int"}},
				"required":   []string{"a"},
			},
		},
		{
			name: "drop a required property that others share the list with",
			before: func() bson.M {
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "int"}},
					"required":   bson.A{"a", "b"},
				}
			},
			edit: func(s bson.M) bson.M { return dropProperty(s, "a") },
			want: bson.M{
				"properties": bson.M{"b": bson.M{"bsonType": "int"}},
				"required":   []string{"b"},
			},
		},
		{
			name: "drop the last required property removes the list",
			before: func() bson.M {
				// An empty required list is itself a validator error.
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}},
					"required":   []string{"a"},
				}
			},
			edit: func(s bson.M) bson.M { return dropProperty(s, "a") },
			want: bson.M{"properties": bson.M{}},
		},
		{
			name: "drop a property the schema does not list",
			before: func() bson.M {
				return bson.M{"properties": bson.M{"a": bson.M{"bsonType": "int"}}}
			},
			edit: func(s bson.M) bson.M { return dropProperty(s, "zzz") },
			want: bson.M{"properties": bson.M{"a": bson.M{"bsonType": "int"}}},
		},
		{
			name:   "drop from a schema with no properties",
			before: func() bson.M { return bson.M{"bsonType": "object"} },
			edit:   func(s bson.M) bson.M { return dropProperty(s, "a") },
			want:   bson.M{"bsonType": "object"},
		},
		{
			name: "rename an optional property",
			before: func() bson.M {
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "string"}},
					"required":   []string{"b"},
				}
			},
			edit: func(s bson.M) bson.M { return renameProperty(s, "a", "c") },
			want: bson.M{
				"properties": bson.M{"c": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "string"}},
				"required":   []string{"b"},
			},
		},
		{
			name: "rename a required property in place",
			before: func() bson.M {
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "string"}},
					"required":   bson.A{"a", "b"},
				}
			},
			edit: func(s bson.M) bson.M { return renameProperty(s, "a", "c") },
			want: bson.M{
				"properties": bson.M{"c": bson.M{"bsonType": "int"}, "b": bson.M{"bsonType": "string"}},
				"required":   []string{"c", "b"},
			},
		},
		{
			name: "rename onto a name that is already required",
			before: func() bson.M {
				return bson.M{
					"properties": bson.M{"a": bson.M{"bsonType": "int"}},
					"required":   []string{"a", "c"},
				}
			},
			edit: func(s bson.M) bson.M { return renameProperty(s, "a", "c") },
			want: bson.M{
				"properties": bson.M{"c": bson.M{"bsonType": "int"}},
				"required":   []string{"c"},
			},
		},
		{
			name: "rename a property the schema does not list",
			before: func() bson.M {
				return bson.M{"properties": bson.M{"a": bson.M{"bsonType": "int"}}, "required": []string{"a"}}
			},
			edit: func(s bson.M) bson.M { return renameProperty(s, "zzz", "c") },
			want: bson.M{"properties": bson.M{"a": bson.M{"bsonType": "int"}}, "required": []string{"a"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.before()

			got := tt.edit(input)

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.before(), input, "the edit must not touch its input")
		})
	}
}

func TestValidatorEdits_nilSchema(t *testing.T) {
	// A validator can be read back with nothing in it. The helpers still have to
	// hand back something a collMod can carry.
	got := addProperty(nil, "a", bson.M{"bsonType": "int"}, true)

	assert.Equal(t, bson.M{
		"properties": bson.M{"a": bson.M{"bsonType": "int"}},
		"required":   []string{"a"},
	}, got)
}

func TestHasProperty(t *testing.T) {
	schema := bson.M{"properties": bson.M{"a": bson.M{"bsonType": "int"}}, "required": []string{"b"}}

	assert.True(t, hasProperty(schema, "a"))
	assert.False(t, hasProperty(schema, "b"), "a required name with no property is not a property")
	assert.False(t, hasProperty(bson.M{"bsonType": "object"}, "a"))
	assert.False(t, hasProperty(nil, "a"))
}

func TestDecodeValidator(t *testing.T) {
	collectionOptions := func(t *testing.T, doc bson.M) bson.Raw {
		t.Helper()
		raw, err := bson.Marshal(doc)
		require.NoError(t, err)
		return raw
	}

	t.Run("a collection with no options", func(t *testing.T) {
		got, err := decodeValidator(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("options without a validator", func(t *testing.T) {
		got, err := decodeValidator(collectionOptions(t, bson.M{"capped": true}))
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("nested documents come back as maps and lists as arrays", func(t *testing.T) {
		got, err := decodeValidator(collectionOptions(t, bson.M{
			"validator": bson.M{"$jsonSchema": bson.M{
				"bsonType":   "object",
				"properties": bson.M{"a": bson.M{"bsonType": bson.A{"string", "null"}}},
				"required":   bson.A{"a"},
			}},
		}))
		require.NoError(t, err)

		schema := jsonSchemaOf(got)
		require.NotNil(t, schema)
		assert.True(t, hasProperty(schema, "a"), "properties must decode as a bson.M, got %T", schema["properties"])
		assert.Equal(t, []string{"a"}, requiredOf(schema))
	})

	t.Run("a validator that is not a json schema", func(t *testing.T) {
		got, err := decodeValidator(collectionOptions(t, bson.M{
			"validator": bson.M{"a": bson.M{"$gt": 1}},
		}))
		require.NoError(t, err)

		require.NotNil(t, got)
		assert.Nil(t, jsonSchemaOf(got), "only a $jsonSchema validator is edited")
	})

	t.Run("a validator that is not a document", func(t *testing.T) {
		_, err := decodeValidator(collectionOptions(t, bson.M{"validator": "nope"}))
		require.Error(t, err)
	})
}

// namedString has a string kind but not the string type, the way an enum does.
type namedString string

func TestColumnDefault(t *testing.T) {
	dec, err := bson.ParseDecimal128("1.5")
	require.NoError(t, err)
	oid := bson.NewObjectID()
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name    string
		typ     migrate.ColumnType
		value   any
		want    any
		wantErr string
	}{
		// Go integers take the width the property names.
		{name: "int on integer", typ: migrate.Integer, value: 0, want: int32(0)},
		{name: "int64 on integer", typ: migrate.Integer, value: int64(1), want: int32(1)},
		{name: "uint8 on integer", typ: migrate.Integer, value: uint8(7), want: int32(7)},
		{name: "int on smallint", typ: migrate.SmallInt, value: -3, want: int32(-3)},
		{name: "largest int32 on integer", typ: migrate.Integer, value: int64(math.MaxInt32), want: int32(math.MaxInt32)},
		{name: "int on bigint", typ: migrate.BigInt, value: 0, want: int64(0)},
		{name: "int32 on bigint", typ: migrate.BigInt, value: int32(5), want: int64(5)},
		{name: "int on bigserial", typ: migrate.BigSerial, value: 9, want: int64(9)},
		{name: "int on float", typ: migrate.Float, value: 0, want: float64(0)},
		{name: "float32 on float", typ: migrate.Float, value: float32(1.5), want: 1.5},
		{name: "float64 on float", typ: migrate.Float, value: 2.5, want: 2.5},
		{name: "largest exact int on float", typ: migrate.Float, value: int64(1 << 53), want: float64(1 << 53)},

		// Out of range or not exact is an error and not a silent change.
		{name: "int32 overflow", typ: migrate.Integer, value: int64(math.MaxInt32) + 1, wantErr: `is out of range for bsonType "int"`},
		{name: "int32 underflow", typ: migrate.Integer, value: int64(math.MinInt32) - 1, wantErr: `is out of range for bsonType "int"`},
		{name: "uint64 overflow on bigint", typ: migrate.BigInt, value: uint64(math.MaxUint64), wantErr: `is out of range for bsonType "long"`},
		{name: "int64 too big to be exact on float", typ: migrate.Float, value: int64(1<<53) + 1, wantErr: `is out of range for bsonType "double"`},

		// A float is not turned into an integer, even a whole one.
		{name: "float on integer", typ: migrate.Integer, value: 2.0, wantErr: `does not match bsonType "int"`},
		{name: "float on bigint", typ: migrate.BigInt, value: 2.0, wantErr: `does not match bsonType "long"`},
		{name: "string on integer", typ: migrate.Integer, value: "5", wantErr: `does not match bsonType "int"`},
		{name: "bool on float", typ: migrate.Float, value: true, wantErr: `does not match bsonType "double"`},

		// These pass as they are when the value already encodes as the type.
		{name: "string on text", typ: migrate.Text, value: "x", want: "x"},
		{name: "named string on text", typ: migrate.Text, value: namedString("x"), want: namedString("x")},
		{name: "string on uuid", typ: migrate.UUID, value: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", want: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{name: "bool on boolean", typ: migrate.Boolean, value: true, want: true},
		{name: "time on timestamp", typ: migrate.Timestamp, value: when, want: when},
		{name: "time on date", typ: migrate.Date, value: when, want: when},
		{name: "bytes on bytes", typ: migrate.Bytes, value: []byte{1, 2}, want: []byte{1, 2}},
		{name: "decimal on numeric", typ: migrate.Numeric(10, 2), value: dec, want: dec},
		{name: "map on json", typ: migrate.JSON, value: bson.M{"a": 1}, want: bson.M{"a": 1}},
		{name: "int on text", typ: migrate.Text, value: 5, wantErr: `default 5 (int) does not match bsonType "string"`},
		{name: "string on boolean", typ: migrate.Boolean, value: "true", wantErr: `does not match bsonType "bool"`},
		{name: "string on timestamp", typ: migrate.Timestamp, value: "2026-01-02", wantErr: `does not match bsonType "date"`},
		{name: "string on bytes", typ: migrate.Bytes, value: "ab", wantErr: `does not match bsonType "binData"`},
		{name: "int on numeric", typ: migrate.Numeric(10, 2), value: 5, wantErr: `does not match bsonType "decimal"`},
		{name: "string on json", typ: migrate.JSON, value: "{}", wantErr: `does not match bsonType "object"`},

		// A pointer that is nil encodes as null, which no property accepts a
		// default of, nullable or not.
		{name: "typed nil pointer on text", typ: migrate.Text, value: (*string)(nil), wantErr: `does not match bsonType "string"`},
		{name: "typed nil pointer on integer", typ: migrate.Integer, value: (*int)(nil), wantErr: `does not match bsonType "int"`},
		{name: "non-nil pointer on text", typ: migrate.Text, value: ptr("x"), want: ptr("x")},

		// An array takes its elements in the element type's width.
		{name: "ints on array of integer", typ: migrate.ArrayOf(migrate.Integer), value: []int{1, 2}, want: bson.A{int32(1), int32(2)}},
		{name: "ints on array of bigint", typ: migrate.ArrayOf(migrate.BigInt), value: []int{1}, want: bson.A{int64(1)}},
		{name: "strings on array of text", typ: migrate.ArrayOf(migrate.Text), value: []string{"a"}, want: bson.A{"a"}},
		{name: "empty slice on array", typ: migrate.ArrayOf(migrate.Text), value: []string{}, want: bson.A{}},
		{name: "any slice on array", typ: migrate.ArrayOf(migrate.BigInt), value: []any{1, int8(2)}, want: bson.A{int64(1), int64(2)}},
		{name: "array of arrays", typ: migrate.ArrayOf(migrate.ArrayOf(migrate.BigInt)), value: [][]int{{1}, {2}}, want: bson.A{bson.A{int64(1)}, bson.A{int64(2)}}},
		{name: "ints on array of text", typ: migrate.ArrayOf(migrate.Text), value: []int{1}, wantErr: `does not match bsonType "string"`},
		{name: "nil slice on array", typ: migrate.ArrayOf(migrate.Text), value: []string(nil), wantErr: `does not match bsonType "array"`},
		{name: "string on array", typ: migrate.ArrayOf(migrate.Text), value: "a", wantErr: `does not match bsonType "array"`},
		{name: "bytes on array", typ: migrate.ArrayOf(migrate.Integer), value: []byte{1}, wantErr: `does not match bsonType "array"`},
		{name: "a nil element", typ: migrate.ArrayOf(migrate.Text), value: []any{"a", nil}, wantErr: `does not match bsonType "string"`},

		// A raw type is the driver's, so the value has to encode as exactly it.
		{name: "object id on raw objectId", typ: migrate.Type("objectId"), value: oid, want: oid},
		{name: "string on raw objectId", typ: migrate.Type("objectId"), value: "abc", wantErr: `does not match bsonType "objectId"`},
		{name: "int on raw int", typ: migrate.Type("int"), value: 5, want: 5},
		{name: "int64 on raw int", typ: migrate.Type("int"), value: int64(5), wantErr: `does not match bsonType "int"`},
		{name: "int on raw long", typ: migrate.Type("long"), value: 5, wantErr: `does not match bsonType "long"`},
		{name: "int64 on raw long", typ: migrate.Type("long"), value: int64(5), want: int64(5)},
		{name: "int on raw number", typ: migrate.Type("number"), value: 5, want: 5},
		{name: "float on raw number", typ: migrate.Type("number"), value: 1.5, want: 1.5},
		{name: "string on raw number", typ: migrate.Type("number"), value: "5", wantErr: `does not match bsonType "number"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &migrate.Column{Name: "c", Type: tt.typ, NotNull: true, Default: &migrate.DefaultValue{Value: tt.value}}

			got, err := columnDefault(c)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, migrate.ErrInvalidOperation)
				assert.ErrorContains(t, err, tt.wantErr)
				assert.ErrorContains(t, err, `mongomigrate: column "c": default `)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestColumnDefault_noLiteral(t *testing.T) {
	tests := []struct {
		name   string
		defVal *migrate.DefaultValue
	}{
		{"no default at all", nil},
		{"an untyped nil is DEFAULT NULL, the same as having none", &migrate.DefaultValue{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &migrate.Column{Name: "c", Type: migrate.Text, Default: tt.defVal}

			got, err := columnDefault(c)

			require.NoError(t, err)
			assert.Nil(t, got, "there is nothing to backfill")
		})
	}
}

func TestUpdateOptions_bypassOnlyWhenEditingTheValidator(t *testing.T) {
	// Bypassing validation takes a privilege readWrite lacks, and skips a
	// validator nothing is about to retire. So it is asked for only when the
	// action also edits a $jsonSchema validator.
	tests := []struct {
		name           string
		editsValidator bool
		want           bool
	}{
		{"an action that edits the validator", true, true},
		{"an action that leaves the validator alone", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var applied options.UpdateManyOptions
			for _, set := range updateOptions(tt.editsValidator).List() {
				require.NoError(t, set(&applied))
			}

			if tt.want {
				require.NotNil(t, applied.BypassDocumentValidation)
				assert.True(t, *applied.BypassDocumentValidation)
				return
			}
			assert.True(t, applied.BypassDocumentValidation == nil || !*applied.BypassDocumentValidation)
		})
	}
}
