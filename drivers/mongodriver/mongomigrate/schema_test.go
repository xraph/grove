package mongomigrate

import (
	"context"
	"errors"
	"testing"

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
		{"alter table", &migrate.AlterTable{Table: "c"}, "operation *migrate.AlterTable"},
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
