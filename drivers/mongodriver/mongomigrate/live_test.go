package mongomigrate

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/drivers/mongodriver"
	"github.com/xraph/grove/migrate"
)

// liveBootTimeout bounds how long the shared container may take to accept
// connections, and is the timeout of each step of its wait. A cold start on a
// loaded machine has run past the library's 60 second default for a step.
const liveBootTimeout = 4 * time.Minute

// liveLockWait is how long a second Migrate waits for the migration lock. A
// lock a failed migration leaked would otherwise stall the test for the
// orchestrator's default budget.
const liveLockWait = 5 * time.Second

// codeDocumentValidationFailure is what Mongo answers when a write breaks the
// collection's validator.
const codeDocumentValidationFailure = 121

// sharedMongo is the one container every live test borrows. Booting one is
// slow on a busy machine, so each test gets a database of its own inside it
// instead of a container of its own.
var sharedMongo struct {
	once  sync.Once
	ctr   *tcmongo.MongoDBContainer
	uri   string // the server, with no database in the path
	err   error
	dbSeq atomic.Int64
}

// TestMain exists to stop the shared container. It starts lazily, so a run
// that touches no live test never pays for it.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedMongo.ctr != nil {
		_ = testcontainers.TerminateContainer(sharedMongo.ctr)
	}
	os.Exit(code)
}

func bootSharedMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), liveBootTimeout)
	defer cancel()

	ctr, err := tcmongo.Run(ctx, "mongo:7",
		// The same two steps the module installs by default, with the long
		// timeout on each. The deadline alone only caps the whole wait: every
		// step still gives up after its own 60 second default.
		testcontainers.WithWaitStrategyAndDeadline(liveBootTimeout,
			wait.ForLog("Waiting for connections").WithStartupTimeout(liveBootTimeout),
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(liveBootTimeout),
		),
	)
	sharedMongo.ctr = ctr
	if err != nil {
		sharedMongo.err = err
		return
	}

	sharedMongo.uri, sharedMongo.err = ctr.ConnectionString(ctx)
}

// startMongo returns the URI of a new, empty database in the shared mongo
// container. Skips when -short is set or no container runtime is available.
func startMongo(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	sharedMongo.once.Do(bootSharedMongo)
	if sharedMongo.err != nil {
		t.Skipf("container runtime unavailable: %v", sharedMongo.err)
	}

	u, err := url.Parse(sharedMongo.uri)
	require.NoError(t, err)
	u.Path = fmt.Sprintf("/live_%d", sharedMongo.dbSeq.Add(1))
	return u.String()
}

// openLive connects to a fresh database in the shared container and drops it
// when the test ends.
func openLive(t *testing.T) *mongodriver.MongoDB {
	t.Helper()
	mdb := mongodriver.New()
	require.NoError(t, mdb.Open(context.Background(), startMongo(t)))
	t.Cleanup(func() {
		if err := mdb.Database().Drop(context.Background()); err != nil {
			t.Logf("drop %s: %v", mdb.DatabaseName(), err)
		}
		require.NoError(t, mdb.Close())
	})
	return mdb
}

// migrateLive runs ms as one group through a new executor on mdb, the way an
// application would on boot.
func migrateLive(ctx context.Context, mdb *mongodriver.MongoDB, ms ...*migrate.Migration) error {
	g := migrate.NewGroup("app")
	g.MustRegister(ms...)
	_, err := migrate.NewOrchestrator(New(mdb), g).SetLockTimeout(liveLockWait).Migrate(ctx)
	return err
}

// rollbackLive rolls back the last migration of the group, through a new
// executor on mdb.
func rollbackLive(ctx context.Context, mdb *mongodriver.MongoDB, ms ...*migrate.Migration) error {
	g := migrate.NewGroup("app")
	g.MustRegister(ms...)
	_, err := migrate.NewOrchestrator(New(mdb), g).SetLockTimeout(liveLockWait).Rollback(ctx)
	return err
}

// upOnly is a migration with no Down, for tests that only build things.
func upOnly(version, name string, up migrate.Func) *migrate.Migration {
	return migrate.Define(version, name, up, nil)
}

// collectionEntry is the part of a listCollections entry the tests read.
type collectionEntry struct {
	Options struct {
		ValidationLevel  string   `bson:"validationLevel"`
		ValidationAction string   `bson:"validationAction"`
		Validator        bson.Raw `bson:"validator"`
	} `bson:"options"`
}

// collectionInfo returns the listCollections entry for name, or nil when the
// collection doesn't exist.
func collectionInfo(t *testing.T, mdb *mongodriver.MongoDB, name string) *collectionEntry {
	t.Helper()
	cursor, err := mdb.Database().ListCollections(context.Background(), bson.M{"name": name})
	require.NoError(t, err)
	var entries []collectionEntry
	require.NoError(t, cursor.All(context.Background(), &entries))
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}

// indexEntry is the part of a listIndexes entry the tests read. Decoding into
// a struct keeps nested documents in their stored order, which a bson.M would
// not.
type indexEntry struct {
	Name    string   `bson:"name"`
	Key     bson.D   `bson:"key"`
	Unique  bool     `bson:"unique"`
	Partial bson.Raw `bson:"partialFilterExpression"`
}

// indexDocs returns every index on the collection as the server reports it.
func indexDocs(t *testing.T, mdb *mongodriver.MongoDB, collection string) map[string]indexEntry {
	t.Helper()
	cursor, err := mdb.Collection(collection).Indexes().List(context.Background())
	require.NoError(t, err)
	var entries []indexEntry
	require.NoError(t, cursor.All(context.Background(), &entries))
	byName := make(map[string]indexEntry, len(entries))
	for _, e := range entries {
		byName[e.Name] = e
	}
	return byName
}

// failsValidation reports whether err is the server rejecting a document
// because of the collection's validator.
func failsValidation(err error) bool {
	var we mongo.WriteException
	if !errors.As(err, &we) {
		return false
	}
	for _, e := range we.WriteErrors {
		if e.Code == codeDocumentValidationFailure {
			return true
		}
	}
	return false
}

func TestLive_createCollectionValidates(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	up := func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("users").
			Column("email", migrate.Text, migrate.NotNull()).
			Column("age", migrate.Integer).
			Column("tags", migrate.ArrayOf(migrate.Text)).
			Exec(ctx)
	}
	down := func(ctx context.Context, db *migrate.DB) error {
		return db.NewDropTable("users").Exec(ctx)
	}
	m := migrate.Define("20260101000000", "create_users", up, down)
	require.NoError(t, migrateLive(ctx, mdb, m))

	users := mdb.Collection("users")

	_, err := users.InsertOne(ctx, bson.M{"age": 3})
	assert.True(t, failsValidation(err), "a document without email must fail validation, got %v", err)

	_, err = users.InsertOne(ctx, bson.M{"email": 5})
	assert.True(t, failsValidation(err), "an email of the wrong type must fail validation, got %v", err)

	_, err = users.InsertOne(ctx, bson.M{"email": "a@example.com"})
	require.NoError(t, err)

	// A column without NotNull accepts null as well as its type.
	_, err = users.InsertOne(ctx, bson.M{"email": "b@example.com", "age": nil, "tags": []string{"x"}})
	require.NoError(t, err)

	_, err = users.InsertOne(ctx, bson.M{"email": "c@example.com", "tags": []int{1}})
	assert.True(t, failsValidation(err), "tags of the wrong element type must fail validation, got %v", err)

	info := collectionInfo(t, mdb, "users")
	require.NotNil(t, info)
	assert.Equal(t, "strict", info.Options.ValidationLevel)
	assert.Equal(t, "error", info.Options.ValidationAction)

	require.NoError(t, rollbackLive(ctx, mdb, m))
	assert.Nil(t, collectionInfo(t, mdb, "users"), "the down migration drops the collection")
}

func TestLive_createCollectionHonoursOptions(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	err := migrateLive(ctx, mdb, upOnly("20260101000000", "create_events", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("events").
			Column("kind", migrate.Text, migrate.NotNull()).
			With(
				WithValidationLevel("moderate"),
				WithValidationAction("warn"),
				WithAdditionalProperties(false),
			).
			Exec(ctx)
	}))
	require.NoError(t, err)

	info := collectionInfo(t, mdb, "events")
	require.NotNil(t, info)
	assert.Equal(t, "moderate", info.Options.ValidationLevel)
	assert.Equal(t, "warn", info.Options.ValidationAction)

	// "warn" logs a violation and lets the write through, so this passes
	// even though the extra field breaks additionalProperties.
	_, err = mdb.Collection("events").InsertOne(ctx, bson.M{"_id": 1, "kind": "click", "extra": true})
	require.NoError(t, err)
}

func TestLive_createCollectionAdditionalPropertiesRejects(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	err := migrateLive(ctx, mdb, upOnly("20260101000000", "create_strict", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("strict_docs").
			Column("_id", migrate.Integer, migrate.NotNull()).
			Column("kind", migrate.Text, migrate.NotNull()).
			With(WithAdditionalProperties(false)).
			Exec(ctx)
	}))
	require.NoError(t, err)

	_, err = mdb.Collection("strict_docs").InsertOne(ctx, bson.M{"_id": 1, "kind": "click", "extra": true})
	assert.True(t, failsValidation(err), "an undeclared field must fail validation, got %v", err)

	_, err = mdb.Collection("strict_docs").InsertOne(ctx, bson.M{"_id": 2, "kind": "click"})
	require.NoError(t, err)
}

// modelUser maps to the collection "model_users".
type modelUser struct {
	ID    bson.ObjectID `grove:"id,pk"`
	Email string        `grove:"email,notnull"`
}

func TestLive_createCollectionFromModel(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	err := migrateLive(ctx, mdb, upOnly("20260101000000", "create_model_users", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("model_users").Model((*modelUser)(nil)).Exec(ctx)
	}))
	require.NoError(t, err)

	users := mdb.Collection("model_users")
	_, err = users.InsertOne(ctx, bson.M{})
	assert.True(t, failsValidation(err), "the model's notnull email is required, got %v", err)
	_, err = users.InsertOne(ctx, bson.M{"email": "a@example.com"})
	require.NoError(t, err)

	info := collectionInfo(t, mdb, "model_users")
	require.NotNil(t, info)
	assert.Equal(t, "strict", info.Options.ValidationLevel)
	assert.Equal(t, "error", info.Options.ValidationAction)
}

func TestLive_createCollectionFromModelNameMismatch(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	err := migrateLive(ctx, mdb, upOnly("20260101000000", "create_accounts", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("accounts").Model((*modelUser)(nil)).Exec(ctx)
	}))

	require.ErrorIs(t, err, migrate.ErrInvalidOperation)
	assert.Nil(t, collectionInfo(t, mdb, "accounts"))
	assert.Nil(t, collectionInfo(t, mdb, "model_users"), "a mismatch creates nothing under either name")
}

func TestLive_createCollectionIfNotExists(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	first := upOnly("20260101000000", "create_things", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("things").Column("name", migrate.Text, migrate.NotNull()).Exec(ctx)
	})
	require.NoError(t, migrateLive(ctx, mdb, first))

	t.Run("without IfNotExists an existing collection is an error", func(t *testing.T) {
		err := migrateLive(ctx, mdb, first, upOnly("20260101000001", "create_things_again", func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("things").Column("other", migrate.Text).Exec(ctx)
		}))
		require.Error(t, err)
		assert.ErrorContains(t, err, `create table "things"`)
	})

	t.Run("without IfNotExists an identical collection is an error too", func(t *testing.T) {
		// Mongo 7 accepts a repeated create with the same validator, so the
		// failure has to come from the renderer.
		err := migrateLive(ctx, mdb, first, upOnly("20260101000003", "create_things_identically", func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("things").Column("name", migrate.Text, migrate.NotNull()).Exec(ctx)
		}))
		require.Error(t, err)
		assert.ErrorContains(t, err, `create table "things": collection already exists`)
	})

	t.Run("with IfNotExists it is skipped and the validator stays", func(t *testing.T) {
		err := migrateLive(ctx, mdb, first, upOnly("20260101000002", "create_things_if_missing", func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("things").IfNotExists().Column("other", migrate.Text, migrate.NotNull()).Exec(ctx)
		}))
		require.NoError(t, err)

		_, err = mdb.Collection("things").InsertOne(ctx, bson.M{"name": "kept"})
		require.NoError(t, err, "the first validator, which wants name, is still in force")
		_, err = mdb.Collection("things").InsertOne(ctx, bson.M{"other": "x"})
		assert.True(t, failsValidation(err), "got %v", err)
	})
}

func TestLive_createCollectionFromModelIfNotExists(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	require.NoError(t, db.NewCreateTable("model_users").Model((*modelUser)(nil)).Exec(ctx))

	err := db.NewCreateTable("model_users").Model((*modelUser)(nil)).Exec(ctx)
	require.Error(t, err)
	assert.ErrorContains(t, err, `create table "model_users": collection already exists`)

	require.NoError(t, db.NewCreateTable("model_users").IfNotExists().Model((*modelUser)(nil)).Exec(ctx))
}

func TestLive_dropTable(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	require.NoError(t, migrateLive(ctx, mdb, upOnly("20260101000000", "create_gone", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("gone").Column("a", migrate.Text).Exec(ctx)
	})))
	require.NotNil(t, collectionInfo(t, mdb, "gone"))

	db := migrate.NewDB(New(mdb))

	require.NoError(t, db.NewDropTable("gone").Exec(ctx))
	assert.Nil(t, collectionInfo(t, mdb, "gone"))

	// Mongo 7 drops a missing collection without complaint, so the error
	// without IfExists has to come from the renderer.
	t.Run("a missing collection with IfExists", func(t *testing.T) {
		require.NoError(t, db.NewDropTable("gone").IfExists().Exec(ctx))
	})
	t.Run("a missing collection without IfExists", func(t *testing.T) {
		err := db.NewDropTable("gone").Exec(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, `drop table "gone": collection does not exist`)
	})
	t.Run("an existing collection with IfExists", func(t *testing.T) {
		require.NoError(t, db.NewCreateTable("gone").Column("a", migrate.Text).Exec(ctx))
		require.NoError(t, db.NewDropTable("gone").IfExists().Exec(ctx))
		assert.Nil(t, collectionInfo(t, mdb, "gone"))
	})
}

func TestLive_indexes(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	up := func(ctx context.Context, db *migrate.DB) error {
		err := db.NewCreateTable("accounts").
			Column("email", migrate.Text, migrate.NotNull()).
			Column("username", migrate.Text).
			Column("app_id", migrate.Text).
			Column("created_at", migrate.Timestamp).
			Exec(ctx)
		if err != nil {
			return err
		}
		err = db.NewCreateIndex("accounts_email_key").On("accounts").Columns("email").Unique().Exec(ctx)
		if err != nil {
			return err
		}
		err = db.NewCreateIndex("accounts_username_key").On("accounts").Columns("username").Unique().
			Filter(bson.M{"username": bson.M{"$gt": ""}}).Exec(ctx)
		if err != nil {
			return err
		}
		return db.NewCreateIndex("accounts_app_created").On("accounts").Columns("app_id").ColumnDesc("created_at").Exec(ctx)
	}
	require.NoError(t, migrateLive(ctx, mdb, upOnly("20260101000000", "create_accounts", up)))

	accounts := mdb.Collection("accounts")

	t.Run("a unique index rejects a duplicate", func(t *testing.T) {
		_, err := accounts.InsertOne(ctx, bson.M{"email": "a@example.com"})
		require.NoError(t, err)
		_, err = accounts.InsertOne(ctx, bson.M{"email": "a@example.com"})
		assert.True(t, mongo.IsDuplicateKeyError(err), "got %v", err)
	})

	t.Run("a partial unique index skips documents outside its filter", func(t *testing.T) {
		_, err := accounts.InsertOne(ctx, bson.M{"email": "b@example.com", "username": ""})
		require.NoError(t, err)
		_, err = accounts.InsertOne(ctx, bson.M{"email": "c@example.com", "username": ""})
		require.NoError(t, err, "two empty usernames are outside the filter, so they don't collide")

		_, err = accounts.InsertOne(ctx, bson.M{"email": "d@example.com", "username": "dave"})
		require.NoError(t, err)
		_, err = accounts.InsertOne(ctx, bson.M{"email": "e@example.com", "username": "dave"})
		assert.True(t, mongo.IsDuplicateKeyError(err), "got %v", err)
	})

	t.Run("the server holds what the builder described", func(t *testing.T) {
		docs := indexDocs(t, mdb, "accounts")

		assert.True(t, docs["accounts_email_key"].Unique)
		assert.JSONEq(t, `{"username": {"$gt": ""}}`, docs["accounts_username_key"].Partial.String())
		assert.False(t, docs["accounts_app_created"].Unique)
		assert.Empty(t, docs["accounts_app_created"].Partial)

		key := docs["accounts_app_created"].Key
		require.Len(t, key, 2)
		assert.Equal(t, "app_id", key[0].Key)
		assert.EqualValues(t, 1, key[0].Value)
		assert.Equal(t, "created_at", key[1].Key)
		assert.EqualValues(t, -1, key[1].Value)
	})
}

func TestLive_createIndexExistence(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	require.NoError(t, db.NewCreateTable("things").Column("a", migrate.Text).Column("b", migrate.Text).Exec(ctx))
	require.NoError(t, db.NewCreateIndex("things_a").On("things").Columns("a").Exec(ctx))

	t.Run("an index of that name is skipped under IfNotExists even when it differs", func(t *testing.T) {
		err := db.NewCreateIndex("things_a").IfNotExists().On("things").Columns("b").Unique().Exec(ctx)
		require.NoError(t, err)

		key := indexDocs(t, mdb, "things")["things_a"].Key
		require.Len(t, key, 1)
		assert.Equal(t, "a", key[0].Key, "the existing index is untouched")
	})

	t.Run("an identical index is skipped under IfNotExists", func(t *testing.T) {
		require.NoError(t, db.NewCreateIndex("things_a").IfNotExists().On("things").Columns("a").Exec(ctx))
	})

	t.Run("without IfNotExists an identical index is an error", func(t *testing.T) {
		// Mongo would accept this as a no-op, so the failure has to come from
		// the renderer.
		err := db.NewCreateIndex("things_a").On("things").Columns("a").Exec(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, `create index "things_a" on "things": index already exists`)
	})

	t.Run("without IfNotExists a clashing definition is an error", func(t *testing.T) {
		err := db.NewCreateIndex("things_a").On("things").Columns("b").Unique().Exec(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, `create index "things_a" on "things": index already exists`)
	})

	t.Run("a missing collection is an error with or without IfNotExists", func(t *testing.T) {
		// createIndexes would otherwise make the collection, with no validator,
		// and a typo in On would pass while the real collection kept no index.
		for name, build := range map[string]func() *migrate.CreateIndexQuery{
			"plain":       func() *migrate.CreateIndexQuery { return db.NewCreateIndex("typo_a") },
			"IfNotExists": func() *migrate.CreateIndexQuery { return db.NewCreateIndex("typo_a").IfNotExists() },
		} {
			err := build().On("thngs").Columns("a").Exec(ctx)
			require.Error(t, err, name)
			assert.ErrorContains(t, err, `create index "typo_a" on "thngs": collection does not exist`, name)
		}
		assert.Nil(t, collectionInfo(t, mdb, "thngs"), "no collection is made as a side effect")
	})
}

func TestLive_dropIndexIfExists(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	require.NoError(t, db.NewCreateTable("things").Column("a", migrate.Text).Exec(ctx))
	require.NoError(t, db.NewCreateIndex("things_a").On("things").Columns("a").Exec(ctx))

	t.Run("a missing index with IfExists", func(t *testing.T) {
		require.NoError(t, db.NewDropIndex("nope").On("things").IfExists().Exec(ctx))
	})
	t.Run("a missing index without IfExists", func(t *testing.T) {
		err := db.NewDropIndex("nope").On("things").Exec(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, `drop index "nope"`)
	})
	t.Run("an index on a missing collection with IfExists", func(t *testing.T) {
		require.NoError(t, db.NewDropIndex("nope").On("no_such_collection").IfExists().Exec(ctx))
	})
	t.Run("an index on a missing collection without IfExists", func(t *testing.T) {
		require.Error(t, db.NewDropIndex("nope").On("no_such_collection").Exec(ctx))
	})
	t.Run("an existing index", func(t *testing.T) {
		require.NoError(t, db.NewDropIndex("things_a").On("things").Exec(ctx))
		assert.NotContains(t, indexDocs(t, mdb, "things"), "things_a")
	})
	t.Run("a real failure is not swallowed by IfExists", func(t *testing.T) {
		// The server refuses to drop the _id index, which is not "not found".
		require.Error(t, db.NewDropIndex("_id_").On("things").IfExists().Exec(ctx))
	})
}

func TestLive_native(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	var got *mongodriver.MongoDB
	up := func(ctx context.Context, db *migrate.DB) error {
		native, err := Native(db)
		if err != nil {
			return err
		}
		got = native
		// A data migration no builder covers.
		_, err = native.Collection("seeds").InsertOne(ctx, bson.M{"_id": "first", "n": 1})
		return err
	}
	require.NoError(t, migrateLive(ctx, mdb, upOnly("20260101000000", "seed", up)))

	assert.Same(t, mdb, got)
	n, err := mdb.Collection("seeds").CountDocuments(ctx, bson.M{"_id": "first"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

func TestLive_unsupportedFailsTheMigration(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	err := migrateLive(ctx, mdb, upOnly("20260101000000", "bad", func(ctx context.Context, db *migrate.DB) error {
		return db.NewCreateTable("widgets").Column("id", migrate.Text, migrate.PrimaryKey()).Exec(ctx)
	}))

	require.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	assert.Nil(t, collectionInfo(t, mdb, "widgets"), "an unsupported operation leaves nothing behind")
}

// liveSchema is the part of a $jsonSchema validator the alter tests read.
type liveSchema struct {
	Properties bson.M   `bson:"properties"`
	Required   []string `bson:"required"`
}

// schemaOf returns the collection's $jsonSchema validator, or nil when it has
// none.
func schemaOf(t *testing.T, mdb *mongodriver.MongoDB, collection string) *liveSchema {
	t.Helper()
	info := collectionInfo(t, mdb, collection)
	require.NotNil(t, info, "collection %q is missing", collection)
	if len(info.Options.Validator) == 0 {
		return nil
	}
	var validator struct {
		Schema *liveSchema `bson:"$jsonSchema"`
	}
	require.NoError(t, bson.Unmarshal(info.Options.Validator, &validator))
	return validator.Schema
}

// docsOf returns every document in the collection, ordered by _id.
func docsOf(t *testing.T, mdb *mongodriver.MongoDB, collection string) []bson.M {
	t.Helper()
	ctx := context.Background()
	cursor, err := mdb.Collection(collection).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	require.NoError(t, err)
	var docs []bson.M
	require.NoError(t, cursor.All(ctx, &docs))
	return docs
}

func insertDocs(t *testing.T, mdb *mongodriver.MongoDB, collection string, docs ...any) {
	t.Helper()
	_, err := mdb.Collection(collection).InsertMany(context.Background(), docs)
	require.NoError(t, err)
}

// createUsers makes the "users" collection the alter tests start from: email
// is required and bio is optional.
func createUsers(t *testing.T, db *migrate.DB) {
	t.Helper()
	err := db.NewCreateTable("users").
		Column("email", migrate.Text, migrate.NotNull()).
		Column("bio", migrate.Text).
		Exec(context.Background())
	require.NoError(t, err)
}

func TestLive_addFieldBackfills(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	insertDocs(t, mdb, "users",
		bson.M{"_id": 1, "email": "a@example.com"},
		bson.M{"_id": 2, "email": "b@example.com"},
	)

	err := db.NewAlterTable("users").
		AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default("active")).
		Exec(ctx)
	require.NoError(t, err)

	docs := docsOf(t, mdb, "users")
	require.Len(t, docs, 2)
	for _, doc := range docs {
		assert.Equal(t, "active", doc["status"], "every existing document gets the default")
	}

	schema := schemaOf(t, mdb, "users")
	require.NotNil(t, schema)
	assert.Contains(t, schema.Properties, "status")
	assert.Equal(t, []string{"email", "status"}, schema.Required)

	users := mdb.Collection("users")
	_, err = users.InsertOne(ctx, bson.M{"email": "c@example.com"})
	assert.True(t, failsValidation(err), "a new document without status must fail validation, got %v", err)
	_, err = users.InsertOne(ctx, bson.M{"email": "c@example.com", "status": "pending"})
	require.NoError(t, err)
	_, err = users.InsertOne(ctx, bson.M{"email": "d@example.com", "status": 5})
	assert.True(t, failsValidation(err), "status keeps the type the column declared, got %v", err)
}

func TestLive_addFieldBackfillSkipsExistingValues(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	// note and tag are not in the validator yet, so the documents can carry them.
	insertDocs(t, mdb, "users",
		bson.M{"_id": 1, "email": "a@example.com"},
		bson.M{"_id": 2, "email": "b@example.com", "note": "keep", "tag": "x"},
		bson.M{"_id": 3, "email": "c@example.com", "note": nil, "tag": nil},
	)

	t.Run("an optional field fills only where it is missing", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("note", migrate.Text, migrate.Default("n/a")).Exec(ctx)
		require.NoError(t, err)

		docs := docsOf(t, mdb, "users")
		assert.Equal(t, "n/a", docs[0]["note"])
		assert.Equal(t, "keep", docs[1]["note"])
		assert.Nil(t, docs[2]["note"], "an explicit null is a value the column accepts")
		assert.Contains(t, docs[2], "note")
	})

	t.Run("a required field fills nulls too", func(t *testing.T) {
		// The property will not accept null, so a null left behind would fail
		// the document's next write.
		err := db.NewAlterTable("users").AddColumn("tag", migrate.Text, migrate.NotNull(), migrate.Default("none")).Exec(ctx)
		require.NoError(t, err)

		docs := docsOf(t, mdb, "users")
		assert.Equal(t, "none", docs[0]["tag"])
		assert.Equal(t, "x", docs[1]["tag"])
		assert.Equal(t, "none", docs[2]["tag"])
	})
}

func TestLive_addFieldBackfillBypassesValidation(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	// With additionalProperties off, the validator refuses the new field until
	// it lists it, and it can't list it until the documents hold it.
	err := db.NewCreateTable("strict_things").
		Column("_id", migrate.Integer, migrate.NotNull()).
		Column("name", migrate.Text, migrate.NotNull()).
		With(WithAdditionalProperties(false)).
		Exec(ctx)
	require.NoError(t, err)
	insertDocs(t, mdb, "strict_things", bson.M{"_id": 1, "name": "a"})

	err = db.NewAlterTable("strict_things").
		AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default("active")).
		Exec(ctx)
	require.NoError(t, err)

	assert.Equal(t, "active", docsOf(t, mdb, "strict_things")[0]["status"])
	_, err = mdb.Collection("strict_things").InsertOne(ctx, bson.M{"_id": 2, "name": "b"})
	assert.True(t, failsValidation(err), "status is required now, got %v", err)
}

func TestLive_addRequiredFieldWithoutDefaultFails(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	insertDocs(t, mdb, "users",
		bson.M{"_id": 1, "email": "a@example.com"},
		bson.M{"_id": 2, "email": "b@example.com", "status": nil},
		bson.M{"_id": 3, "email": "c@example.com", "status": "x"},
	)

	t.Run("names the count and changes nothing", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("status", migrate.Text, migrate.NotNull()).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `cannot add required field "status" to "users": 2 documents lack it`)
		assert.ErrorContains(t, err, `alter table "users": action 1 (add column "status")`)

		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.NotContains(t, schema.Properties, "status")
		assert.Equal(t, []string{"email"}, schema.Required)
	})

	t.Run("a null default is no default", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default(nil)).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, "2 documents lack it")
	})

	t.Run("an empty collection takes the field", func(t *testing.T) {
		require.NoError(t, db.NewCreateTable("fresh").Column("email", migrate.Text, migrate.NotNull()).Exec(ctx))

		err := db.NewAlterTable("fresh").AddColumn("status", migrate.Text, migrate.NotNull()).Exec(ctx)
		require.NoError(t, err)

		schema := schemaOf(t, mdb, "fresh")
		require.NotNil(t, schema)
		assert.Equal(t, []string{"email", "status"}, schema.Required)
	})

	t.Run("an optional field needs no default", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("nickname", migrate.Text).Exec(ctx)
		require.NoError(t, err)

		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.Contains(t, schema.Properties, "nickname")
		assert.Equal(t, []string{"email"}, schema.Required)
	})
}

func TestLive_dropAndRenameField(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	err := db.NewCreateTable("people").
		Column("name", migrate.Text, migrate.NotNull()).
		Column("nick", migrate.Text).
		Column("age", migrate.Integer).
		Exec(ctx)
	require.NoError(t, err)
	insertDocs(t, mdb, "people",
		bson.M{"_id": 1, "name": "ann", "nick": "a", "age": 3},
		bson.M{"_id": 2, "name": "bob", "nick": "b"},
	)

	t.Run("drop an optional field", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("people").DropColumn("nick").Exec(ctx))

		for _, doc := range docsOf(t, mdb, "people") {
			assert.NotContains(t, doc, "nick")
		}
		schema := schemaOf(t, mdb, "people")
		require.NotNil(t, schema)
		assert.NotContains(t, schema.Properties, "nick")
		assert.Contains(t, schema.Properties, "name")
		assert.Contains(t, schema.Properties, "age")
	})

	t.Run("rename a required field", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("people").RenameColumn("name", "full_name").Exec(ctx))

		docs := docsOf(t, mdb, "people")
		assert.Equal(t, "ann", docs[0]["full_name"], "a renamed field carries its values")
		assert.Equal(t, "bob", docs[1]["full_name"])
		for _, doc := range docs {
			assert.NotContains(t, doc, "name")
		}

		schema := schemaOf(t, mdb, "people")
		require.NotNil(t, schema)
		assert.Contains(t, schema.Properties, "full_name")
		assert.NotContains(t, schema.Properties, "name")
		assert.Equal(t, []string{"full_name"}, schema.Required)

		people := mdb.Collection("people")
		_, err := people.InsertOne(ctx, bson.M{"age": 1})
		assert.True(t, failsValidation(err), "the renamed field is still required, got %v", err)
		_, err = people.InsertOne(ctx, bson.M{"full_name": "cy"})
		require.NoError(t, err)
	})

	t.Run("drop a required field", func(t *testing.T) {
		// Under a strict validator the $unset would be refused, so this also
		// shows the data step doesn't trip over the validator it is retiring.
		require.NoError(t, db.NewAlterTable("people").DropColumn("full_name").Exec(ctx))

		for _, doc := range docsOf(t, mdb, "people") {
			assert.NotContains(t, doc, "full_name")
		}
		schema := schemaOf(t, mdb, "people")
		require.NotNil(t, schema)
		assert.NotContains(t, schema.Properties, "full_name")
		assert.Empty(t, schema.Required)

		_, err := mdb.Collection("people").InsertOne(ctx, bson.M{"age": 9})
		require.NoError(t, err, "nothing is required any more")
	})
}

func TestLive_alterWithoutValidator(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	require.NoError(t, mdb.Database().CreateCollection(ctx, "plain"))
	insertDocs(t, mdb, "plain",
		bson.M{"_id": 1, "name": "a"},
		bson.M{"_id": 2, "name": "b", "legacy": "x"},
	)

	noValidator := func(t *testing.T) {
		t.Helper()
		info := collectionInfo(t, mdb, "plain")
		require.NotNil(t, info)
		assert.Empty(t, info.Options.Validator, "an alter never makes a validator")
	}

	t.Run("add a field with a default", func(t *testing.T) {
		err := db.NewAlterTable("plain").
			AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default("active")).
			Exec(ctx)
		require.NoError(t, err)

		for _, doc := range docsOf(t, mdb, "plain") {
			assert.Equal(t, "active", doc["status"])
		}
		noValidator(t)
	})

	t.Run("a required field with no default still checks the documents", func(t *testing.T) {
		err := db.NewAlterTable("plain").AddColumn("owner", migrate.Text, migrate.NotNull()).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `cannot add required field "owner" to "plain": 2 documents lack it`)
		noValidator(t)
	})

	t.Run("rename and drop work on the data alone", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("plain").RenameColumn("legacy", "old").DropColumn("name").Exec(ctx))

		docs := docsOf(t, mdb, "plain")
		assert.NotContains(t, docs[0], "old")
		assert.Equal(t, "x", docs[1]["old"])
		for _, doc := range docs {
			assert.NotContains(t, doc, "name")
			assert.NotContains(t, doc, "legacy")
		}
		noValidator(t)
	})

	t.Run("with no schema a field no document has is not an error", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("plain").DropColumn("never_there").Exec(ctx))
		noValidator(t)
	})
}

func TestLive_alterMissingCollection(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	// An update on a missing collection would just match nothing, and a collMod
	// would fail late, so the check has to be ours and has to hold for every
	// modifier.
	builds := map[string]func() *migrate.AlterTableQuery{
		"add": func() *migrate.AlterTableQuery { return db.NewAlterTable("ghost").AddColumn("a", migrate.Text) },
		"add if not exists": func() *migrate.AlterTableQuery {
			return db.NewAlterTable("ghost").AddColumn("a", migrate.Text).IfNotExists()
		},
		"drop":           func() *migrate.AlterTableQuery { return db.NewAlterTable("ghost").DropColumn("a") },
		"drop if exists": func() *migrate.AlterTableQuery { return db.NewAlterTable("ghost").DropColumn("a").IfExists() },
		"rename":         func() *migrate.AlterTableQuery { return db.NewAlterTable("ghost").RenameColumn("a", "b") },
		"add with a default": func() *migrate.AlterTableQuery {
			return db.NewAlterTable("ghost").AddColumn("a", migrate.Text, migrate.Default("x"))
		},
	}
	for name, build := range builds {
		t.Run(name, func(t *testing.T) {
			err := build().Exec(ctx)

			require.Error(t, err)
			assert.ErrorContains(t, err, `alter table "ghost": collection does not exist`)
			assert.Nil(t, collectionInfo(t, mdb, "ghost"), "no collection is made as a side effect")
		})
	}
}

func TestLive_addFieldExistence(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	insertDocs(t, mdb, "users", bson.M{"_id": 1, "email": "a@example.com"})

	t.Run("a listed field without IfNotExists is an error", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("bio", migrate.Text, migrate.Default("x")).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `field "bio" already exists in the validator`)
		assert.NotContains(t, docsOf(t, mdb, "users")[0], "bio", "a refused action backfills nothing")
	})

	t.Run("a listed field with IfNotExists is a no-op", func(t *testing.T) {
		err := db.NewAlterTable("users").
			AddColumn("bio", migrate.Text, migrate.NotNull(), migrate.Default("x")).
			IfNotExists().
			Exec(ctx)
		require.NoError(t, err)

		assert.NotContains(t, docsOf(t, mdb, "users")[0], "bio", "a skipped action backfills nothing")
		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.Equal(t, []string{"email"}, schema.Required, "the existing definition stays")
	})

	t.Run("an unlisted field with IfNotExists is added", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("age", migrate.Integer).IfNotExists().Exec(ctx)
		require.NoError(t, err)

		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.Contains(t, schema.Properties, "age")
	})
}

func TestLive_dropFieldExistence(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	// extra is in the document and not in the validator.
	insertDocs(t, mdb, "users", bson.M{"_id": 1, "email": "a@example.com", "extra": "x"})

	t.Run("an unlisted field without IfExists is an error", func(t *testing.T) {
		err := db.NewAlterTable("users").DropColumn("extra").Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `field "extra" does not exist in the validator`)
		assert.Equal(t, "x", docsOf(t, mdb, "users")[0]["extra"])
	})

	t.Run("an unlisted field with IfExists is a no-op", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("users").DropColumn("extra").IfExists().Exec(ctx))

		assert.Equal(t, "x", docsOf(t, mdb, "users")[0]["extra"], "a skipped drop leaves the data alone")
	})

	t.Run("a listed field with IfExists is dropped", func(t *testing.T) {
		require.NoError(t, db.NewAlterTable("users").DropColumn("bio").IfExists().Exec(ctx))

		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.NotContains(t, schema.Properties, "bio")
	})
}

func TestLive_renameFieldExistence(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	insertDocs(t, mdb, "users", bson.M{"_id": 1, "email": "a@example.com", "bio": "hi", "extra": "x"})

	t.Run("an unlisted source is an error", func(t *testing.T) {
		err := db.NewAlterTable("users").RenameColumn("extra", "other").Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `field "extra" does not exist in the validator`)
		doc := docsOf(t, mdb, "users")[0]
		assert.Equal(t, "x", doc["extra"])
		assert.NotContains(t, doc, "other")
	})

	t.Run("a listed destination is an error", func(t *testing.T) {
		err := db.NewAlterTable("users").RenameColumn("bio", "email").Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `field "email" already exists in the validator`)
		doc := docsOf(t, mdb, "users")[0]
		assert.Equal(t, "hi", doc["bio"])
		assert.Equal(t, "a@example.com", doc["email"])
	})

	t.Run("without a validator a clash would lose data", func(t *testing.T) {
		// $rename onto a field a document already has throws that value away.
		require.NoError(t, mdb.Database().CreateCollection(ctx, "plain"))
		insertDocs(t, mdb, "plain",
			bson.M{"_id": 1, "a": "keep a", "b": "old b"},
			bson.M{"_id": 2, "a": "only a"},
		)

		err := db.NewAlterTable("plain").RenameColumn("a", "b").Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `cannot rename field "a" to "b": 1 documents already have both`)
		docs := docsOf(t, mdb, "plain")
		assert.Equal(t, "keep a", docs[0]["a"])
		assert.Equal(t, "old b", docs[0]["b"])
		assert.Equal(t, "only a", docs[1]["a"])
	})
}

func TestLive_alterActionsRunInOrder(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)

	err := db.NewAlterTable("users").
		AddColumn("a", migrate.Text).
		AddColumn("b", migrate.Text).
		DropColumn("nope").
		AddColumn("c", migrate.Text).
		Exec(ctx)

	require.Error(t, err)
	assert.ErrorContains(t, err, `alter table "users": action 3 (drop column "nope")`)

	// Mongo can't undo the first two, and the fourth never runs.
	schema := schemaOf(t, mdb, "users")
	require.NotNil(t, schema)
	assert.Contains(t, schema.Properties, "a")
	assert.Contains(t, schema.Properties, "b")
	assert.NotContains(t, schema.Properties, "c")
}

func TestLive_alterUnsupportedRunsNothing(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)

	err := db.NewAlterTable("users").AddColumn("a", migrate.Text).SetNotNull("bio").Exec(ctx)

	require.ErrorIs(t, err, migrate.ErrSchemaUnsupported)
	schema := schemaOf(t, mdb, "users")
	require.NotNil(t, schema)
	assert.NotContains(t, schema.Properties, "a", "an unsupported action is found before the first one runs")
}

func TestLive_alterInMigration(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)

	create := migrate.Define("20260101000000", "create_users",
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("users").Column("email", migrate.Text, migrate.NotNull()).Exec(ctx)
		},
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewDropTable("users").Exec(ctx)
		},
	)
	addStatus := migrate.Define("20260101000001", "add_status",
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewAlterTable("users").
				AddColumn("status", migrate.Text, migrate.NotNull(), migrate.Default("active")).
				Exec(ctx)
		},
		func(ctx context.Context, db *migrate.DB) error {
			return db.NewAlterTable("users").DropColumn("status").Exec(ctx)
		},
	)

	require.NoError(t, migrateLive(ctx, mdb, create))
	insertDocs(t, mdb, "users", bson.M{"_id": 1, "email": "a@example.com"})

	require.NoError(t, migrateLive(ctx, mdb, create, addStatus))
	assert.Equal(t, "active", docsOf(t, mdb, "users")[0]["status"])

	require.NoError(t, rollbackLive(ctx, mdb, create, addStatus))
	assert.NotContains(t, docsOf(t, mdb, "users")[0], "status")
	schema := schemaOf(t, mdb, "users")
	require.NotNil(t, schema)
	assert.NotContains(t, schema.Properties, "status")
	assert.Equal(t, []string{"email"}, schema.Required)
}

func TestLive_addFieldDefaultMatchesItsType(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	insertDocs(t, mdb, "users",
		bson.M{"_id": 1, "email": "a@example.com"},
		bson.M{"_id": 2, "email": "b@example.com"},
	)

	// A Go int encodes as int32, so each of these would otherwise be written
	// in a width its property refuses.
	err := db.NewAlterTable("users").
		AddColumn("count", migrate.BigInt, migrate.NotNull(), migrate.Default(0)).
		AddColumn("ratio", migrate.Float, migrate.NotNull(), migrate.Default(0)).
		AddColumn("rank", migrate.Integer, migrate.NotNull(), migrate.Default(int64(1))).
		AddColumn("tags", migrate.ArrayOf(migrate.BigInt), migrate.NotNull(), migrate.Default([]int{1, 2})).
		Exec(ctx)
	require.NoError(t, err)

	for _, doc := range docsOf(t, mdb, "users") {
		assert.Equal(t, int64(0), doc["count"])
		assert.Equal(t, float64(0), doc["ratio"])
		assert.Equal(t, int32(1), doc["rank"])
		assert.Equal(t, bson.A{int64(1), int64(2)}, doc["tags"])
	}

	// The backfill skipped validation, so the proof is that a normal write to a
	// backfilled document passes it now.
	users := mdb.Collection("users")
	_, err = users.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"email": "z@example.com"}})
	require.NoError(t, err, "a backfilled document must satisfy the validator it was backfilled for")
	_, err = users.UpdateOne(ctx, bson.M{"_id": 2}, bson.M{"$inc": bson.M{"count": int64(1)}})
	require.NoError(t, err)
}

func TestLive_addFieldChecksExistingValues(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))
	createUsers(t, db)
	// age is not in the validator yet, so the documents can hold anything there.
	insertDocs(t, mdb, "users",
		bson.M{"_id": 1, "email": "a@example.com"},
		bson.M{"_id": 2, "email": "b@example.com", "age": "old"},
		bson.M{"_id": 3, "email": "c@example.com", "age": int32(7)},
		bson.M{"_id": 4, "email": "d@example.com", "age": int64(8)},
	)

	t.Run("a value of another type fails the action with a count", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("age", migrate.Integer).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `cannot add field "age" to "users": 2 documents hold a value that is not bsonType "int" or "null"`)
		schema := schemaOf(t, mdb, "users")
		require.NotNil(t, schema)
		assert.NotContains(t, schema.Properties, "age")
	})

	t.Run("a required field names only its own type", func(t *testing.T) {
		err := db.NewAlterTable("users").AddColumn("age", migrate.Integer, migrate.NotNull(), migrate.Default(5)).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `2 documents hold a value that is not bsonType "int"`)
	})

	t.Run("nothing is backfilled when the check fails", func(t *testing.T) {
		docs := docsOf(t, mdb, "users")
		assert.NotContains(t, docs[0], "age", "the check runs before the first write")
	})

	t.Run("a null is a value an optional field accepts", func(t *testing.T) {
		require.NoError(t, db.NewCreateTable("nulls").Column("email", migrate.Text, migrate.NotNull()).Exec(ctx))
		insertDocs(t, mdb, "nulls", bson.M{"_id": 1, "email": "a", "age": nil}, bson.M{"_id": 2, "email": "b", "age": int32(7)})

		require.NoError(t, db.NewAlterTable("nulls").AddColumn("age", migrate.Integer).Exec(ctx))
	})

	t.Run("a null in a required field is filled with the default", func(t *testing.T) {
		require.NoError(t, db.NewCreateTable("nulls2").Column("email", migrate.Text, migrate.NotNull()).Exec(ctx))
		insertDocs(t, mdb, "nulls2", bson.M{"_id": 1, "email": "a", "age": nil}, bson.M{"_id": 2, "email": "b", "age": int32(7)})

		err := db.NewAlterTable("nulls2").AddColumn("age", migrate.Integer, migrate.NotNull(), migrate.Default(5)).Exec(ctx)
		require.NoError(t, err)

		docs := docsOf(t, mdb, "nulls2")
		assert.Equal(t, int32(5), docs[0]["age"])
		assert.Equal(t, int32(7), docs[1]["age"])
	})

	t.Run("it holds without a validator too", func(t *testing.T) {
		require.NoError(t, mdb.Database().CreateCollection(ctx, "plain"))
		insertDocs(t, mdb, "plain", bson.M{"_id": 1, "age": "old"})

		err := db.NewAlterTable("plain").AddColumn("age", migrate.Integer).Exec(ctx)

		require.Error(t, err)
		assert.ErrorContains(t, err, `cannot add field "age" to "plain": 1 documents hold a value that is not bsonType "int" or "null"`)
	})
}

func TestLive_alterRefusesAValidatorItCannotEdit(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	validators := map[string]bson.M{
		"query": {"name": bson.M{"$type": "string"}},
		"schema inside $and": {"$and": bson.A{
			bson.M{"$jsonSchema": bson.M{"bsonType": "object", "required": bson.A{"name"}}},
		}},
	}
	for label, validator := range validators {
		t.Run(label, func(t *testing.T) {
			// A write that skipped such a validator could leave documents it
			// rejects, and the edit that would repair it has nowhere to go.
			coll := "guarded"
			require.NoError(t, mdb.Database().CreateCollection(ctx, coll, options.CreateCollection().SetValidator(validator)))
			t.Cleanup(func() { require.NoError(t, mdb.Collection(coll).Drop(ctx)) })
			insertDocs(t, mdb, coll, bson.M{"_id": 1, "name": "a", "extra": "x"})

			before := docsOf(t, mdb, coll)
			infoBefore := collectionInfo(t, mdb, coll)
			require.NotNil(t, infoBefore)

			builds := map[string]func() *migrate.AlterTableQuery{
				"add": func() *migrate.AlterTableQuery { return db.NewAlterTable(coll).AddColumn("n", migrate.Text) },
				"add with default": func() *migrate.AlterTableQuery {
					return db.NewAlterTable(coll).AddColumn("n", migrate.Text, migrate.Default("x"))
				},
				"drop":           func() *migrate.AlterTableQuery { return db.NewAlterTable(coll).DropColumn("name") },
				"drop if exists": func() *migrate.AlterTableQuery { return db.NewAlterTable(coll).DropColumn("extra").IfExists() },
				"rename":         func() *migrate.AlterTableQuery { return db.NewAlterTable(coll).RenameColumn("name", "title") },
				"after a good one": func() *migrate.AlterTableQuery {
					return db.NewAlterTable(coll).AddColumn("n", migrate.Text, migrate.Default("x")).DropColumn("name")
				},
			}
			for name, build := range builds {
				err := build().Exec(ctx)

				require.ErrorIs(t, err, migrate.ErrInvalidOperation, name)
				assert.ErrorContains(t, err, `alter table "guarded": validator has no top-level $jsonSchema; alter it by hand`, name)
			}

			assert.Equal(t, before, docsOf(t, mdb, coll), "no document changed")
			infoAfter := collectionInfo(t, mdb, coll)
			require.NotNil(t, infoAfter)
			assert.Equal(t, infoBefore.Options.Validator, infoAfter.Options.Validator, "the validator is as it was")
		})
	}
}
