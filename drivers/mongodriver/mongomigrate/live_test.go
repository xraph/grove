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
		assert.ErrorContains(t, err, "create table things")
	})

	t.Run("without IfNotExists an identical collection is an error too", func(t *testing.T) {
		// Mongo 7 accepts a repeated create with the same validator, so the
		// failure has to come from the renderer.
		err := migrateLive(ctx, mdb, first, upOnly("20260101000003", "create_things_identically", func(ctx context.Context, db *migrate.DB) error {
			return db.NewCreateTable("things").Column("name", migrate.Text, migrate.NotNull()).Exec(ctx)
		}))
		require.Error(t, err)
		assert.ErrorContains(t, err, "create table things: collection already exists")
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
	assert.ErrorContains(t, err, "create table model_users: collection already exists")

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

	// Mongo drops a missing collection without complaint, so IfExists changes
	// nothing and neither form fails.
	t.Run("a missing collection with IfExists", func(t *testing.T) {
		require.NoError(t, db.NewDropTable("gone").IfExists().Exec(ctx))
	})
	t.Run("a missing collection without IfExists", func(t *testing.T) {
		require.NoError(t, db.NewDropTable("gone").Exec(ctx))
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

func TestLive_createIndexIfNotExists(t *testing.T) {
	ctx := context.Background()
	mdb := openLive(t)
	db := migrate.NewDB(New(mdb))

	require.NoError(t, db.NewCreateTable("things").Column("a", migrate.Text).Column("b", migrate.Text).Exec(ctx))
	require.NoError(t, db.NewCreateIndex("things_a").On("things").Columns("a").Exec(ctx))

	t.Run("an index of that name is skipped even when it differs", func(t *testing.T) {
		err := db.NewCreateIndex("things_a").IfNotExists().On("things").Columns("b").Unique().Exec(ctx)
		require.NoError(t, err)

		key := indexDocs(t, mdb, "things")["things_a"].Key
		require.Len(t, key, 1)
		assert.Equal(t, "a", key[0].Key, "the existing index is untouched")
	})

	t.Run("without IfNotExists a clashing definition is an error", func(t *testing.T) {
		err := db.NewCreateIndex("things_a").On("things").Columns("b").Unique().Exec(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, "create index things_a")
	})

	t.Run("a missing collection is created by the first index", func(t *testing.T) {
		require.NoError(t, db.NewCreateIndex("fresh_a").IfNotExists().On("fresh").Columns("a").Exec(ctx))
		assert.Contains(t, indexDocs(t, mdb, "fresh"), "fresh_a")
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
		assert.ErrorContains(t, err, "drop index nope")
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
