// Package mongodriver implements a MongoDB driver for the Grove ORM.
//
// Unlike the SQL-based drivers (pgdriver, mysqldriver), this driver uses
// MongoDB-native BSON operations (Find, InsertOne, UpdateOne, DeleteOne,
// Aggregate) instead of SQL query builders. It implements grove.GroveDriver
// and the adapter interfaces (txBeginner, queryBuilder) so that it integrates
// with the top-level grove.DB handle.
//
// Usage:
//
//	mdb := mongodriver.New()
//	err := mdb.Open(ctx, "mongodb://localhost:27017/mydb")
//	db, err := grove.Open(mdb)
//
//	// Typed access via Unwrap:
//	mongo := mongodriver.Unwrap(db)
//	mongo.NewFind(&users).Filter(bson.M{"role": "admin"}).Scan(ctx)
package mongodriver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/hook"
)

// MongoDB implements grove.GroveDriver for MongoDB using the official
// Go MongoDB driver v2. It also implements the grove adapter interfaces
// (txBeginner, queryBuilder) for integration with grove.DB.
type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
	dbName   string
	hooks    *hook.Engine
}

// New creates a new unconnected MongoDB driver. Call Open to establish
// a connection to the MongoDB server.
func New() *MongoDB {
	return &MongoDB{}
}

// Name returns the driver identifier.
func (db *MongoDB) Name() string { return "mongo" }

// Open connects to MongoDB using the given URI. The database name is
// extracted from the URI path. Use WithDatabase to override.
//
//	mdb := mongodriver.New()
//	err := mdb.Open(ctx, "mongodb://localhost:27017/mydb")
func (db *MongoDB) Open(ctx context.Context, uri string, opts ...MongoOption) error {
	mopts := defaultMongoOptions()
	mopts.apply(opts)

	clientOpts := options.Client().ApplyURI(uri)
	if mopts.ServerSelectionTimeout > 0 {
		clientOpts.SetServerSelectionTimeout(mopts.ServerSelectionTimeout)
	}
	if mopts.ConnectTimeout > 0 {
		clientOpts.SetConnectTimeout(mopts.ConnectTimeout)
	}
	if mopts.Timeout > 0 {
		clientOpts.SetTimeout(mopts.Timeout)
	}
	if mopts.MaxPoolSize > 0 {
		clientOpts.SetMaxPoolSize(mopts.MaxPoolSize)
	}
	if mopts.MinPoolSize > 0 {
		clientOpts.SetMinPoolSize(mopts.MinPoolSize)
	}
	if mopts.MaxConnIdleTime > 0 {
		clientOpts.SetMaxConnIdleTime(mopts.MaxConnIdleTime)
	}
	if mopts.MaxConnecting > 0 {
		clientOpts.SetMaxConnecting(mopts.MaxConnecting)
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return fmt.Errorf("mongodriver: connect: %w", err)
	}

	// Determine the database name.
	dbName := mopts.Database
	if dbName == "" {
		dbName = extractDBName(uri)
	}
	if dbName == "" {
		return fmt.Errorf("mongodriver: no database name in URI or options; use WithDatabase()")
	}

	db.client = client
	db.dbName = dbName
	db.database = client.Database(dbName)

	if mopts.SkipPing {
		return nil
	}

	if err := pingWithRetry(ctx, clientPinger{client}, mopts); err != nil {
		// Best-effort cleanup so we don't leak a client on failed Open.
		_ = client.Disconnect(context.Background())
		db.client = nil
		db.database = nil
		return fmt.Errorf("mongodriver: ping: %w", err)
	}

	return nil
}

// pinger is the minimal interface satisfied by *mongo.Client; it exists so
// pingWithRetry can be unit-tested without a real MongoDB server.
type pinger interface {
	Ping(ctx context.Context, rp any) error
}

// clientPinger adapts *mongo.Client to the pinger interface. The real Ping
// takes a *readpref.ReadPref; we accept any so the test fake can pass nil.
type clientPinger struct{ c *mongo.Client }

func (p clientPinger) Ping(ctx context.Context, _ any) error {
	return p.c.Ping(ctx, nil)
}

// pingWithRetry runs Ping with bounded exponential backoff. Each attempt has
// its own PingTimeout; total attempts = 1 + PingRetries. Honors ctx cancel
// during backoff sleeps.
func pingWithRetry(ctx context.Context, p pinger, mopts *mongoOptions) error {
	attempts := 1 + mopts.PingRetries
	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		attemptCtx, cancel := contextWithOptionalTimeout(ctx, mopts.PingTimeout)
		err := p.Ping(attemptCtx, nil)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == attempts-1 {
			break
		}

		delay := backoffFor(attempt, mopts.PingRetryBackoff)
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			if lastErr != nil {
				return fmt.Errorf("after %d attempt(s): %w", attempt+1, lastErr)
			}
			return ctx.Err()
		case <-t.C:
		}
	}

	return fmt.Errorf("after %d attempt(s): %w", attempts, lastErr)
}

// contextWithOptionalTimeout returns ctx unchanged when timeout <= 0.
func contextWithOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// backoffFor returns base * 2^attempt, capped at pingRetryMaxBackoff.
func backoffFor(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base << attempt
	if d <= 0 || d > pingRetryMaxBackoff {
		return pingRetryMaxBackoff
	}
	return d
}

// Close disconnects from the MongoDB server.
func (db *MongoDB) Close() error {
	if db.client != nil {
		return db.client.Disconnect(context.Background())
	}
	return nil
}

// Ping verifies that the MongoDB server is reachable.
func (db *MongoDB) Ping(ctx context.Context) error {
	if db.client == nil {
		return fmt.Errorf("mongodriver: client is not initialized; call Open first")
	}
	return db.client.Ping(ctx, nil)
}

// SetHooks attaches a hook engine for lifecycle hooks.
func (db *MongoDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Client returns the underlying mongo.Client.
func (db *MongoDB) Client() *mongo.Client {
	return db.client
}

// Database returns the underlying mongo.Database.
func (db *MongoDB) Database() *mongo.Database {
	return db.database
}

// Collection returns a mongo.Collection handle for the named collection.
func (db *MongoDB) Collection(name string) *mongo.Collection {
	return db.database.Collection(name)
}

// DatabaseName returns the database name.
func (db *MongoDB) DatabaseName() string {
	return db.dbName
}

// ---------------------------------------------------------------------------
// Grove adapter interface implementations
// ---------------------------------------------------------------------------

// GroveTx starts a MongoDB session-based transaction. This is the adapter
// method for grove.DB.BeginTx().
func (db *MongoDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("mongodriver: start session: %w", err)
	}

	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, fmt.Errorf("mongodriver: start transaction: %w", err)
	}

	return &MongoTx{session: session, db: db}, nil
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *MongoDB) GroveSelect(model ...any) any { return db.NewFind(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *MongoDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *MongoDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *MongoDB) GroveDelete(model any) any { return db.NewDelete(model) }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// extractDBName extracts the database name from a MongoDB connection URI.
// For a URI like "mongodb://host:port/mydb?opts", it returns "mydb".
func extractDBName(uri string) string {
	// Strip the scheme.
	s := uri
	if idx := strings.Index(s, "://"); idx >= 0 {
		s = s[idx+3:]
	}

	// Strip query parameters.
	if idx := strings.Index(s, "?"); idx >= 0 {
		s = s[:idx]
	}

	// Find the first "/" after the host(s).
	if idx := strings.Index(s, "/"); idx >= 0 {
		return s[idx+1:]
	}

	return ""
}
