// Package mongomigrate provides a MongoDB-specific migration executor
// for the Grove migration system.
//
// Unlike SQL-based executors, MongoDB migrations use collections instead
// of tables. The migration tracking collection is "grove_migrations" and
// the lock mechanism uses a "grove_migration_locks" collection with
// findOneAndUpdate for distributed locking.
package mongomigrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/mongodriver"
	"github.com/xraph/grove/migrate"
)

func init() {
	migrate.RegisterExecutor("mongo", func(drv any) migrate.Executor {
		return New(drv.(*mongodriver.MongoDB))
	})
}

const (
	migrationCollectionName = "grove_migrations"
	lockCollectionName      = "grove_migration_locks"
	lockID                  = "grove_migration_lock"
)

// Executor implements migrate.Executor for MongoDB.
type Executor struct {
	db *mongodriver.MongoDB
}

var _ migrate.Executor = (*Executor)(nil)

// New creates a new MongoDB migration executor.
func New(db *mongodriver.MongoDB) *Executor {
	return &Executor{db: db}
}

// Exec is not supported for MongoDB migrations. MongoDB migrations should
// use the mongodriver API directly within their Up/Down functions instead
// of raw SQL. This method returns ErrNotSupported.
func (e *Executor) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return nil, mongodriver.ErrNotSupported
}

// Query is not supported for MongoDB migrations. MongoDB migrations should
// use the mongodriver API directly within their Up/Down functions instead
// of raw SQL. This method returns ErrNotSupported.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return nil, mongodriver.ErrNotSupported
}

// DB returns the underlying MongoDB driver, allowing migration functions
// to perform MongoDB operations directly.
func (e *Executor) DB() *mongodriver.MongoDB {
	return e.db
}

// EnsureMigrationTable creates the grove_migrations collection if it
// doesn't exist and ensures the necessary indexes.
func (e *Executor) EnsureMigrationTable(ctx context.Context) error {
	coll := e.db.Database().Collection(migrationCollectionName)

	// Create a unique index on (version, group).
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "version", Value: 1},
			{Key: "group", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("mongomigrate: ensure migration collection: %w", err)
	}

	return nil
}

// EnsureLockTable creates the grove_migration_locks collection if it
// doesn't exist. No special indexes needed since we use a single document.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	// MongoDB creates collections on first write. We just need the
	// collection to exist, which happens automatically.
	// We insert the lock document if it doesn't exist.
	coll := e.db.Database().Collection(lockCollectionName)

	_, err := coll.UpdateOne(
		ctx,
		bson.M{"_id": lockID},
		bson.M{
			"$setOnInsert": bson.M{
				"_id":       lockID,
				"locked":    false,
				"locked_at": nil,
				"locked_by": nil,
			},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("mongomigrate: ensure lock collection: %w", err)
	}

	return nil
}

// AcquireLock attempts to acquire the distributed migration lock using
// MongoDB's findOneAndUpdate with an atomic compare-and-swap.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	coll := e.db.Database().Collection(lockCollectionName)

	// Atomically set locked=true only if it's currently unlocked.
	result := coll.FindOneAndUpdate(
		ctx,
		bson.M{
			"_id":    lockID,
			"locked": false,
		},
		bson.M{
			"$set": bson.M{
				"locked":    true,
				"locked_at": time.Now(),
				"locked_by": lockedBy,
			},
		},
	)

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return fmt.Errorf("mongomigrate: migration lock is held by another process")
		}
		return fmt.Errorf("mongomigrate: acquire lock: %w", result.Err())
	}

	return nil
}

// ReleaseLock releases the distributed migration lock.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	coll := e.db.Database().Collection(lockCollectionName)

	_, err := coll.UpdateOne(
		ctx,
		bson.M{"_id": lockID},
		bson.M{
			"$set": bson.M{
				"locked":    false,
				"locked_at": nil,
				"locked_by": nil,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mongomigrate: release lock: %w", err)
	}

	return nil
}

// ListApplied returns all migrations that have been applied, ordered by
// migrated_at ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	coll := e.db.Database().Collection(migrationCollectionName)

	findOpts := options.Find().SetSort(bson.D{{Key: "migrated_at", Value: 1}})
	cursor, err := coll.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("mongomigrate: list applied: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []migrationDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongomigrate: decode applied: %w", err)
	}

	applied := make([]*migrate.AppliedMigration, len(docs))
	for i, doc := range docs {
		applied[i] = &migrate.AppliedMigration{
			ID:         doc.ID,
			Version:    doc.Version,
			Name:       doc.Name,
			Group:      doc.Group,
			MigratedAt: doc.MigratedAt.Format(time.RFC3339),
		}
	}

	return applied, nil
}

// RecordApplied records that a migration was successfully applied.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	coll := e.db.Database().Collection(migrationCollectionName)

	doc := bson.M{
		"version":     m.Version,
		"name":        m.Name,
		"group":       m.Group,
		"migrated_at": time.Now(),
	}

	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("mongomigrate: record applied: %w", err)
	}

	return nil
}

// RemoveApplied removes the record of an applied migration (for rollback).
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	coll := e.db.Database().Collection(migrationCollectionName)

	_, err := coll.DeleteOne(ctx, bson.M{
		"version": m.Version,
		"group":   m.Group,
	})
	if err != nil {
		return fmt.Errorf("mongomigrate: remove applied: %w", err)
	}

	return nil
}

// migrationDoc is the internal BSON structure for the migration tracking
// collection.
type migrationDoc struct {
	ID         int64     `bson:"_id,omitempty"`
	Version    string    `bson:"version"`
	Name       string    `bson:"name"`
	Group      string    `bson:"group"`
	MigratedAt time.Time `bson:"migrated_at"`
}
