package mongodriver

import (
	"context"
	"fmt"

	"github.com/xraph/grove/schema"
)

// DropCollectionQuery drops a MongoDB collection.
type DropCollectionQuery struct {
	db         *MongoDB
	table      *schema.Table
	collection string
	err        error
}

// NewDropCollection creates a DropCollectionQuery for the given model.
//
//	mdb.NewDropCollection((*User)(nil)).Exec(ctx)
func (db *MongoDB) NewDropCollection(model any) *DropCollectionQuery {
	q := &DropCollectionQuery{
		db: db,
	}

	table, err := resolveTable(model)
	if err != nil {
		q.err = err
		return q
	}
	q.table = table
	q.collection = collectionName(table)
	return q
}

// Collection overrides the collection name derived from the model.
func (q *DropCollectionQuery) Collection(name string) *DropCollectionQuery {
	q.collection = name
	return q
}

// GetCollection returns the collection name. Useful for testing.
func (q *DropCollectionQuery) GetCollection() string {
	return q.collection
}

// Exec drops the collection.
func (q *DropCollectionQuery) Exec(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}
	if q.collection == "" {
		return fmt.Errorf("mongodriver: no collection specified")
	}

	if err := q.db.Collection(q.collection).Drop(ctx); err != nil {
		return fmt.Errorf("mongodriver: drop collection %q: %w", q.collection, err)
	}

	return nil
}
