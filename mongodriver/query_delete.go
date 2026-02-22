package mongodriver

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xraph/grove/schema"
)

// DeleteQuery builds and executes MongoDB delete operations.
// Use MongoDB.NewDelete() to create one.
type DeleteQuery struct {
	db         *MongoDB
	session    *mongo.Session
	collection string
	table      *schema.Table
	model      any
	filter     bson.M
	many       bool
	err        error
}

// NewDelete creates a new DeleteQuery.
func (db *MongoDB) NewDelete(model any) *DeleteQuery {
	q := &DeleteQuery{
		db:     db,
		model:  model,
		filter: bson.M{},
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
func (q *DeleteQuery) Collection(name string) *DeleteQuery {
	q.collection = name
	return q
}

// Filter sets the query filter for matching documents to delete.
func (q *DeleteQuery) Filter(f bson.M) *DeleteQuery {
	for k, v := range f {
		q.filter[k] = v
	}
	return q
}

// Many configures the query to delete all matching documents instead of
// just the first one.
func (q *DeleteQuery) Many() *DeleteQuery {
	q.many = true
	return q
}

// GetFilter returns the current filter. Useful for testing.
func (q *DeleteQuery) GetFilter() bson.M {
	return q.filter
}

// GetCollection returns the collection name. Useful for testing.
func (q *DeleteQuery) GetCollection() string {
	return q.collection
}

// IsMany returns whether the query targets multiple documents. Useful for testing.
func (q *DeleteQuery) IsMany() bool {
	return q.many
}

// Exec executes the delete operation.
func (q *DeleteQuery) Exec(ctx context.Context) (*mongoResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.collection == "" {
		return nil, fmt.Errorf("mongodriver: no collection specified")
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	if q.many {
		return q.deleteMany(ctx, coll)
	}
	return q.deleteOne(ctx, coll)
}

// deleteOne deletes the first matching document.
func (q *DeleteQuery) deleteOne(ctx context.Context, coll *mongo.Collection) (*mongoResult, error) {
	result, err := coll.DeleteOne(ctx, q.filter)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: delete one: %w", err)
	}

	return &mongoResult{
		deletedCount: result.DeletedCount,
	}, nil
}

// deleteMany deletes all matching documents.
func (q *DeleteQuery) deleteMany(ctx context.Context, coll *mongo.Collection) (*mongoResult, error) {
	result, err := coll.DeleteMany(ctx, q.filter)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: delete many: %w", err)
	}

	return &mongoResult{
		deletedCount: result.DeletedCount,
	}, nil
}
