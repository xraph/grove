package mongodriver

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/schema"
)

// UpdateQuery builds and executes MongoDB update operations.
// Use MongoDB.NewUpdate() to create one.
type UpdateQuery struct {
	db         *MongoDB
	session    *mongo.Session
	collection string
	table      *schema.Table
	model      any
	filter     bson.M
	update     bson.M
	upsert     bool
	many       bool
	err        error
}

// NewUpdate creates a new UpdateQuery.
func (db *MongoDB) NewUpdate(model any) *UpdateQuery {
	q := &UpdateQuery{
		db:     db,
		model:  model,
		filter: bson.M{},
		update: bson.M{},
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
func (q *UpdateQuery) Collection(name string) *UpdateQuery {
	q.collection = name
	return q
}

// Filter sets the query filter for matching documents to update.
func (q *UpdateQuery) Filter(f bson.M) *UpdateQuery {
	for k, v := range f {
		q.filter[k] = v
	}
	return q
}

// Set adds a field to the $set update operator.
func (q *UpdateQuery) Set(field string, value any) *UpdateQuery {
	setDoc, ok := q.update["$set"].(bson.M)
	if !ok {
		setDoc = bson.M{}
	}
	setDoc[field] = value
	q.update["$set"] = setDoc
	return q
}

// SetUpdate sets the entire update document (replaces any previous $set calls).
// Use this for complex update operations like $inc, $push, $unset, etc.
func (q *UpdateQuery) SetUpdate(update bson.M) *UpdateQuery {
	q.update = update
	return q
}

// Upsert enables upsert behavior: if no document matches the filter,
// a new document is inserted.
func (q *UpdateQuery) Upsert() *UpdateQuery {
	q.upsert = true
	return q
}

// Many configures the query to update all matching documents instead of
// just the first one.
func (q *UpdateQuery) Many() *UpdateQuery {
	q.many = true
	return q
}

// GetFilter returns the current filter. Useful for testing.
func (q *UpdateQuery) GetFilter() bson.M {
	return q.filter
}

// GetUpdate returns the current update document. Useful for testing.
func (q *UpdateQuery) GetUpdate() bson.M {
	return q.update
}

// GetCollection returns the collection name. Useful for testing.
func (q *UpdateQuery) GetCollection() string {
	return q.collection
}

// IsUpsert returns whether upsert is enabled. Useful for testing.
func (q *UpdateQuery) IsUpsert() bool {
	return q.upsert
}

// IsMany returns whether the query targets multiple documents. Useful for testing.
func (q *UpdateQuery) IsMany() bool {
	return q.many
}

// Exec executes the update operation.
func (q *UpdateQuery) Exec(ctx context.Context) (*mongoResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.collection == "" {
		return nil, fmt.Errorf("mongodriver: no collection specified")
	}

	// If no explicit $set was provided, build from model fields.
	if len(q.update) == 0 {
		setDoc, err := structToUpdateMap(q.model, q.table)
		if err != nil {
			return nil, err
		}
		q.update = bson.M{"$set": setDoc}
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	if q.many {
		return q.updateMany(ctx, coll)
	}
	return q.updateOne(ctx, coll)
}

// updateOne updates a single document.
func (q *UpdateQuery) updateOne(ctx context.Context, coll *mongo.Collection) (*mongoResult, error) {
	opts := options.UpdateOne()
	if q.upsert {
		opts = opts.SetUpsert(true)
	}

	result, err := coll.UpdateOne(ctx, q.filter, q.update, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: update one: %w", err)
	}

	return &mongoResult{
		matchedCount:  result.MatchedCount,
		modifiedCount: result.ModifiedCount,
		upsertedCount: result.UpsertedCount,
	}, nil
}

// updateMany updates all matching documents.
func (q *UpdateQuery) updateMany(ctx context.Context, coll *mongo.Collection) (*mongoResult, error) {
	opts := options.UpdateMany()
	if q.upsert {
		opts = opts.SetUpsert(true)
	}

	result, err := coll.UpdateMany(ctx, q.filter, q.update, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: update many: %w", err)
	}

	return &mongoResult{
		matchedCount:  result.MatchedCount,
		modifiedCount: result.ModifiedCount,
		upsertedCount: result.UpsertedCount,
	}, nil
}
