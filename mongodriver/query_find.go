package mongodriver

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/schema"
)

// FindQuery builds and executes MongoDB find operations.
// Use MongoDB.NewFind() to create one.
type FindQuery struct {
	db         *MongoDB
	session    *mongo.Session
	collection string
	table      *schema.Table
	model      any
	filter     bson.M
	sort       bson.D
	projection bson.M
	limit      int64
	skip       int64
	err        error
}

// NewFind creates a new FindQuery. model can be:
//   - *[]User (slice pointer for multi-row)
//   - *User (struct pointer for single row)
//   - (*User)(nil) (nil pointer for collection reference without binding)
func (db *MongoDB) NewFind(model ...any) *FindQuery {
	q := &FindQuery{
		db:     db,
		filter: bson.M{},
	}

	if len(model) > 0 && model[0] != nil {
		q.model = model[0]
		table, err := resolveTable(model[0])
		if err != nil {
			q.err = err
		} else {
			q.table = table
			q.collection = collectionName(table)
		}
	}

	return q
}

// Collection overrides the collection name derived from the model.
func (q *FindQuery) Collection(name string) *FindQuery {
	q.collection = name
	return q
}

// Filter sets the query filter. Multiple calls merge filters with $and semantics.
func (q *FindQuery) Filter(f bson.M) *FindQuery {
	for k, v := range f {
		q.filter[k] = v
	}
	return q
}

// Sort sets the sort order. Example: bson.D{{"name", 1}, {"created_at", -1}}
func (q *FindQuery) Sort(s bson.D) *FindQuery {
	q.sort = s
	return q
}

// Project sets the projection (which fields to include/exclude).
func (q *FindQuery) Project(p bson.M) *FindQuery {
	q.projection = p
	return q
}

// Limit sets the maximum number of documents to return.
func (q *FindQuery) Limit(n int64) *FindQuery {
	q.limit = n
	return q
}

// Skip sets the number of documents to skip before returning results.
func (q *FindQuery) Skip(n int64) *FindQuery {
	q.skip = n
	return q
}

// GetFilter returns the current filter document. Useful for testing.
func (q *FindQuery) GetFilter() bson.M {
	return q.filter
}

// GetSort returns the current sort document. Useful for testing.
func (q *FindQuery) GetSort() bson.D {
	return q.sort
}

// GetProjection returns the current projection document. Useful for testing.
func (q *FindQuery) GetProjection() bson.M {
	return q.projection
}

// GetLimit returns the current limit. Useful for testing.
func (q *FindQuery) GetLimit() int64 {
	return q.limit
}

// GetSkip returns the current skip. Useful for testing.
func (q *FindQuery) GetSkip() int64 {
	return q.skip
}

// GetCollection returns the collection name. Useful for testing.
func (q *FindQuery) GetCollection() string {
	return q.collection
}

// Scan executes the find query and decodes results into the model.
// For slice pointers, it decodes all matching documents.
// For struct pointers, it decodes the first matching document.
func (q *FindQuery) Scan(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}
	if q.collection == "" {
		return fmt.Errorf("mongodriver: no collection specified")
	}

	target := q.model
	if target == nil {
		return fmt.Errorf("mongodriver: Scan requires a model; pass a model to NewFind")
	}

	coll := q.db.Collection(q.collection)

	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		innerType := targetType.Elem()
		if innerType.Kind() == reflect.Slice {
			return q.scanMany(ctx, coll, target)
		}
	}

	return q.scanOne(ctx, coll, target)
}

// scanMany executes a find and decodes all results into a slice.
func (q *FindQuery) scanMany(ctx context.Context, coll *mongo.Collection, dest any) error {
	findOpts := q.buildFindOptions()

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	cursor, err := coll.Find(ctx, q.filter, findOpts)
	if err != nil {
		return fmt.Errorf("mongodriver: find: %w", err)
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, dest); err != nil {
		return fmt.Errorf("mongodriver: decode results: %w", err)
	}

	return nil
}

// scanOne executes a find and decodes the first result into a struct.
func (q *FindQuery) scanOne(ctx context.Context, coll *mongo.Collection, dest any) error {
	findOpts := q.buildFindOneOptions()

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	result := coll.FindOne(ctx, q.filter, findOpts)
	if err := result.Err(); err != nil {
		return fmt.Errorf("mongodriver: find one: %w", err)
	}

	if err := result.Decode(dest); err != nil {
		return fmt.Errorf("mongodriver: decode: %w", err)
	}

	return nil
}

// Count returns the number of documents matching the filter.
func (q *FindQuery) Count(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	if q.collection == "" {
		return 0, fmt.Errorf("mongodriver: no collection specified")
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	count, err := coll.CountDocuments(ctx, q.filter)
	if err != nil {
		return 0, fmt.Errorf("mongodriver: count: %w", err)
	}

	return count, nil
}

// buildFindOptions constructs the MongoDB find options.
func (q *FindQuery) buildFindOptions() *options.FindOptionsBuilder {
	opts := options.Find()

	if q.sort != nil {
		opts = opts.SetSort(q.sort)
	}
	if q.projection != nil {
		opts = opts.SetProjection(q.projection)
	}
	if q.limit > 0 {
		opts = opts.SetLimit(q.limit)
	}
	if q.skip > 0 {
		opts = opts.SetSkip(q.skip)
	}

	return opts
}

// buildFindOneOptions constructs the MongoDB findOne options.
func (q *FindQuery) buildFindOneOptions() *options.FindOneOptionsBuilder {
	opts := options.FindOne()

	if q.sort != nil {
		opts = opts.SetSort(q.sort)
	}
	if q.projection != nil {
		opts = opts.SetProjection(q.projection)
	}
	if q.skip > 0 {
		opts = opts.SetSkip(q.skip)
	}

	return opts
}
