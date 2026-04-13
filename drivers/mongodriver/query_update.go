package mongodriver

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/hook"
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

// buildUpdateHookContext creates a hook.QueryContext for update operations.
func (q *UpdateQuery) buildUpdateHookContext() *hook.QueryContext {
	var modelType reflect.Type
	if q.table != nil {
		modelType = q.table.ModelType
	}
	tableName := ""
	if q.table != nil {
		tableName = q.table.Name
	}
	op := hook.OpUpdate
	if q.many {
		op = hook.OpBulkUpdate
	}
	return &hook.QueryContext{
		Operation: op,
		Table:     tableName,
		ModelType: modelType,
	}
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

		// For upserts, add PK fields to $setOnInsert so the caller's ID
		// (e.g. a TypeID string) is used on insert instead of MongoDB
		// auto-generating an ObjectID.
		if q.upsert {
			pkDoc, pkErr := structToPKMap(q.model, q.table)
			if pkErr == nil && len(pkDoc) > 0 {
				q.update["$setOnInsert"] = pkDoc
			}
		}
	}

	qc := q.buildUpdateHookContext()

	// Run model BeforeUpdate hooks.
	if err := hook.RunModelBeforeUpdate(ctx, qc, q.model); err != nil {
		return nil, err
	}

	// Run operation-level pre-mutation hooks.
	if q.db.hooks != nil {
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("mongodriver: update denied by hook")
		}
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	var res *mongoResult
	var execErr error
	if q.many {
		res, execErr = q.updateMany(ctx, coll)
	} else {
		res, execErr = q.updateOne(ctx, coll)
	}
	if execErr != nil {
		return nil, execErr
	}

	// Run operation-level post-mutation hooks.
	if q.db.hooks != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, res); err != nil {
			return nil, err
		}
	}

	// Run model AfterUpdate hooks.
	if err := hook.RunModelAfterUpdate(ctx, qc, q.model); err != nil {
		return nil, err
	}

	return res, nil
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
