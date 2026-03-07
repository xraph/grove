package mongodriver

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/schema"
)

// InsertQuery builds and executes MongoDB insert operations.
// Use MongoDB.NewInsert() to create one.
type InsertQuery struct {
	db         *MongoDB
	session    *mongo.Session
	collection string
	table      *schema.Table
	model      any
	err        error
}

// NewInsert creates a new InsertQuery.
// model can be a struct pointer or a pointer to a slice (for bulk insert).
func (db *MongoDB) NewInsert(model any) *InsertQuery {
	q := &InsertQuery{
		db:    db,
		model: model,
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
func (q *InsertQuery) Collection(name string) *InsertQuery {
	q.collection = name
	return q
}

// GetCollection returns the collection name. Useful for testing.
func (q *InsertQuery) GetCollection() string {
	return q.collection
}

// buildInsertHookContext creates a hook.QueryContext for insert operations.
func (q *InsertQuery) buildInsertHookContext() *hook.QueryContext {
	var modelType reflect.Type
	if q.table != nil {
		modelType = q.table.ModelType
	}
	tableName := ""
	if q.table != nil {
		tableName = q.table.Name
	}
	op := hook.OpInsert
	// Detect bulk insert.
	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			break
		}
		val = val.Elem()
	}
	if val.Kind() == reflect.Slice {
		op = hook.OpBulkInsert
	}
	return &hook.QueryContext{
		Operation: op,
		Table:     tableName,
		ModelType: modelType,
	}
}

// Exec executes the insert operation.
// For single documents, uses InsertOne.
// For slices, uses InsertMany.
func (q *InsertQuery) Exec(ctx context.Context) (*mongoResult, error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.collection == "" {
		return nil, fmt.Errorf("mongodriver: no collection specified")
	}

	qc := q.buildInsertHookContext()

	// Run model BeforeInsert hooks.
	if err := hook.RunModelBeforeInsert(ctx, qc, q.model); err != nil {
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
			return nil, fmt.Errorf("mongodriver: insert denied by hook")
		}
	}

	coll := q.db.Collection(q.collection)

	if q.session != nil {
		ctx = mongo.NewSessionContext(ctx, q.session)
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("mongodriver: nil model pointer")
		}
		val = val.Elem()
	}

	var res *mongoResult
	var execErr error
	if val.Kind() == reflect.Slice {
		res, execErr = q.insertMany(ctx, coll, val)
	} else {
		res, execErr = q.insertOne(ctx, coll)
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

	// Run model AfterInsert hooks.
	if err := hook.RunModelAfterInsert(ctx, qc, q.model); err != nil {
		return nil, err
	}

	return res, nil
}

// insertOne inserts a single document.
func (q *InsertQuery) insertOne(ctx context.Context, coll *mongo.Collection) (*mongoResult, error) {
	doc, err := structToMapInsert(q.model, q.table)
	if err != nil {
		return nil, err
	}

	result, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: insert one: %w", err)
	}

	return &mongoResult{
		insertedID: result.InsertedID,
	}, nil
}

// insertMany inserts multiple documents from a slice.
func (q *InsertQuery) insertMany(ctx context.Context, coll *mongo.Collection, sliceVal reflect.Value) (*mongoResult, error) {
	n := sliceVal.Len()
	if n == 0 {
		return nil, fmt.Errorf("mongodriver: empty slice for bulk insert")
	}

	docs := make([]any, n)
	for i := 0; i < n; i++ {
		elem := sliceVal.Index(i)
		// Get interface value; if it's a pointer, pass it as-is.
		elemIface := elem.Interface()

		doc, err := structToMapInsert(elemIface, q.table)
		if err != nil {
			return nil, fmt.Errorf("mongodriver: insert many: element %d: %w", i, err)
		}
		docs[i] = doc
	}

	result, err := coll.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("mongodriver: insert many: %w", err)
	}

	// For InsertMany, return the count of inserted IDs.
	var firstID any
	if len(result.InsertedIDs) > 0 {
		firstID = result.InsertedIDs[0]
	}

	return &mongoResult{
		insertedID:   firstID,
		matchedCount: int64(len(result.InsertedIDs)),
	}, nil
}

// buildDoc converts the model to a BSON document for insertion.
// Exported for testing purposes.
func (q *InsertQuery) BuildDoc() (bson.M, error) {
	if q.err != nil {
		return nil, q.err
	}
	return structToMapInsert(q.model, q.table)
}

// buildDocs converts a slice model to BSON documents for insertion.
// Exported for testing purposes.
func (q *InsertQuery) BuildDocs() ([]bson.M, error) {
	if q.err != nil {
		return nil, q.err
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("mongodriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		doc, err := structToMapInsert(q.model, q.table)
		if err != nil {
			return nil, err
		}
		return []bson.M{doc}, nil
	}

	n := val.Len()
	docs := make([]bson.M, n)
	for i := 0; i < n; i++ {
		elem := val.Index(i).Interface()
		doc, err := structToMapInsert(elem, q.table)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}
	return docs, nil
}
