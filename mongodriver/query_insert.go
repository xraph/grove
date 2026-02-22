package mongodriver

import (
	"context"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

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

	if val.Kind() == reflect.Slice {
		return q.insertMany(ctx, coll, val)
	}

	return q.insertOne(ctx, coll)
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
