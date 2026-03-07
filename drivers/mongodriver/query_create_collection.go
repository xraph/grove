package mongodriver

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/schema"
)

// CreateCollectionQuery builds and executes a MongoDB createCollection command
// with optional $jsonSchema validation derived from a Grove model.
type CreateCollectionQuery struct {
	db               *MongoDB
	table            *schema.Table
	collection       string
	validationLevel  string // "strict", "moderate", "off"
	validationAction string // "error", "warn"
	additionalProps  *bool  // controls additionalProperties in the schema
	ifNotExists      bool
	err              error
}

// NewCreateCollection creates a CreateCollectionQuery for the given model.
//
//	mdb.NewCreateCollection((*User)(nil)).
//	    ValidationLevel("strict").
//	    ValidationAction("error").
//	    AdditionalProperties(false).
//	    IfNotExists().
//	    Exec(ctx)
func (db *MongoDB) NewCreateCollection(model any) *CreateCollectionQuery {
	q := &CreateCollectionQuery{
		db:               db,
		validationLevel:  "strict",
		validationAction: "error",
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
func (q *CreateCollectionQuery) Collection(name string) *CreateCollectionQuery {
	q.collection = name
	return q
}

// ValidationLevel sets the validation level: "strict", "moderate", or "off".
// Default is "strict".
func (q *CreateCollectionQuery) ValidationLevel(level string) *CreateCollectionQuery {
	q.validationLevel = level
	return q
}

// ValidationAction sets what happens when validation fails: "error" or "warn".
// Default is "error".
func (q *CreateCollectionQuery) ValidationAction(action string) *CreateCollectionQuery {
	q.validationAction = action
	return q
}

// AdditionalProperties controls whether documents may contain fields
// not defined in the schema. Default is unset (MongoDB default: true).
func (q *CreateCollectionQuery) AdditionalProperties(allow bool) *CreateCollectionQuery {
	q.additionalProps = &allow
	return q
}

// IfNotExists makes the operation a no-op if the collection already exists.
func (q *CreateCollectionQuery) IfNotExists() *CreateCollectionQuery {
	q.ifNotExists = true
	return q
}

// GetCollection returns the collection name. Useful for testing.
func (q *CreateCollectionQuery) GetCollection() string {
	return q.collection
}

// BuildSchema generates the $jsonSchema document without executing.
// Useful for inspecting or logging the generated schema.
func (q *CreateCollectionQuery) BuildSchema() (bson.M, error) {
	if q.err != nil {
		return nil, q.err
	}
	return buildJSONSchema(q.table, q.additionalProps), nil
}

// Exec creates the collection with the generated $jsonSchema validator.
func (q *CreateCollectionQuery) Exec(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}
	if q.collection == "" {
		return fmt.Errorf("mongodriver: no collection specified")
	}

	// Check if collection exists when IfNotExists is set.
	if q.ifNotExists {
		exists, err := q.collectionExists(ctx)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
	}

	jsonSchema := buildJSONSchema(q.table, q.additionalProps)

	validator := bson.M{
		"$jsonSchema": jsonSchema,
	}

	opts := options.CreateCollection().
		SetValidator(validator)

	if q.validationLevel != "" {
		opts = opts.SetValidationLevel(q.validationLevel)
	}
	if q.validationAction != "" {
		opts = opts.SetValidationAction(q.validationAction)
	}

	if err := q.db.database.CreateCollection(ctx, q.collection, opts); err != nil {
		return fmt.Errorf("mongodriver: create collection %q: %w", q.collection, err)
	}

	return nil
}

// collectionExists checks if a collection already exists in the database.
func (q *CreateCollectionQuery) collectionExists(ctx context.Context) (bool, error) {
	collections, err := q.db.database.ListCollectionNames(ctx, bson.M{"name": q.collection})
	if err != nil {
		// If ListCollectionNames is not supported, try to create and handle the error.
		if mongo.IsNetworkError(err) {
			return false, fmt.Errorf("mongodriver: list collections: %w", err)
		}
		return false, nil
	}
	for _, name := range collections {
		if name == q.collection {
			return true, nil
		}
	}
	return false, nil
}
