package mongomigrate

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CreateCollection creates a MongoDB collection with JSON Schema validation
// generated from the given Grove model. The model should be a pointer to a
// struct with grove tags (e.g., (*User)(nil)).
//
// The collection name is derived from the model's BaseModel table tag.
// Schema validation is set to "strict" level with "error" action by default.
//
// Example usage in a migration:
//
//	Up: func(ctx context.Context, exec migrate.Executor) error {
//	    mexec := exec.(*mongomigrate.Executor)
//	    return mexec.CreateCollection(ctx, (*User)(nil))
//	}
func (e *Executor) CreateCollection(ctx context.Context, model any, opts ...CollectionOption) error {
	o := defaultCollectionOpts()
	for _, opt := range opts {
		opt(&o)
	}

	q := e.db.NewCreateCollection(model)

	if o.ifNotExists {
		q = q.IfNotExists()
	}
	if o.validationLevel != "" {
		q = q.ValidationLevel(o.validationLevel)
	}
	if o.validationAction != "" {
		q = q.ValidationAction(o.validationAction)
	}
	if o.additionalProps != nil {
		q = q.AdditionalProperties(*o.additionalProps)
	}
	if o.collection != "" {
		q = q.Collection(o.collection)
	}

	if err := q.Exec(ctx); err != nil {
		return fmt.Errorf("mongomigrate: create collection: %w", err)
	}

	return nil
}

// DropCollection drops the MongoDB collection associated with the given model.
//
// Example usage in a migration:
//
//	Down: func(ctx context.Context, exec migrate.Executor) error {
//	    mexec := exec.(*mongomigrate.Executor)
//	    return mexec.DropCollection(ctx, (*User)(nil))
//	}
func (e *Executor) DropCollection(ctx context.Context, model any) error {
	q := e.db.NewDropCollection(model)
	if err := q.Exec(ctx); err != nil {
		return fmt.Errorf("mongomigrate: drop collection: %w", err)
	}
	return nil
}

// RefreshValidator regenerates the $jsonSchema validator for the collection
// associated with the given model and applies it via collMod. Use this in
// migrations that update model definitions (e.g. adding nullable pointer
// fields, changing types) so existing collections pick up the new schema
// without being recreated.
//
// Example usage in a migration:
//
//	Up: func(ctx context.Context, exec migrate.Executor) error {
//	    mexec := exec.(*mongomigrate.Executor)
//	    return mexec.RefreshValidator(ctx, (*User)(nil))
//	}
func (e *Executor) RefreshValidator(ctx context.Context, model any, opts ...CollectionOption) error {
	o := defaultCollectionOpts()
	for _, opt := range opts {
		opt(&o)
	}

	q := e.db.NewCreateCollection(model)
	if o.collection != "" {
		q = q.Collection(o.collection)
	}
	if o.additionalProps != nil {
		q = q.AdditionalProperties(*o.additionalProps)
	}

	jsonSchema, err := q.BuildSchema()
	if err != nil {
		return fmt.Errorf("mongomigrate: build schema: %w", err)
	}

	cmd := bson.D{
		{Key: "collMod", Value: q.GetCollection()},
		{Key: "validator", Value: bson.M{"$jsonSchema": jsonSchema}},
	}
	if o.validationLevel != "" {
		cmd = append(cmd, bson.E{Key: "validationLevel", Value: o.validationLevel})
	}
	if o.validationAction != "" {
		cmd = append(cmd, bson.E{Key: "validationAction", Value: o.validationAction})
	}

	if err := e.db.Database().RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("mongomigrate: collMod %q: %w", q.GetCollection(), err)
	}
	return nil
}

// CreateIndexes creates the given indexes on the named collection.
//
// Example usage in a migration:
//
//	Up: func(ctx context.Context, exec migrate.Executor) error {
//	    mexec := exec.(*mongomigrate.Executor)
//	    return mexec.CreateIndexes(ctx, "users", []mongo.IndexModel{
//	        {Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
//	    })
//	}
func (e *Executor) CreateIndexes(ctx context.Context, collection string, indexes []mongo.IndexModel) error {
	if len(indexes) == 0 {
		return nil
	}

	coll := e.db.Collection(collection)
	_, err := coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("mongomigrate: create indexes on %q: %w", collection, err)
	}
	return nil
}

// CollectionOption configures collection creation behavior.
type CollectionOption func(*collectionOpts)

type collectionOpts struct {
	ifNotExists      bool
	validationLevel  string
	validationAction string
	additionalProps  *bool
	collection       string
}

func defaultCollectionOpts() collectionOpts {
	return collectionOpts{
		ifNotExists:      true,
		validationLevel:  "strict",
		validationAction: "error",
	}
}

// WithIfNotExists controls whether the operation is a no-op if the
// collection already exists. Default is true.
func WithIfNotExists(v bool) CollectionOption {
	return func(o *collectionOpts) { o.ifNotExists = v }
}

// WithValidationLevel sets the MongoDB validation level.
// Valid values: "strict", "moderate", "off". Default is "strict".
func WithValidationLevel(level string) CollectionOption {
	return func(o *collectionOpts) { o.validationLevel = level }
}

// WithValidationAction sets what happens when validation fails.
// Valid values: "error", "warn". Default is "error".
func WithValidationAction(action string) CollectionOption {
	return func(o *collectionOpts) { o.validationAction = action }
}

// WithAdditionalProperties controls whether documents may contain fields
// not defined in the schema.
func WithAdditionalProperties(allow bool) CollectionOption {
	return func(o *collectionOpts) { o.additionalProps = &allow }
}

// WithCollection overrides the collection name derived from the model.
func WithCollection(name string) CollectionOption {
	return func(o *collectionOpts) { o.collection = name }
}
