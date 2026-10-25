package mongomigrate

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xraph/grove/drivers/mongodriver"
	"github.com/xraph/grove/migrate"
)

// Server error codes the renderer tells apart from a real failure.
const (
	codeNamespaceNotFound = 26
	codeIndexNotFound     = 27
)

// Index key directions, as createIndexes spells them.
const (
	ascending  = 1
	descending = -1
)

// bsonTypes maps the portable types that need no parameters.
var bsonTypes = map[migrate.TypeKind]string{
	migrate.KindText:      "string",
	migrate.KindSmallInt:  "int",
	migrate.KindInteger:   "int",
	migrate.KindBigInt:    "long",
	migrate.KindBigSerial: "long",
	migrate.KindBoolean:   "bool",
	migrate.KindTimestamp: "date",
	migrate.KindDate:      "date",
	migrate.KindJSON:      "object",
	migrate.KindUUID:      "string",
	migrate.KindBytes:     "binData",
	migrate.KindFloat:     "double",
	migrate.KindNumeric:   "decimal",
}

// Renderer applies schema operations to MongoDB. It is what the mongo
// executor registers with the migrate package, so a migration's builders
// reach it.
type Renderer struct{}

var _ migrate.Renderer = Renderer{}

// Apply carries out op against the database behind exec. A collection is a
// table here and an index is an index. Anything Mongo has no equivalent for
// comes back as migrate.ErrSchemaUnsupported, so a migration written with
// SQL habits fails where it is written and not halfway through a run.
func (Renderer) Apply(ctx context.Context, exec migrate.Executor, op migrate.Operation) error {
	e, err := executorOf(exec)
	if err != nil {
		return err
	}

	switch o := op.(type) {
	case *migrate.CreateTable:
		return e.createCollection(ctx, o)
	case *migrate.DropTable:
		return e.dropCollection(ctx, o)
	case *migrate.CreateIndex:
		return e.createIndex(ctx, o)
	case *migrate.DropIndex:
		return e.dropIndex(ctx, o)
	default:
		return migrate.Unsupported(driverName, fmt.Sprintf("operation %T", op))
	}
}

// Native returns the MongoDB driver behind a migration's handle, for the work
// no builder covers, such as a data migration. It follows executor wrappers
// the way the renderer does.
func Native(db *migrate.DB) (*mongodriver.MongoDB, error) {
	if db == nil {
		return nil, errors.New("mongomigrate: no migrate.DB")
	}
	e, err := executorOf(db.Executor())
	if err != nil {
		return nil, err
	}
	return e.db, nil
}

// executorOf finds the mongomigrate executor in exec's wrapper chain. One
// built on no driver is refused here, because everything after this point
// would dereference it.
func executorOf(exec migrate.Executor) (*Executor, error) {
	e, ok := migrate.ExecutorAs[*Executor](exec)
	if !ok {
		return nil, fmt.Errorf("mongomigrate: executor %T is not a mongomigrate executor", exec)
	}
	if e.db == nil {
		return nil, errors.New("mongomigrate: executor has no database")
	}
	return e, nil
}

// invalid reports a misused operation. The detail comes first so the message
// reads naturally, and the sentinel is wrapped so callers can match it.
func invalid(detail string) error {
	return fmt.Errorf("mongomigrate: %s: %w", detail, migrate.ErrInvalidOperation)
}

func invalidf(format string, args ...any) error {
	return invalid(fmt.Sprintf(format, args...))
}

// hasCode reports whether err carries the server error code.
func hasCode(err error, code int) bool {
	var serverErr mongo.ServerError
	return errors.As(err, &serverErr) && serverErr.HasErrorCode(code)
}

func (e *Executor) createCollection(ctx context.Context, op *migrate.CreateTable) error {
	if op.Table == "" {
		return invalid("create table needs a name")
	}
	switch {
	case op.Model != nil && (len(op.Columns) > 0 || len(op.Constraints) > 0):
		return invalidf("create table %q mixes a model with columns or constraints", op.Table)
	case op.Model == nil && len(op.Columns) == 0:
		return invalidf("create table %q has no columns and no model", op.Table)
	}

	opts, err := collectionOptions(op)
	if err != nil {
		return err
	}

	if op.Model != nil {
		return e.createFromModel(ctx, op, opts)
	}
	return e.createFromColumns(ctx, op, opts)
}

// collectionOptions reads the options a migration passed to With. They are
// the same CollectionOption values CreateCollection takes, minus the two that
// would give a collection a second name or a second IfNotExists: the
// collection is always the name given to NewCreateTable, and IfNotExists is
// always the builder's. Accepting either silently would let the two disagree.
func collectionOptions(op *migrate.CreateTable) (collectionOpts, error) {
	// The defaults carry the validation level and action CreateCollection uses.
	// IfNotExists is the one default that is not ours to keep: helpers.go turns
	// it on, and a migration that didn't ask for it must fail on an existing
	// collection.
	o := defaultCollectionOpts()
	o.ifNotExists = op.IfNotExists

	for i, raw := range op.Options {
		opt, ok := raw.(CollectionOption)
		if !ok || opt == nil {
			return collectionOpts{}, invalidf("create table %q: option %d (%T) is not a mongomigrate.CollectionOption", op.Table, i+1, raw)
		}
		opt(&o)
	}

	if o.collection != "" {
		return collectionOpts{}, invalidf("create table %q: name the collection in NewCreateTable, not WithCollection", op.Table)
	}
	if o.ifNotExists != op.IfNotExists {
		return collectionOpts{}, invalidf("create table %q: use IfNotExists on the builder, not WithIfNotExists", op.Table)
	}
	return o, nil
}

// createFromModel leaves the validator to the query that already knows how to
// read a model's tags. That query names the collection after the model and
// nothing else, so the name the migration wrote is checked against it first.
// Without the check a mismatch would create a collection other than the one
// asked for and still report success.
func (e *Executor) createFromModel(ctx context.Context, op *migrate.CreateTable, opts collectionOpts) error {
	q := e.db.NewCreateCollection(op.Model)
	// BuildSchema is how the query reports a model it couldn't read.
	if _, err := q.BuildSchema(); err != nil {
		return fmt.Errorf("mongomigrate: create table %q from model: %w: %w", op.Table, err, migrate.ErrInvalidOperation)
	}
	if got := q.GetCollection(); got != op.Table {
		return invalidf("create table %q: model %T maps to collection %q", op.Table, op.Model, got)
	}

	skip, err := e.skipExisting(ctx, op)
	if err != nil || skip {
		return err
	}

	if opts.validationLevel != "" {
		q = q.ValidationLevel(opts.validationLevel)
	}
	if opts.validationAction != "" {
		q = q.ValidationAction(opts.validationAction)
	}
	if opts.additionalProps != nil {
		q = q.AdditionalProperties(*opts.additionalProps)
	}

	if execErr := q.Exec(ctx); execErr != nil {
		return fmt.Errorf("mongomigrate: create table %s: %w", op.Table, execErr)
	}
	return nil
}

func (e *Executor) createFromColumns(ctx context.Context, op *migrate.CreateTable, opts collectionOpts) error {
	schema, err := jsonSchema(op, opts.additionalProps)
	if err != nil {
		return err
	}

	skip, err := e.skipExisting(ctx, op)
	if err != nil || skip {
		return err
	}

	create := options.CreateCollection().SetValidator(bson.M{"$jsonSchema": schema})
	if opts.validationLevel != "" {
		create.SetValidationLevel(opts.validationLevel)
	}
	if opts.validationAction != "" {
		create.SetValidationAction(opts.validationAction)
	}

	if createErr := e.db.Database().CreateCollection(ctx, op.Table, create); createErr != nil {
		return fmt.Errorf("mongomigrate: create table %s: %w", op.Table, createErr)
	}
	return nil
}

// skipExisting settles what an existing collection means for op. Mongo 7 lets
// a create succeed when the collection is already there with the same options,
// so leaving the failure to the server would make a CreateTable without
// IfNotExists quietly pass or fail depending on whether the validator
// happened to match. Checking first makes it fail every time.
func (e *Executor) skipExisting(ctx context.Context, op *migrate.CreateTable) (bool, error) {
	names, err := e.db.Database().ListCollectionNames(ctx, bson.M{"name": op.Table})
	if err != nil {
		return false, fmt.Errorf("mongomigrate: create table %s: list collections: %w", op.Table, err)
	}
	if len(names) == 0 {
		return false, nil
	}
	if op.IfNotExists {
		return true, nil
	}
	return false, fmt.Errorf("mongomigrate: create table %s: collection already exists", op.Table)
}

// jsonSchema builds the $jsonSchema validator for a table declared with
// columns. A column without NotNull also accepts null, which is what
// jsonschema.go does for a nullable pointer: a Go nil encodes as BSON null,
// and a validator that refused it would reject documents the models write.
func jsonSchema(op *migrate.CreateTable, additionalProps *bool) (bson.M, error) {
	properties := make(bson.M, len(op.Columns))
	required := make([]string, 0, len(op.Columns))

	for i := range op.Columns {
		c := &op.Columns[i]
		if c.Name == "" {
			return nil, invalidf("create table %q column %d has no name", op.Table, i+1)
		}
		if _, dup := properties[c.Name]; dup {
			return nil, invalidf("create table %q declares column %q twice", op.Table, c.Name)
		}
		if err := checkColumn(c); err != nil {
			return nil, err
		}

		prop, err := propertySchema(c.Type)
		if err != nil {
			return nil, invalidf("column %q: %v", c.Name, err)
		}
		if c.NotNull {
			required = append(required, c.Name)
		} else {
			prop["bsonType"] = bson.A{prop["bsonType"], "null"}
		}
		properties[c.Name] = prop
	}

	if len(op.Constraints) > 0 {
		return nil, migrate.Unsupported(driverName, constraintFeature(op.Constraints[0].Kind))
	}

	schema := bson.M{
		"bsonType":   "object",
		"properties": properties,
	}
	// An empty required list is itself a validator error, so leave it out.
	if len(required) > 0 {
		schema["required"] = required
	}
	if additionalProps != nil {
		schema["additionalProperties"] = *additionalProps
	}
	return schema, nil
}

// propertySchema maps a portable type to the schema of one property. A raw
// type is the bsonType as written, so Type("objectId") works.
func propertySchema(t migrate.ColumnType) (bson.M, error) {
	if name, ok := bsonTypes[t.Kind]; ok {
		return bson.M{"bsonType": name}, nil
	}

	switch t.Kind {
	case migrate.KindArray:
		if t.Elem == nil {
			return nil, errors.New("array type has no element type")
		}
		items, err := propertySchema(*t.Elem)
		if err != nil {
			return nil, err
		}
		return bson.M{"bsonType": "array", "items": items}, nil
	case migrate.KindRaw:
		if t.Raw == "" {
			return nil, errors.New("raw type is empty")
		}
		return bson.M{"bsonType": t.Raw}, nil
	default:
		return nil, fmt.Errorf("column type is not set (kind %d)", t.Kind)
	}
}

// checkColumn rejects what a document has no counterpart for. Each one names
// the column so a long CREATE TABLE points at the line to change.
func checkColumn(c *migrate.Column) error {
	switch {
	case c.Default != nil:
		return migrate.Unsupported(driverName, fmt.Sprintf("defaults on column %q", c.Name))
	case c.PrimaryKey:
		return migrate.Unsupported(driverName, fmt.Sprintf("primary keys on column %q", c.Name))
	case c.Unique:
		return migrate.Unsupported(driverName, fmt.Sprintf("unique constraints on column %q (create a unique index instead)", c.Name))
	case c.Check != "":
		return migrate.Unsupported(driverName, fmt.Sprintf("check constraints on column %q", c.Name))
	case c.References != nil:
		return migrate.Unsupported(driverName, fmt.Sprintf("foreign keys on column %q", c.Name))
	case c.Identity:
		return migrate.Unsupported(driverName, fmt.Sprintf("identity column %q", c.Name))
	case c.Generated != "":
		return migrate.Unsupported(driverName, fmt.Sprintf("generated column %q", c.Name))
	}
	return nil
}

func constraintFeature(kind migrate.ConstraintKind) string {
	switch kind {
	case migrate.PrimaryKeyConstraint:
		return "primary key constraints"
	case migrate.UniqueConstraint:
		return "unique constraints (create a unique index instead)"
	case migrate.CheckConstraint:
		return "check constraints"
	case migrate.ForeignKeyConstraint:
		return "foreign key constraints"
	default:
		return fmt.Sprintf("table constraints (kind %d)", kind)
	}
}

func (e *Executor) dropCollection(ctx context.Context, op *migrate.DropTable) error {
	if op.Cascade {
		return migrate.Unsupported(driverName, "CASCADE on drop table")
	}
	if op.Table == "" {
		return invalid("drop table needs a name")
	}

	// Dropping a collection that isn't there succeeds on Mongo, so IfExists
	// has nothing left to guard and there is no error to tell apart.
	if err := e.db.Collection(op.Table).Drop(ctx); err != nil {
		return fmt.Errorf("mongomigrate: drop table %s: %w", op.Table, err)
	}
	return nil
}

func (e *Executor) createIndex(ctx context.Context, op *migrate.CreateIndex) error {
	model, err := indexModel(op)
	if err != nil {
		return err
	}

	// Mongo already treats creating an identical index as a no-op, but one
	// with the same name and another definition is an error. IfNotExists
	// skips by name alone, as it does on Postgres.
	if op.IfNotExists {
		exists, existsErr := e.indexExists(ctx, op.Table, op.Name)
		if existsErr != nil {
			return fmt.Errorf("mongomigrate: create index %s on %s: %w", op.Name, op.Table, existsErr)
		}
		if exists {
			return nil
		}
	}

	if _, createErr := e.db.Collection(op.Table).Indexes().CreateOne(ctx, model); createErr != nil {
		return fmt.Errorf("mongomigrate: create index %s on %s: %w", op.Name, op.Table, createErr)
	}
	return nil
}

func (e *Executor) indexExists(ctx context.Context, collection, name string) (bool, error) {
	// A collection that doesn't exist yet lists no indexes rather than failing.
	specs, err := e.db.Collection(collection).Indexes().ListSpecifications(ctx)
	if err != nil {
		return false, err
	}
	for _, spec := range specs {
		if spec.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// indexModel turns a CreateIndex into the model createIndexes takes. It needs
// no connection.
func indexModel(op *migrate.CreateIndex) (mongo.IndexModel, error) {
	switch {
	case op.Where != "":
		return mongo.IndexModel{}, migrate.Unsupported(driverName, "Where predicates (use Filter)")
	case op.Using != "":
		return mongo.IndexModel{}, migrate.Unsupported(driverName, "index access methods")
	case len(op.Include) > 0:
		return mongo.IndexModel{}, migrate.Unsupported(driverName, "included columns")
	case op.Concurrently:
		return mongo.IndexModel{}, migrate.Unsupported(driverName, "concurrent index builds")
	}
	for _, c := range op.Columns {
		if c.Expr != "" {
			return mongo.IndexModel{}, migrate.Unsupported(driverName, "expression index keys")
		}
	}

	switch {
	case op.Name == "":
		return mongo.IndexModel{}, invalid("create index needs a name")
	case op.Table == "":
		return mongo.IndexModel{}, invalidf("create index %q needs a collection", op.Name)
	case len(op.Columns) == 0:
		return mongo.IndexModel{}, invalidf("create index %q has no columns", op.Name)
	case len(op.Options) > 0:
		return mongo.IndexModel{}, invalidf("create index %q takes no index options", op.Name)
	}

	// An ordered document: the order of the keys is the order of the index.
	keys := make(bson.D, 0, len(op.Columns))
	for i, c := range op.Columns {
		if c.Name == "" {
			return mongo.IndexModel{}, invalidf("create index %q key %d has no column name", op.Name, i+1)
		}
		direction := ascending
		if c.Desc {
			direction = descending
		}
		keys = append(keys, bson.E{Key: c.Name, Value: direction})
	}

	opts := options.Index().SetName(op.Name)
	if op.Unique {
		opts.SetUnique(true)
	}
	if op.Filter != nil {
		opts.SetPartialFilterExpression(op.Filter)
	}
	return mongo.IndexModel{Keys: keys, Options: opts}, nil
}

func (e *Executor) dropIndex(ctx context.Context, op *migrate.DropIndex) error {
	switch {
	case op.Cascade:
		return migrate.Unsupported(driverName, "CASCADE on drop index")
	case op.Concurrently:
		return migrate.Unsupported(driverName, "concurrent index drops")
	case op.Name == "":
		return invalid("drop index needs a name")
	case op.Table == "":
		return invalidf("drop index %q needs On with a collection", op.Name)
	}

	err := e.db.Collection(op.Table).Indexes().DropOne(ctx, op.Name)
	if err != nil {
		// A collection that isn't there holds no index of that name either.
		if op.IfExists && (hasCode(err, codeIndexNotFound) || hasCode(err, codeNamespaceNotFound)) {
			return nil
		}
		return fmt.Errorf("mongomigrate: drop index %s on %s: %w", op.Name, op.Table, err)
	}
	return nil
}
