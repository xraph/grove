package mongomigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"

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
// table here, a field is a column and an index is an index. Anything Mongo has
// no equivalent for comes back as migrate.ErrSchemaUnsupported, so a migration
// written with SQL habits fails where it is written and not halfway through a
// run.
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
	case *migrate.AlterTable:
		return e.alterCollection(ctx, o)
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
		return fmt.Errorf("mongomigrate: create table %q: %w", op.Table, execErr)
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
		return fmt.Errorf("mongomigrate: create table %q: %w", op.Table, createErr)
	}
	return nil
}

// skipExisting settles what an existing collection means for op. Mongo 7 lets
// a create succeed when the collection is already there with the same options,
// so leaving the failure to the server would make a CreateTable without
// IfNotExists quietly pass or fail depending on whether the validator
// happened to match. Checking first makes it fail every time.
func (e *Executor) skipExisting(ctx context.Context, op *migrate.CreateTable) (bool, error) {
	exists, err := e.collectionExists(ctx, op.Table)
	if err != nil {
		return false, fmt.Errorf("mongomigrate: create table %q: %w", op.Table, err)
	}
	if !exists {
		return false, nil
	}
	if op.IfNotExists {
		return true, nil
	}
	return false, fmt.Errorf("mongomigrate: create table %q: collection already exists", op.Table)
}

// collectionExists asks the server whether the collection is there.
func (e *Executor) collectionExists(ctx context.Context, name string) (bool, error) {
	names, err := e.db.Database().ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		return false, fmt.Errorf("list collections: %w", err)
	}
	return len(names) > 0, nil
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

		prop, err := columnProperty(c)
		if err != nil {
			return nil, err
		}
		if c.NotNull {
			required = append(required, c.Name)
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

// columnProperty builds the schema of one column's property. A column without
// NotNull also accepts null, for the reason jsonSchema gives.
func columnProperty(c *migrate.Column) (bson.M, error) {
	prop, err := propertySchema(c.Type)
	if err != nil {
		return nil, invalidf("column %q: %v", c.Name, err)
	}
	if !c.NotNull {
		prop["bsonType"] = bson.A{prop["bsonType"], "null"}
	}
	return prop, nil
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
	if c.Default != nil {
		return migrate.Unsupported(driverName, fmt.Sprintf("defaults on column %q", c.Name))
	}
	return checkColumnFeatures(c)
}

// checkColumnFeatures is checkColumn without the default, for AddColumn, which
// can honour a literal one.
func checkColumnFeatures(c *migrate.Column) error {
	switch {
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

	// Mongo 7 drops a missing collection without complaint, so the check is
	// ours: without IfExists a collection that isn't there is an error, as
	// DROP TABLE is on Postgres.
	exists, err := e.collectionExists(ctx, op.Table)
	if err != nil {
		return fmt.Errorf("mongomigrate: drop table %q: %w", op.Table, err)
	}
	if !exists {
		if op.IfExists {
			return nil
		}
		return fmt.Errorf("mongomigrate: drop table %q: collection does not exist", op.Table)
	}

	if dropErr := e.db.Collection(op.Table).Drop(ctx); dropErr != nil {
		return fmt.Errorf("mongomigrate: drop table %q: %w", op.Table, dropErr)
	}
	return nil
}

func (e *Executor) createIndex(ctx context.Context, op *migrate.CreateIndex) error {
	model, err := indexModel(op)
	if err != nil {
		return err
	}

	// createIndexes would make the collection for an index on one that isn't
	// there, with no validator, so a typo in On would succeed while the real
	// collection never got its index. Postgres refuses an index on a missing
	// table, and so does this.
	collExists, err := e.collectionExists(ctx, op.Table)
	if err != nil {
		return fmt.Errorf("mongomigrate: create index %q on %q: %w", op.Name, op.Table, err)
	}
	if !collExists {
		return fmt.Errorf("mongomigrate: create index %q on %q: collection does not exist", op.Name, op.Table)
	}

	// Mongo treats creating an identical index as a no-op, so the server
	// can't be left to fail it. A same-name index counts whatever its
	// definition, as it does on Postgres.
	idxExists, err := e.indexExists(ctx, op.Table, op.Name)
	if err != nil {
		return fmt.Errorf("mongomigrate: create index %q on %q: %w", op.Name, op.Table, err)
	}
	if idxExists {
		if op.IfNotExists {
			return nil
		}
		return fmt.Errorf("mongomigrate: create index %q on %q: index already exists", op.Name, op.Table)
	}

	if _, createErr := e.db.Collection(op.Table).Indexes().CreateOne(ctx, model); createErr != nil {
		return fmt.Errorf("mongomigrate: create index %q on %q: %w", op.Name, op.Table, createErr)
	}
	return nil
}

func (e *Executor) indexExists(ctx context.Context, collection, name string) (bool, error) {
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
		return fmt.Errorf("mongomigrate: drop index %q on %q: %w", op.Name, op.Table, err)
	}
	return nil
}

// alterCollection carries out the field changes of an AlterTable: a column is
// a field, so adding, dropping and renaming one edit the documents and, when
// the collection has a $jsonSchema validator, the validator too. Every other
// action is refused.
func (e *Executor) alterCollection(ctx context.Context, op *migrate.AlterTable) error {
	// Settled before the server is asked anything, so an action Mongo can't do
	// stops the migration before the actions ahead of it have changed data.
	if err := checkAlter(op); err != nil {
		return err
	}

	// A write to a missing collection would create it with no validator, so a
	// typo in the name would pass while the real collection stayed as it was.
	exists, err := e.collectionExists(ctx, op.Table)
	if err != nil {
		return fmt.Errorf("mongomigrate: alter table %q: %w", op.Table, err)
	}
	if !exists {
		return fmt.Errorf("mongomigrate: alter table %q: collection does not exist", op.Table)
	}

	// A validator that isn't a top-level $jsonSchema can't be edited here, and
	// writing around it would leave documents it rejects. Settled before the
	// first write so nothing is half done.
	validator, err := e.loadValidator(ctx, op.Table)
	if err != nil {
		return fmt.Errorf("mongomigrate: alter table %q: %w", op.Table, err)
	}
	if len(validator) > 0 && jsonSchemaOf(validator) == nil {
		return invalidf("alter table %q: validator has no top-level $jsonSchema; alter it by hand", op.Table)
	}

	// Mongo can't roll an earlier action back, so a failure stops here with the
	// actions before it applied. Each action orders its own steps to keep that
	// as tidy as it can: the validator is written last because it is what a
	// rerun reads to decide whether the action already happened.
	for i := range op.Actions {
		a := &op.Actions[i]
		if actionErr := e.alterField(ctx, op.Table, a); actionErr != nil {
			return fmt.Errorf("mongomigrate: alter table %q: action %d (%s): %w", op.Table, i+1, describeAction(a), actionErr)
		}
	}
	return nil
}

// checkAlter finds everything wrong with an AlterTable that needs no
// connection to see.
func checkAlter(op *migrate.AlterTable) error {
	switch {
	case op.Table == "":
		return invalid("alter table needs a name")
	case len(op.Actions) == 0:
		return invalidf("alter table %q has no actions", op.Table)
	}
	for i := range op.Actions {
		if err := checkAlterAction(&op.Actions[i]); err != nil {
			return err
		}
	}
	return nil
}

func checkAlterAction(a *migrate.AlterAction) error {
	switch a.Kind {
	case migrate.AlterAddColumn:
		return checkAddColumn(&a.Column)
	case migrate.AlterDropColumn:
		if a.Cascade {
			return migrate.Unsupported(driverName, "CASCADE on drop column")
		}
		return checkFieldName(a.Name)
	case migrate.AlterRenameColumn:
		if err := checkFieldName(a.Name); err != nil {
			return err
		}
		if err := checkFieldName(a.NewName); err != nil {
			return err
		}
		if a.Name == a.NewName {
			return invalidf("rename column %q to itself", a.Name)
		}
		return nil
	case migrate.AlterSetNotNull:
		return migrate.Unsupported(driverName, fmt.Sprintf("set not null on column %q", a.Name))
	case migrate.AlterDropNotNull:
		return migrate.Unsupported(driverName, fmt.Sprintf("drop not null on column %q", a.Name))
	case migrate.AlterSetDefault:
		return migrate.Unsupported(driverName, fmt.Sprintf("set default on column %q", a.Name))
	case migrate.AlterDropDefault:
		return migrate.Unsupported(driverName, fmt.Sprintf("drop default on column %q", a.Name))
	case migrate.AlterSetType:
		return migrate.Unsupported(driverName, fmt.Sprintf("set type on column %q", a.Name))
	case migrate.AlterAddConstraint:
		return migrate.Unsupported(driverName, constraintFeature(a.Constraint.Kind))
	case migrate.AlterDropConstraint:
		return migrate.Unsupported(driverName, fmt.Sprintf("drop constraint %q", a.Name))
	case migrate.AlterRenameTo:
		return migrate.Unsupported(driverName, fmt.Sprintf("rename table to %q", a.NewName))
	default:
		return migrate.Unsupported(driverName, fmt.Sprintf("alter action (kind %d)", a.Kind))
	}
}

func checkAddColumn(c *migrate.Column) error {
	if err := checkFieldName(c.Name); err != nil {
		return err
	}
	if c.Default != nil && c.Default.Expr != "" {
		return migrate.Unsupported(driverName, fmt.Sprintf("default expressions on column %q", c.Name))
	}
	if err := checkColumnFeatures(c); err != nil {
		return err
	}
	// Only to learn whether the type has a property schema and the default fits.
	if _, err := columnProperty(c); err != nil {
		return err
	}
	_, err := columnDefault(c)
	return err
}

// checkFieldName keeps a name the validator and the data would read
// differently out of an alter. The validator takes a property name literally,
// while $set, $unset and $rename read a dot as a path and a leading dollar sign
// as an operator.
func checkFieldName(name string) error {
	switch {
	case name == "":
		return invalid("column name is empty")
	case strings.Contains(name, ".") || strings.HasPrefix(name, "$"):
		return invalidf("column name %q must not contain %q or start with %q", name, ".", "$")
	}
	return nil
}

// describeAction names an action for the error that reports it failing.
func describeAction(a *migrate.AlterAction) string {
	switch a.Kind {
	case migrate.AlterAddColumn:
		return fmt.Sprintf("add column %q", a.Column.Name)
	case migrate.AlterDropColumn:
		return fmt.Sprintf("drop column %q", a.Name)
	case migrate.AlterRenameColumn:
		return fmt.Sprintf("rename column %q to %q", a.Name, a.NewName)
	default:
		return fmt.Sprintf("action kind %d", a.Kind)
	}
}

func (e *Executor) alterField(ctx context.Context, table string, a *migrate.AlterAction) error {
	switch a.Kind {
	case migrate.AlterAddColumn:
		return e.addField(ctx, table, a)
	case migrate.AlterDropColumn:
		return e.dropField(ctx, table, a)
	case migrate.AlterRenameColumn:
		return e.renameField(ctx, table, a)
	default:
		// checkAlter has turned every other kind away already.
		return migrate.Unsupported(driverName, describeAction(a))
	}
}

// addField backfills the documents, then lists the field in the validator. The
// order matters when the field is required: the validator would otherwise
// refuse the next write to every document that lacks it. Everything that can
// fail on what the documents hold is checked before the first write, so a
// refused action changes nothing.
//
// Where the collection has no validator there is no schema to consult or to
// extend, so the existence rules don't apply and none is created. The checks
// on what the documents hold still do, because a validator added later would
// find the same documents.
func (e *Executor) addField(ctx context.Context, table string, a *migrate.AlterAction) error {
	c := &a.Column

	validator, err := e.loadValidator(ctx, table)
	if err != nil {
		return err
	}
	schema := jsonSchemaOf(validator)
	if schema != nil && hasProperty(schema, c.Name) {
		if a.IfNotExists {
			return nil
		}
		return fmt.Errorf("field %q already exists in the validator", c.Name)
	}

	prop, err := columnProperty(c)
	if err != nil {
		return err
	}
	def, err := columnDefault(c)
	if err != nil {
		return err
	}

	// A required property doesn't accept null, so for one a null counts as
	// missing. An optional one accepts it, and a null there is a real value.
	missing := missingValue(c.Name, c.NotNull)
	if def == nil && c.NotNull {
		n, countErr := e.countDocuments(ctx, table, missing)
		if countErr != nil {
			return countErr
		}
		if n > 0 {
			return fmt.Errorf("cannot add required field %q to %q: %d documents lack it", c.Name, table, n)
		}
	}
	if checkErr := e.checkFieldValues(ctx, table, c, prop); checkErr != nil {
		return checkErr
	}

	if def != nil {
		update := bson.M{"$set": bson.M{c.Name: def}}
		if setErr := e.updateDocuments(ctx, table, missing, update, schema != nil); setErr != nil {
			return setErr
		}
	}

	if schema == nil {
		return nil
	}
	return e.setSchema(ctx, table, validator, addProperty(schema, c.Name, prop, c.NotNull))
}

// checkFieldValues fails when a document already holds a value for the new
// field that its property would reject. The backfill skips validation and
// collMod doesn't revalidate, so without this a document of the wrong type
// would pass the migration and fail its next ordinary write.
func (e *Executor) checkFieldValues(ctx context.Context, table string, c *migrate.Column, prop bson.M) error {
	present := bson.M{"$exists": true}
	if c.NotNull {
		// A null is either about to be filled by the default or has already been
		// counted as a missing value.
		present["$ne"] = nil
	}
	filter := bson.M{
		c.Name: present,
		"$nor": bson.A{bson.M{"$jsonSchema": bson.M{"properties": bson.M{c.Name: prop}}}},
	}

	n, err := e.countDocuments(ctx, table, filter)
	if err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("cannot add field %q to %q: %d documents hold a value that is not bsonType %s",
			c.Name, table, n, quotedList(bsonTypeNames(prop)))
	}
	return nil
}

func quotedList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, " or ")
}

// bsonTypeNames lists the types a property accepts. A nullable one names two.
func bsonTypeNames(prop bson.M) []string {
	switch b := prop["bsonType"].(type) {
	case string:
		return []string{b}
	case bson.A:
		return stringsIn(b)
	default:
		return nil
	}
}

// bsonAliases names the BSON types the way $jsonSchema's bsonType does.
var bsonAliases = map[bson.Type]string{
	bson.TypeDouble:           "double",
	bson.TypeString:           "string",
	bson.TypeEmbeddedDocument: "object",
	bson.TypeArray:            "array",
	bson.TypeBinary:           "binData",
	bson.TypeUndefined:        "undefined",
	bson.TypeObjectID:         "objectId",
	bson.TypeBoolean:          "bool",
	bson.TypeDateTime:         "date",
	bson.TypeNull:             "null",
	bson.TypeRegex:            "regex",
	bson.TypeDBPointer:        "dbPointer",
	bson.TypeJavaScript:       "javascript",
	bson.TypeSymbol:           "symbol",
	bson.TypeCodeWithScope:    "javascriptWithScope",
	bson.TypeInt32:            "int",
	bson.TypeTimestamp:        "timestamp",
	bson.TypeInt64:            "long",
	bson.TypeDecimal128:       "decimal",
	bson.TypeMinKey:           "minKey",
	bson.TypeMaxKey:           "maxKey",
}

// maxExactFloatInt is the largest integer a double holds without rounding.
const maxExactFloatInt = 1 << 53

// columnDefault returns the literal default of an added column as the value to
// write, or nil when there is nothing to backfill. The backfill skips
// validation and collMod doesn't revalidate, so a default of the wrong type
// would be written and then fail every later update to the document. The value
// is therefore made to fit the property or refused here. A nil literal is
// DEFAULT NULL, which is the same as having no default.
func columnDefault(c *migrate.Column) (any, error) {
	if c.Default == nil || c.Default.Value == nil {
		return nil, nil
	}
	value, err := coerceDefault(c.Type, c.Default.Value)
	if err != nil {
		return nil, invalidf("column %q: %v", c.Name, err)
	}
	return value, nil
}

// coerceDefault fits v to the type t. A Go integer takes the width the
// property names, because the driver would encode a Go int as an int32
// whatever the column holds, and a Go integer or float becomes a double. These
// are the only conversions, so a float is never turned into an integer. Any
// other value has to encode as the property's type already. A raw type is the
// driver's own name, so it gets no conversion at all.
func coerceDefault(t migrate.ColumnType, v any) (any, error) {
	prop, err := propertySchema(t)
	if err != nil {
		return nil, err
	}
	var want string
	if names := bsonTypeNames(prop); len(names) > 0 {
		want = names[0]
	}

	if v == nil {
		return nil, defaultMismatch(v, want)
	}
	if t.Kind != migrate.KindRaw {
		if n, handled, numErr := coerceNumber(v, want); handled {
			return n, numErr
		}
	}

	encoded, _, err := bson.MarshalValue(v)
	if err != nil {
		return nil, fmt.Errorf("default %v (%T) cannot be encoded: %w", v, v, err)
	}
	if !acceptsType(want, encoded) {
		return nil, defaultMismatch(v, want)
	}

	if want == "array" && t.Kind == migrate.KindArray && t.Elem != nil {
		return coerceElements(*t.Elem, v)
	}
	return v, nil
}

func defaultMismatch(v any, want string) error {
	return fmt.Errorf("default %v (%T) does not match bsonType %q", v, v, want)
}

// acceptsType reports whether a value that encodes as got fits want. The
// "number" alias is the one that covers more than one BSON type.
func acceptsType(want string, got bson.Type) bool {
	if want == "number" {
		return got == bson.TypeDouble || got == bson.TypeInt32 || got == bson.TypeInt64 || got == bson.TypeDecimal128
	}
	return bsonAliases[got] == want
}

// coerceNumber converts a Go integer to the width of want, and a Go integer or
// float to a double. handled is false for anything else, which then has to
// match want as it is.
func coerceNumber(v any, want string) (value any, handled bool, err error) {
	rv := reflect.ValueOf(v)

	var n int64
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n = rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := rv.Uint()
		if u > math.MaxInt64 {
			// Too big for any of the three widths, so there is nothing to convert.
			if wantsNumber(want) {
				return nil, true, outOfRange(v, want)
			}
			return nil, false, nil
		}
		n = int64(u)
	case reflect.Float32, reflect.Float64:
		if want == "double" {
			return rv.Float(), true, nil
		}
		return nil, false, nil
	default:
		return nil, false, nil
	}

	switch want {
	case "int":
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, true, outOfRange(v, want)
		}
		return int32(n), true, nil
	case "long":
		return n, true, nil
	case "double":
		if n < -maxExactFloatInt || n > maxExactFloatInt {
			return nil, true, outOfRange(v, want)
		}
		return float64(n), true, nil
	default:
		return nil, false, nil
	}
}

func wantsNumber(want string) bool {
	return want == "int" || want == "long" || want == "double"
}

func outOfRange(v any, want string) error {
	return fmt.Errorf("default %v (%T) is out of range for bsonType %q", v, v, want)
}

// coerceElements fits each element of a slice default to the array's element
// type, so a []int on an array of bigint is written as longs.
func coerceElements(elem migrate.ColumnType, v any) (any, error) {
	rv := reflect.ValueOf(v)
	isList := rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array
	// A byte slice only encodes as an array when it is a bson.RawArray, which
	// already holds encoded elements.
	if !isList || rv.Type().Elem().Kind() == reflect.Uint8 {
		return v, nil
	}

	out := make(bson.A, 0, rv.Len())
	for i := range rv.Len() {
		item, err := coerceDefault(elem, rv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

// missingValue matches the documents that hold no value for field.
func missingValue(field string, nullCounts bool) bson.M {
	if nullCounts {
		// Matching null in a query matches a missing field as well.
		return bson.M{field: nil}
	}
	return bson.M{field: bson.M{"$exists": false}}
}

func (e *Executor) dropField(ctx context.Context, table string, a *migrate.AlterAction) error {
	validator, err := e.loadValidator(ctx, table)
	if err != nil {
		return err
	}
	schema := jsonSchemaOf(validator)
	if schema != nil && !hasProperty(schema, a.Name) {
		if a.IfExists {
			return nil
		}
		return fmt.Errorf("field %q does not exist in the validator", a.Name)
	}

	present := bson.M{a.Name: bson.M{"$exists": true}}
	unset := bson.M{"$unset": bson.M{a.Name: ""}}
	if unsetErr := e.updateDocuments(ctx, table, present, unset, schema != nil); unsetErr != nil {
		return unsetErr
	}

	if schema == nil {
		return nil
	}
	return e.setSchema(ctx, table, validator, dropProperty(schema, a.Name))
}

func (e *Executor) renameField(ctx context.Context, table string, a *migrate.AlterAction) error {
	validator, err := e.loadValidator(ctx, table)
	if err != nil {
		return err
	}
	schema := jsonSchemaOf(validator)
	if schema != nil {
		switch {
		case !hasProperty(schema, a.Name):
			return fmt.Errorf("field %q does not exist in the validator", a.Name)
		case hasProperty(schema, a.NewName):
			return fmt.Errorf("field %q already exists in the validator", a.NewName)
		}
	}

	// $rename onto a field a document already holds throws that value away,
	// which Postgres refuses to do by refusing the rename. The validator can't
	// tell us about the documents, so ask them.
	both := bson.M{a.Name: bson.M{"$exists": true}, a.NewName: bson.M{"$exists": true}}
	clashes, err := e.countDocuments(ctx, table, both)
	if err != nil {
		return err
	}
	if clashes > 0 {
		return fmt.Errorf("cannot rename field %q to %q: %d documents already have both", a.Name, a.NewName, clashes)
	}

	present := bson.M{a.Name: bson.M{"$exists": true}}
	update := bson.M{"$rename": bson.M{a.Name: a.NewName}}
	if renameErr := e.updateDocuments(ctx, table, present, update, schema != nil); renameErr != nil {
		return renameErr
	}

	if schema == nil {
		return nil
	}
	return e.setSchema(ctx, table, validator, renameProperty(schema, a.Name, a.NewName))
}

// updateDocuments writes to every document that matches filter.
// editsValidator says whether the action goes on to edit a $jsonSchema
// validator.
func (e *Executor) updateDocuments(ctx context.Context, table string, filter, update bson.M, editsValidator bool) error {
	if _, err := e.db.Collection(table).UpdateMany(ctx, filter, update, updateOptions(editsValidator)); err != nil {
		return fmt.Errorf("update documents: %w", err)
	}
	return nil
}

// updateOptions skips validation only when the action edits the validator. The
// data and the validator change in two steps, and the first would trip over
// the validator the second is about to retire: a field being dropped is still
// required, and one being added is not yet allowed under additionalProperties
// false. With no validator to edit there is nothing to retire, and bypassing
// would ask for a privilege readWrite doesn't have.
func updateOptions(editsValidator bool) *options.UpdateManyOptionsBuilder {
	opts := options.UpdateMany()
	if editsValidator {
		opts.SetBypassDocumentValidation(true)
	}
	return opts
}

func (e *Executor) countDocuments(ctx context.Context, table string, filter bson.M) (int64, error) {
	n, err := e.db.Collection(table).CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("count documents: %w", err)
	}
	return n, nil
}

// loadValidator reads the collection's validator document, or nil when it has
// none.
func (e *Executor) loadValidator(ctx context.Context, table string) (bson.M, error) {
	specs, err := e.db.Database().ListCollectionSpecifications(ctx, bson.M{"name": table})
	if err != nil {
		return nil, fmt.Errorf("read validator: %w", err)
	}
	if len(specs) == 0 {
		return nil, errors.New("read validator: collection does not exist")
	}
	return decodeValidator(specs[0].Options)
}

// decodeValidator pulls the validator out of a collection's options. Nested
// documents come back as bson.M, so the edits below find properties without
// caring that the driver's default for them is bson.D.
func decodeValidator(opts bson.Raw) (bson.M, error) {
	if len(opts) == 0 {
		return nil, nil
	}
	var spec struct {
		Validator bson.Raw `bson:"validator"`
	}
	if err := bson.Unmarshal(opts, &spec); err != nil {
		return nil, fmt.Errorf("read validator: %w", err)
	}
	if len(spec.Validator) == 0 {
		return nil, nil
	}

	dec := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(spec.Validator)))
	dec.DefaultDocumentM()
	var validator bson.M
	if err := dec.Decode(&validator); err != nil {
		return nil, fmt.Errorf("read validator: %w", err)
	}
	return validator, nil
}

// jsonSchemaOf returns the $jsonSchema inside a validator, or nil when the
// validator has none. A validator written as a plain query is left alone.
func jsonSchemaOf(validator bson.M) bson.M {
	schema, ok := validator["$jsonSchema"].(bson.M)
	if !ok {
		return nil
	}
	return schema
}

// setSchema replaces the validator, keeping whatever else it held next to the
// $jsonSchema. collMod leaves the validation level and action as they were.
func (e *Executor) setSchema(ctx context.Context, table string, validator, schema bson.M) error {
	next := make(bson.M, len(validator)+1)
	maps.Copy(next, validator)
	next["$jsonSchema"] = schema

	cmd := bson.D{{Key: "collMod", Value: table}, {Key: "validator", Value: next}}
	if err := e.db.Database().RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("update validator: %w", err)
	}
	return nil
}

// The three edits below take a schema as read from the server, or as
// jsonSchema built it, and return a changed copy. They never touch their
// input, and need no connection.

// addProperty lists name in the schema, and in required when it is. It never
// takes a name out of required, so an optional column added next to a stray
// requirement leaves the requirement alone.
func addProperty(schema bson.M, name string, prop bson.M, required bool) bson.M {
	out := cloneSchema(schema)

	props := propertiesOf(schema)
	props[name] = prop
	out["properties"] = props

	if required {
		names := requiredOf(schema)
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
		setRequired(out, names)
	}
	return out
}

// dropProperty removes name from the schema's properties and required list.
func dropProperty(schema bson.M, name string) bson.M {
	out := cloneSchema(schema)

	if _, ok := schema["properties"]; ok {
		props := propertiesOf(schema)
		delete(props, name)
		out["properties"] = props
	}

	names := slices.DeleteFunc(requiredOf(schema), func(n string) bool { return n == name })
	setRequired(out, names)
	return out
}

// renameProperty moves a property to a new name, keeping its place in the
// required list.
func renameProperty(schema bson.M, from, to string) bson.M {
	out := cloneSchema(schema)

	props := propertiesOf(schema)
	if prop, ok := props[from]; ok {
		delete(props, from)
		props[to] = prop
		out["properties"] = props
	}

	names := requiredOf(schema)
	if i := slices.Index(names, from); i >= 0 {
		if slices.Contains(names, to) {
			names = slices.Delete(names, i, i+1)
		} else {
			names[i] = to
		}
	}
	setRequired(out, names)
	return out
}

// hasProperty reports whether the schema lists name under properties. A name
// that is only in required doesn't count: nothing says what type it has.
func hasProperty(schema bson.M, name string) bool {
	_, listed := propertiesOf(schema)[name]
	return listed
}

// cloneSchema copies the top level of a schema. It is never nil, so a caller
// can always set keys on it.
func cloneSchema(schema bson.M) bson.M {
	out := make(bson.M, len(schema)+2)
	maps.Copy(out, schema)
	return out
}

// propertiesOf returns a copy of the schema's properties that is safe to edit.
func propertiesOf(schema bson.M) bson.M {
	out := make(bson.M, 1)
	// A schema with no properties reads the same as one with an empty list.
	if props, ok := schema["properties"].(bson.M); ok {
		maps.Copy(out, props)
	}
	return out
}

// requiredOf returns a copy of the schema's required names. The server hands
// them back as a bson.A and jsonSchema builds a []string, so both are read.
func requiredOf(schema bson.M) []string {
	switch r := schema["required"].(type) {
	case []string:
		return slices.Clone(r)
	case bson.A:
		return stringsIn(r)
	case []any:
		return stringsIn(r)
	default:
		return nil
	}
}

func stringsIn(values []any) []string {
	names := make([]string, 0, len(values))
	for _, v := range values {
		if name, ok := v.(string); ok {
			names = append(names, name)
		}
	}
	return names
}

// setRequired stores the list, or removes it when it is empty: an empty
// required array is itself a validator error.
func setRequired(schema bson.M, names []string) {
	if len(names) == 0 {
		delete(schema, "required")
		return
	}
	schema["required"] = names
}
