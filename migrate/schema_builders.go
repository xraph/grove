package migrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// invalid wraps err in [ErrInvalidOperation].
func invalid(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidOperation, err)
}

func invalidf(format string, args ...any) error {
	return invalid(fmt.Errorf(format, args...))
}

// builderErr holds the first misuse a chain ran into. Every builder method
// returns early once it is set, so a chain never panics and Operation reports
// the mistake that came first, not a knock-on error from a later call.
type builderErr struct {
	err error
}

func (b *builderErr) fail(err error) {
	if b.err == nil {
		b.err = invalid(err)
	}
}

func (b *builderErr) failf(format string, args ...any) {
	b.fail(fmt.Errorf(format, args...))
}

// record keeps err when it is non-nil. It lets a method that ends in a call
// returning an error stay a single statement.
func (b *builderErr) record(err error) {
	if err != nil {
		b.fail(err)
	}
}

// need reports whether the chain is still healthy and value is non-empty,
// recording the misuse when it isn't. what names the argument for the error.
func (b *builderErr) need(what, value string) bool {
	if b.err != nil {
		return false
	}
	if value == "" {
		b.failf("%s is empty", what)
		return false
	}
	return true
}

// ColumnOption adjusts a column as a builder adds it. The builder wraps an
// error from an option in [ErrInvalidOperation] and the column's name, so an
// option returns a plain error.
type ColumnOption func(*Column) error

// NotNull forbids NULL in the column.
func NotNull() ColumnOption {
	return func(c *Column) error {
		c.NotNull = true
		return nil
	}
}

// Default sets a literal default. A nil value means DEFAULT NULL.
func Default(value any) ColumnOption {
	return func(c *Column) error {
		c.Default = &DefaultValue{Value: value}
		return nil
	}
}

// DefaultExpr sets a default from a SQL expression such as now(), spliced in
// as written.
func DefaultExpr(sql string) ColumnOption {
	return func(c *Column) error {
		if sql == "" {
			return errors.New("default expression is empty")
		}
		c.Default = &DefaultValue{Expr: sql}
		return nil
	}
}

// PrimaryKey makes the column the table's primary key. Use the table-level
// PrimaryKey for a composite one.
func PrimaryKey() ColumnOption {
	return func(c *Column) error {
		c.PrimaryKey = true
		return nil
	}
}

// Unique adds a UNIQUE constraint on this column alone.
func Unique() ColumnOption {
	return func(c *Column) error {
		c.Unique = true
		return nil
	}
}

// Check adds a column-level CHECK constraint.
func Check(sql string) ColumnOption {
	return func(c *Column) error {
		if sql == "" {
			return errors.New("check expression is empty")
		}
		if c.Check != "" {
			return errors.New("column already has a check, combine them with AND in one Check")
		}
		c.Check = sql
		return nil
	}
}

// References makes the column a foreign key to table. A column-level
// reference names exactly one column. Use ForeignKey on the table for a
// composite key.
func References(table string, columns ...string) ColumnOption {
	return func(c *Column) error {
		if c.References != nil {
			return errors.New("column already has references")
		}
		if len(columns) != 1 {
			return fmt.Errorf("a column reference names exactly one column, got %d", len(columns))
		}
		ref, err := newReference(table, columns)
		if err != nil {
			return err
		}
		c.References = ref
		return nil
	}
}

// OnDelete sets what happens to this row when the referenced row is deleted.
// It has to come after [References].
func OnDelete(a Action) ColumnOption {
	return func(c *Column) error {
		if c.References == nil {
			return errors.New("on delete needs references before it")
		}
		if err := a.validate(); err != nil {
			return err
		}
		c.References.OnDelete = a
		return nil
	}
}

// Identity makes the column GENERATED ALWAYS AS IDENTITY.
func Identity() ColumnOption {
	return func(c *Column) error {
		c.Identity = true
		return nil
	}
}

// GeneratedAs makes the column a stored generated column computed from sql.
func GeneratedAs(sql string) ColumnOption {
	return func(c *Column) error {
		if sql == "" {
			return errors.New("generated expression is empty")
		}
		c.Generated = sql
		return nil
	}
}

// newColumn builds a column and applies opts in order, so OnDelete has to
// follow References just as it reads.
func newColumn(name string, typ ColumnType, opts []ColumnOption) (Column, error) {
	if name == "" {
		return Column{}, errors.New("column name is empty")
	}
	if err := typ.validate(); err != nil {
		return Column{}, fmt.Errorf("column %q: %w", name, err)
	}
	col := Column{Name: name, Type: typ}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&col); err != nil {
			return Column{}, fmt.Errorf("column %q: %w", name, err)
		}
	}
	return col, nil
}

// CreateTableQuery builds a [CreateTable].
type CreateTableQuery struct {
	builderErr

	db *DB
	op *CreateTable
}

// IfNotExists skips the statement when the table is already there.
func (q *CreateTableQuery) IfNotExists() *CreateTableQuery {
	q.op.IfNotExists = true
	return q
}

// Column adds a column. Columns render in the order you add them, and they
// all come before the table constraints. Declare every column first: a Column
// after PrimaryKey, Unique, Check or ForeignKey is a misuse, because the
// constraint modifiers (Named, References, OnDelete) bind to the last
// constraint and the written order would no longer be the rendered order.
func (q *CreateTableQuery) Column(name string, typ ColumnType, opts ...ColumnOption) *CreateTableQuery {
	if q.err != nil {
		return q
	}
	if len(q.op.Constraints) > 0 {
		q.failf("column %q declared after a table constraint; declare columns before table constraints", name)
		return q
	}
	col, err := newColumn(name, typ, opts)
	if err != nil {
		q.fail(err)
		return q
	}
	q.op.Columns = append(q.op.Columns, col)
	return q
}

// PrimaryKey adds a table-level primary key over cols.
func (q *CreateTableQuery) PrimaryKey(cols ...string) *CreateTableQuery {
	return q.addConstraint(newColumnsConstraint(PrimaryKeyConstraint, "PrimaryKey", cols))
}

// Unique adds a table-level UNIQUE constraint over cols.
func (q *CreateTableQuery) Unique(cols ...string) *CreateTableQuery {
	return q.addConstraint(newColumnsConstraint(UniqueConstraint, "Unique", cols))
}

// Check adds a table-level CHECK constraint.
func (q *CreateTableQuery) Check(expr string) *CreateTableQuery {
	return q.addConstraint(newCheckConstraint("Check", expr))
}

// ForeignKey starts a foreign key over cols. Follow it with References, and
// optionally OnDelete.
func (q *CreateTableQuery) ForeignKey(cols ...string) *CreateTableQuery {
	return q.addConstraint(newColumnsConstraint(ForeignKeyConstraint, "ForeignKey", cols))
}

// References sets the target of the foreign key you just started.
func (q *CreateTableQuery) References(table string, cols ...string) *CreateTableQuery {
	if c := q.last("References"); c != nil {
		q.record(c.setReferences(table, cols))
	}
	return q
}

// OnDelete sets the referential action of the foreign key you just started.
// It has to follow References.
func (q *CreateTableQuery) OnDelete(a Action) *CreateTableQuery {
	if c := q.last("OnDelete"); c != nil {
		q.record(c.setOnDelete(a))
	}
	return q
}

// Named names the constraint you just added. Left unnamed, the driver picks
// a name.
func (q *CreateTableQuery) Named(name string) *CreateTableQuery {
	if c := q.last("Named"); c != nil {
		q.record(c.setName(name))
	}
	return q
}

// Model takes the columns from a grove model instead of Column calls, for
// example Model((*User)(nil)). It can't be combined with columns or
// constraints.
func (q *CreateTableQuery) Model(model any) *CreateTableQuery {
	if q.err != nil {
		return q
	}
	if model == nil {
		q.failf("Model needs a model, got nil")
		return q
	}
	q.op.Model = model
	return q
}

// With passes driver-specific options. A driver returns an error for any
// option it doesn't recognise.
func (q *CreateTableQuery) With(opts ...any) *CreateTableQuery {
	q.op.Options = append(q.op.Options, opts...)
	return q
}

// Operation returns what the chain describes. A misused chain returns its
// first mistake, wrapped in [ErrInvalidOperation].
func (q *CreateTableQuery) Operation() (*CreateTable, error) {
	if q.err != nil {
		return nil, q.err
	}
	op := q.op
	hasModel := op.Model != nil
	if !hasModel && len(op.Columns) == 0 {
		return nil, invalidf("create table %q has no columns and no model", op.Table)
	}
	if hasModel && (len(op.Columns) > 0 || len(op.Constraints) > 0) {
		return nil, invalidf("create table %q mixes a model with columns or constraints", op.Table)
	}
	for i := range op.Constraints {
		if err := op.Constraints[i].validate(); err != nil {
			return nil, invalid(err)
		}
	}
	return op, nil
}

// Exec checks the chain, then has the renderer for the executor's driver apply
// it. A misused chain returns its error without touching the executor. An
// executor whose driver can't be told returns [ErrUnknownDriver], and a driver
// with no renderer returns [ErrSchemaUnsupported].
func (q *CreateTableQuery) Exec(ctx context.Context) error {
	op, err := q.Operation()
	if err != nil {
		return err
	}
	return q.db.Apply(ctx, op)
}

func (q *CreateTableQuery) addConstraint(c Constraint, err error) *CreateTableQuery {
	if q.err != nil {
		return q
	}
	if err != nil {
		q.fail(err)
		return q
	}
	q.op.Constraints = append(q.op.Constraints, c)
	return q
}

// last returns the constraint added most recently, or records a misuse
// naming what when there isn't one.
func (q *CreateTableQuery) last(what string) *Constraint {
	if q.err != nil {
		return nil
	}
	if n := len(q.op.Constraints); n > 0 {
		return &q.op.Constraints[n-1]
	}
	q.failf("%s needs a constraint before it", what)
	return nil
}

// DropTableQuery builds a [DropTable].
type DropTableQuery struct {
	builderErr

	db *DB
	op *DropTable
}

// IfExists skips the statement when the table is already gone.
func (q *DropTableQuery) IfExists() *DropTableQuery {
	q.op.IfExists = true
	return q
}

// Cascade drops whatever depends on the table too.
func (q *DropTableQuery) Cascade() *DropTableQuery {
	q.op.Cascade = true
	return q
}

// Operation returns what the chain describes, or its first mistake wrapped
// in [ErrInvalidOperation].
func (q *DropTableQuery) Operation() (*DropTable, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.op, nil
}

// Exec checks the chain, then has the renderer for the executor's driver apply
// it. A misused chain returns its error without touching the executor. An
// executor whose driver can't be told returns [ErrUnknownDriver], and a driver
// with no renderer returns [ErrSchemaUnsupported].
func (q *DropTableQuery) Exec(ctx context.Context) error {
	op, err := q.Operation()
	if err != nil {
		return err
	}
	return q.db.Apply(ctx, op)
}

// AlterTableQuery builds an [AlterTable]. Each call adds one action, and the
// modifiers (IfExists, Using, Named and so on) apply to the action before
// them.
type AlterTableQuery struct {
	builderErr

	db *DB
	op *AlterTable
}

// AddColumn adds a column.
func (q *AlterTableQuery) AddColumn(name string, typ ColumnType, opts ...ColumnOption) *AlterTableQuery {
	if q.err != nil {
		return q
	}
	col, err := newColumn(name, typ, opts)
	if err != nil {
		q.fail(err)
		return q
	}
	return q.push(AlterAction{Kind: AlterAddColumn, Column: col})
}

// DropColumn drops a column.
func (q *AlterTableQuery) DropColumn(name string) *AlterTableQuery {
	return q.onName(AlterDropColumn, "DropColumn column name", name)
}

// SetNotNull forbids NULL in col.
func (q *AlterTableQuery) SetNotNull(col string) *AlterTableQuery {
	return q.onName(AlterSetNotNull, "SetNotNull column name", col)
}

// DropNotNull allows NULL in col again.
func (q *AlterTableQuery) DropNotNull(col string) *AlterTableQuery {
	return q.onName(AlterDropNotNull, "DropNotNull column name", col)
}

// SetDefault sets a literal default on col. A nil value means DEFAULT NULL.
func (q *AlterTableQuery) SetDefault(col string, value any) *AlterTableQuery {
	if !q.need("SetDefault column name", col) {
		return q
	}
	return q.push(AlterAction{Kind: AlterSetDefault, Name: col, Default: &DefaultValue{Value: value}})
}

// SetDefaultExpr sets a default on col from a SQL expression.
func (q *AlterTableQuery) SetDefaultExpr(col, sql string) *AlterTableQuery {
	if !q.need("SetDefaultExpr column name", col) || !q.need("SetDefaultExpr expression", sql) {
		return q
	}
	return q.push(AlterAction{Kind: AlterSetDefault, Name: col, Default: &DefaultValue{Expr: sql}})
}

// DropDefault removes the default on col.
func (q *AlterTableQuery) DropDefault(col string) *AlterTableQuery {
	return q.onName(AlterDropDefault, "DropDefault column name", col)
}

// SetType changes the type of col. Follow it with Using when existing values
// need converting.
func (q *AlterTableQuery) SetType(col string, typ ColumnType) *AlterTableQuery {
	if !q.need("SetType column name", col) {
		return q
	}
	if err := typ.validate(); err != nil {
		q.failf("column %q: %w", col, err)
		return q
	}
	return q.push(AlterAction{Kind: AlterSetType, Name: col, Type: typ})
}

// Using sets the expression that converts existing values for the SetType
// before it.
func (q *AlterTableQuery) Using(sql string) *AlterTableQuery {
	if !q.need("Using expression", sql) {
		return q
	}
	if a := q.last("Using", "SetType", AlterSetType); a != nil {
		a.Using = sql
	}
	return q
}

// AddPrimaryKey adds a primary key over cols.
func (q *AlterTableQuery) AddPrimaryKey(cols ...string) *AlterTableQuery {
	return q.addConstraint(newColumnsConstraint(PrimaryKeyConstraint, "AddPrimaryKey", cols))
}

// AddUnique adds a UNIQUE constraint over cols.
func (q *AlterTableQuery) AddUnique(cols ...string) *AlterTableQuery {
	return q.addConstraint(newColumnsConstraint(UniqueConstraint, "AddUnique", cols))
}

// AddCheck adds a CHECK constraint.
func (q *AlterTableQuery) AddCheck(sql string) *AlterTableQuery {
	return q.addConstraint(newCheckConstraint("AddCheck", sql))
}

// AddForeignKey starts a foreign key over cols. Follow it with References,
// and optionally OnDelete.
func (q *AlterTableQuery) AddForeignKey(cols ...string) *AlterTableQuery {
	return q.addConstraint(newColumnsConstraint(ForeignKeyConstraint, "AddForeignKey", cols))
}

// References sets the target of the foreign key you just added.
func (q *AlterTableQuery) References(table string, cols ...string) *AlterTableQuery {
	if a := q.last("References", "AddForeignKey", AlterAddConstraint); a != nil {
		q.record(a.Constraint.setReferences(table, cols))
	}
	return q
}

// OnDelete sets the referential action of the foreign key you just added. It
// has to follow References.
func (q *AlterTableQuery) OnDelete(a Action) *AlterTableQuery {
	if act := q.last("OnDelete", "AddForeignKey", AlterAddConstraint); act != nil {
		q.record(act.Constraint.setOnDelete(a))
	}
	return q
}

// Named names the constraint you just added. Left unnamed, the driver picks
// a name.
func (q *AlterTableQuery) Named(name string) *AlterTableQuery {
	if a := q.last("Named", "an added constraint", AlterAddConstraint); a != nil {
		q.record(a.Constraint.setName(name))
	}
	return q
}

// DropConstraint drops the constraint called name.
func (q *AlterTableQuery) DropConstraint(name string) *AlterTableQuery {
	return q.onName(AlterDropConstraint, "DropConstraint constraint name", name)
}

// RenameColumn renames a column.
func (q *AlterTableQuery) RenameColumn(from, to string) *AlterTableQuery {
	if !q.need("RenameColumn old name", from) || !q.need("RenameColumn new name", to) {
		return q
	}
	return q.push(AlterAction{Kind: AlterRenameColumn, Name: from, NewName: to})
}

// RenameTo renames the table.
func (q *AlterTableQuery) RenameTo(name string) *AlterTableQuery {
	if !q.need("RenameTo table name", name) {
		return q
	}
	return q.push(AlterAction{Kind: AlterRenameTo, NewName: name})
}

// IfExists makes the DropColumn or DropConstraint before it skip a missing
// target.
func (q *AlterTableQuery) IfExists() *AlterTableQuery {
	if a := q.last("IfExists", "DropColumn or DropConstraint", AlterDropColumn, AlterDropConstraint); a != nil {
		a.IfExists = true
	}
	return q
}

// IfNotExists makes the AddColumn before it skip a column that is already
// there.
func (q *AlterTableQuery) IfNotExists() *AlterTableQuery {
	if a := q.last("IfNotExists", "AddColumn", AlterAddColumn); a != nil {
		a.IfNotExists = true
	}
	return q
}

// Cascade makes the DropColumn or DropConstraint before it drop whatever
// depends on the target too.
func (q *AlterTableQuery) Cascade() *AlterTableQuery {
	if a := q.last("Cascade", "DropColumn or DropConstraint", AlterDropColumn, AlterDropConstraint); a != nil {
		a.Cascade = true
	}
	return q
}

// Operation returns what the chain describes. A misused chain returns its
// first mistake, wrapped in [ErrInvalidOperation].
func (q *AlterTableQuery) Operation() (*AlterTable, error) {
	if q.err != nil {
		return nil, q.err
	}
	if len(q.op.Actions) == 0 {
		return nil, invalidf("alter table %q has no actions", q.op.Table)
	}
	for i := range q.op.Actions {
		if q.op.Actions[i].Kind != AlterAddConstraint {
			continue
		}
		if err := q.op.Actions[i].Constraint.validate(); err != nil {
			return nil, invalid(err)
		}
	}
	return q.op, nil
}

// Exec checks the chain, then has the renderer for the executor's driver apply
// it. A misused chain returns its error without touching the executor. An
// executor whose driver can't be told returns [ErrUnknownDriver], and a driver
// with no renderer returns [ErrSchemaUnsupported].
func (q *AlterTableQuery) Exec(ctx context.Context) error {
	op, err := q.Operation()
	if err != nil {
		return err
	}
	return q.db.Apply(ctx, op)
}

func (q *AlterTableQuery) push(a AlterAction) *AlterTableQuery {
	q.op.Actions = append(q.op.Actions, a)
	return q
}

// onName adds an action that takes nothing but a name.
func (q *AlterTableQuery) onName(kind AlterKind, what, name string) *AlterTableQuery {
	if !q.need(what, name) {
		return q
	}
	return q.push(AlterAction{Kind: kind, Name: name})
}

func (q *AlterTableQuery) addConstraint(c Constraint, err error) *AlterTableQuery {
	if q.err != nil {
		return q
	}
	if err != nil {
		q.fail(err)
		return q
	}
	return q.push(AlterAction{Kind: AlterAddConstraint, Constraint: c})
}

// last returns the action added most recently when its kind is one of kinds.
// Otherwise it records a misuse saying the call needs want before it.
func (q *AlterTableQuery) last(what, want string, kinds ...AlterKind) *AlterAction {
	if q.err != nil {
		return nil
	}
	if n := len(q.op.Actions); n > 0 && slices.Contains(kinds, q.op.Actions[n-1].Kind) {
		return &q.op.Actions[n-1]
	}
	q.failf("%s needs %s before it", what, want)
	return nil
}

// CreateIndexQuery builds a [CreateIndex].
type CreateIndexQuery struct {
	builderErr

	db *DB
	op *CreateIndex
}

// On sets the table the index covers.
func (q *CreateIndexQuery) On(table string) *CreateIndexQuery {
	if q.need("On table name", table) {
		q.op.Table = table
	}
	return q
}

// Columns adds plain columns as index keys, in order.
func (q *CreateIndexQuery) Columns(names ...string) *CreateIndexQuery {
	if q.err != nil {
		return q
	}
	if err := checkColumnNames("Columns", names); err != nil {
		q.fail(err)
		return q
	}
	for _, name := range names {
		q.op.Columns = append(q.op.Columns, IndexColumn{Name: name})
	}
	return q
}

// ColumnDesc adds a column sorted in descending order.
func (q *CreateIndexQuery) ColumnDesc(name string) *CreateIndexQuery {
	if q.need("ColumnDesc column name", name) {
		q.op.Columns = append(q.op.Columns, IndexColumn{Name: name, Desc: true})
	}
	return q
}

// ColumnExpr adds a SQL expression such as lower(name) as an index key.
func (q *CreateIndexQuery) ColumnExpr(sql string) *CreateIndexQuery {
	if q.need("ColumnExpr expression", sql) {
		q.op.Columns = append(q.op.Columns, IndexColumn{Expr: sql})
	}
	return q
}

// Unique makes the index enforce uniqueness.
func (q *CreateIndexQuery) Unique() *CreateIndexQuery {
	q.op.Unique = true
	return q
}

// IfNotExists skips the statement when the index is already there.
func (q *CreateIndexQuery) IfNotExists() *CreateIndexQuery {
	q.op.IfNotExists = true
	return q
}

// Concurrently builds the index without blocking writes, on drivers that
// support it.
func (q *CreateIndexQuery) Concurrently() *CreateIndexQuery {
	q.op.Concurrently = true
	return q
}

// Using picks the index access method, such as gin.
func (q *CreateIndexQuery) Using(method string) *CreateIndexQuery {
	if q.need("Using access method", method) {
		q.op.Using = method
	}
	return q
}

// Include adds non-key columns stored in the index.
func (q *CreateIndexQuery) Include(cols ...string) *CreateIndexQuery {
	if q.err != nil {
		return q
	}
	if err := checkColumnNames("Include", cols); err != nil {
		q.fail(err)
		return q
	}
	q.op.Include = append(q.op.Include, cols...)
	return q
}

// Where makes the index partial, covering only rows that match the SQL
// predicate.
func (q *CreateIndexQuery) Where(sql string) *CreateIndexQuery {
	if q.need("Where predicate", sql) {
		q.op.Where = sql
	}
	return q
}

// Filter is the document form of Where, for drivers that filter with a
// document, such as MongoDB's partialFilterExpression.
func (q *CreateIndexQuery) Filter(doc any) *CreateIndexQuery {
	if q.err != nil {
		return q
	}
	if doc == nil {
		q.failf("Filter needs a document, got nil")
		return q
	}
	q.op.Filter = doc
	return q
}

// With passes driver-specific options. A driver returns an error for any
// option it doesn't recognise.
func (q *CreateIndexQuery) With(opts ...any) *CreateIndexQuery {
	q.op.Options = append(q.op.Options, opts...)
	return q
}

// Operation returns what the chain describes. A misused chain returns its
// first mistake, wrapped in [ErrInvalidOperation].
func (q *CreateIndexQuery) Operation() (*CreateIndex, error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.op.Table == "" {
		return nil, invalidf("create index %q needs On with a table", q.op.Name)
	}
	if len(q.op.Columns) == 0 {
		return nil, invalidf("create index %q has no columns", q.op.Name)
	}
	return q.op, nil
}

// Exec checks the chain, then has the renderer for the executor's driver apply
// it. A misused chain returns its error without touching the executor. An
// executor whose driver can't be told returns [ErrUnknownDriver], and a driver
// with no renderer returns [ErrSchemaUnsupported].
func (q *CreateIndexQuery) Exec(ctx context.Context) error {
	op, err := q.Operation()
	if err != nil {
		return err
	}
	return q.db.Apply(ctx, op)
}

// DropIndexQuery builds a [DropIndex].
type DropIndexQuery struct {
	builderErr

	db *DB
	op *DropIndex
}

// On sets the table the index belongs to. Postgres doesn't need it, and
// MongoDB does.
func (q *DropIndexQuery) On(table string) *DropIndexQuery {
	if q.need("On table name", table) {
		q.op.Table = table
	}
	return q
}

// IfExists skips the statement when the index is already gone.
func (q *DropIndexQuery) IfExists() *DropIndexQuery {
	q.op.IfExists = true
	return q
}

// Concurrently drops the index without blocking writes, on drivers that
// support it.
func (q *DropIndexQuery) Concurrently() *DropIndexQuery {
	q.op.Concurrently = true
	return q
}

// Cascade drops whatever depends on the index too.
func (q *DropIndexQuery) Cascade() *DropIndexQuery {
	q.op.Cascade = true
	return q
}

// Operation returns what the chain describes, or its first mistake wrapped
// in [ErrInvalidOperation].
func (q *DropIndexQuery) Operation() (*DropIndex, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.op, nil
}

// Exec checks the chain, then has the renderer for the executor's driver apply
// it. A misused chain returns its error without touching the executor. An
// executor whose driver can't be told returns [ErrUnknownDriver], and a driver
// with no renderer returns [ErrSchemaUnsupported].
func (q *DropIndexQuery) Exec(ctx context.Context) error {
	op, err := q.Operation()
	if err != nil {
		return err
	}
	return q.db.Apply(ctx, op)
}
