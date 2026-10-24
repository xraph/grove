package migrate

// Operation is a schema change a builder produced and a driver renders. The
// concrete types are the five exported structs below, so a renderer switches
// on them. Nobody outside this package implements it.
type Operation interface {
	schemaOperation()
}

var (
	_ Operation = (*CreateTable)(nil)
	_ Operation = (*DropTable)(nil)
	_ Operation = (*AlterTable)(nil)
	_ Operation = (*CreateIndex)(nil)
	_ Operation = (*DropIndex)(nil)
)

// CreateTable creates a table, or a collection on drivers without tables.
//
// Columns render in the order written, then Constraints in the order written,
// because a driver that names unnamed constraints derives the name from that
// order. A builder enforces this by rejecting a column declared after a table
// constraint, so the order you write is the order a driver sees. A builder
// never sets Model together with Columns or Constraints.
type CreateTable struct {
	Table       string
	IfNotExists bool
	Columns     []Column
	Constraints []Constraint

	// Model is a grove model to take the columns from instead of Columns.
	Model any

	// Options carries driver-specific settings, such as a Mongo collection
	// option. A driver rejects any value it doesn't recognise.
	Options []any
}

func (*CreateTable) schemaOperation() {}

// DropTable drops a table, or a collection.
type DropTable struct {
	Table    string
	IfExists bool
	Cascade  bool
}

func (*DropTable) schemaOperation() {}

// AlterKind says what one [AlterAction] does.
type AlterKind int

// The changes an ALTER TABLE can carry.
const (
	AlterAddColumn AlterKind = iota + 1
	AlterDropColumn
	AlterSetNotNull
	AlterDropNotNull
	AlterSetDefault
	AlterDropDefault
	AlterSetType
	AlterAddConstraint
	AlterDropConstraint
	AlterRenameColumn
	AlterRenameTo
)

// AlterAction is one change inside an [AlterTable]. Which fields matter
// depends on Kind:
//
//   - AlterAddColumn uses Column, and IfNotExists.
//   - AlterDropColumn uses Name, IfExists and Cascade.
//   - AlterSetNotNull, AlterDropNotNull and AlterDropDefault use Name, the column.
//   - AlterSetDefault uses Name and Default.
//   - AlterSetType uses Name, Type and optionally Using.
//   - AlterAddConstraint uses Constraint.
//   - AlterDropConstraint uses Name, the constraint, with IfExists and Cascade.
//   - AlterRenameColumn renames Name to NewName.
//   - AlterRenameTo uses NewName, the table's new name.
type AlterAction struct {
	Kind    AlterKind
	Column  Column
	Name    string
	NewName string
	Default *DefaultValue
	Type    ColumnType

	// Using is the expression that converts existing values for AlterSetType.
	Using      string
	Constraint Constraint

	IfExists    bool
	IfNotExists bool
	Cascade     bool
}

// AlterTable changes an existing table. Actions run in the order written.
type AlterTable struct {
	Table   string
	Actions []AlterAction
}

func (*AlterTable) schemaOperation() {}

// IndexColumn is one key of an index: a column by Name, or an expression.
type IndexColumn struct {
	Name string
	Expr string
	Desc bool
}

// CreateIndex creates an index.
type CreateIndex struct {
	Name         string
	Table        string
	Columns      []IndexColumn
	Unique       bool
	IfNotExists  bool
	Concurrently bool

	// Using is the access method, such as gin, on drivers that have one.
	Using   string
	Include []string

	// Where is a SQL predicate for a partial index.
	Where string

	// Filter is the document form of Where, for drivers that take one.
	Filter any

	// Options carries driver-specific settings. A driver rejects any value it
	// doesn't recognise.
	Options []any
}

func (*CreateIndex) schemaOperation() {}

// DropIndex drops an index. Table is needed only on drivers that scope
// indexes to a table or collection.
type DropIndex struct {
	Name         string
	Table        string
	IfExists     bool
	Concurrently bool
	Cascade      bool
}

func (*DropIndex) schemaOperation() {}
