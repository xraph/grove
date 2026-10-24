package migrate

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrInvalidOperation is wrapped by every error a schema builder reports for
// a misused chain or an operation that can't be rendered.
var ErrInvalidOperation = errors.New("migrate: invalid schema operation")

// TypeKind says which portable type a [ColumnType] stands for.
type TypeKind int

// The portable column types. Each driver maps them to its own type names.
const (
	KindText TypeKind = iota + 1
	KindSmallInt
	KindInteger
	KindBigInt
	KindBigSerial
	KindBoolean
	KindTimestamp
	KindDate
	KindJSON
	KindUUID
	KindBytes
	KindFloat
	KindNumeric
	KindArray
	KindRaw
)

// ColumnType is a driver-agnostic column type. Use the package variables
// ([Text], [Integer] and friends), [Numeric], [ArrayOf], or [Type] for
// anything a driver names but grove doesn't.
type ColumnType struct {
	Kind TypeKind

	// Raw is the type name passed straight to the driver when Kind is KindRaw.
	Raw string

	// Precision and Scale apply to KindNumeric. Both zero means no limits.
	Precision, Scale int

	// Elem is the element type when Kind is KindArray.
	Elem *ColumnType
}

// The portable types.
var (
	Text      = ColumnType{Kind: KindText}
	SmallInt  = ColumnType{Kind: KindSmallInt}
	Integer   = ColumnType{Kind: KindInteger}
	BigInt    = ColumnType{Kind: KindBigInt}
	BigSerial = ColumnType{Kind: KindBigSerial}
	Boolean   = ColumnType{Kind: KindBoolean}
	Timestamp = ColumnType{Kind: KindTimestamp}
	Date      = ColumnType{Kind: KindDate}
	JSON      = ColumnType{Kind: KindJSON}
	UUID      = ColumnType{Kind: KindUUID}
	Bytes     = ColumnType{Kind: KindBytes}
	Float     = ColumnType{Kind: KindFloat}
)

// Numeric is an exact decimal with the given precision and scale.
func Numeric(precision, scale int) ColumnType {
	return ColumnType{Kind: KindNumeric, Precision: precision, Scale: scale}
}

// ArrayOf is an array of elem.
func ArrayOf(elem ColumnType) ColumnType {
	return ColumnType{Kind: KindArray, Elem: &elem}
}

// Type passes raw to the driver untouched, for types with no portable name
// such as Type("INET").
func Type(raw string) ColumnType {
	return ColumnType{Kind: KindRaw, Raw: raw}
}

// validate reports why t can't be rendered. The zero value is rejected so a
// forgotten type fails loudly instead of rendering as nothing.
func (t ColumnType) validate() error {
	switch t.Kind {
	case KindText, KindSmallInt, KindInteger, KindBigInt, KindBigSerial,
		KindBoolean, KindTimestamp, KindDate, KindJSON, KindUUID, KindBytes, KindFloat:
		return nil
	case KindNumeric:
		if t.Precision < 0 || t.Scale < 0 {
			return fmt.Errorf("numeric(%d, %d) has a negative precision or scale", t.Precision, t.Scale)
		}
		if t.Precision == 0 && t.Scale != 0 {
			return fmt.Errorf("numeric scale %d needs a precision", t.Scale)
		}
		return nil
	case KindArray:
		if t.Elem == nil {
			return errors.New("array type has no element type")
		}
		return t.Elem.validate()
	case KindRaw:
		if t.Raw == "" {
			return errors.New("raw type is empty")
		}
		return nil
	default:
		return fmt.Errorf("column type is not set (kind %d)", t.Kind)
	}
}

// Action is a foreign key referential action.
type Action string

// The referential actions a foreign key accepts.
const (
	NoAction   Action = "NO ACTION"
	Restrict   Action = "RESTRICT"
	Cascade    Action = "CASCADE"
	SetNull    Action = "SET NULL"
	SetDefault Action = "SET DEFAULT"
)

// validate keeps a made-up action from reaching a renderer that splices it
// into SQL.
func (a Action) validate() error {
	switch a {
	case NoAction, Restrict, Cascade, SetNull, SetDefault:
		return nil
	default:
		return fmt.Errorf("unknown referential action %q", string(a))
	}
}

// Reference is the target of a foreign key.
type Reference struct {
	Table   string
	Columns []string

	// OnDelete is empty when the driver's default applies.
	OnDelete Action
}

// DefaultValue is a column default. A non-empty Expr is a SQL expression the
// driver splices in as written. Otherwise Value is a literal, and nil means
// NULL.
type DefaultValue struct {
	Value any
	Expr  string
}

// Column describes one column of a table.
type Column struct {
	Name       string
	Type       ColumnType
	NotNull    bool
	Default    *DefaultValue
	PrimaryKey bool
	Unique     bool

	// Check is a SQL expression for a column-level CHECK constraint.
	Check      string
	References *Reference

	// Identity makes the column GENERATED ALWAYS AS IDENTITY.
	Identity bool

	// Generated is a SQL expression for a stored generated column.
	Generated string
}

// ConstraintKind says what a [Constraint] enforces.
type ConstraintKind int

// The table constraints a builder can declare.
const (
	PrimaryKeyConstraint ConstraintKind = iota + 1
	UniqueConstraint
	CheckConstraint
	ForeignKeyConstraint
)

// Constraint is a table-level constraint. Name is empty when the driver
// should pick one.
type Constraint struct {
	Kind       ConstraintKind
	Name       string
	Columns    []string
	Check      string
	References *Reference
}

// checkColumnNames rejects an empty list or a blank entry. what names the
// call for the error message.
func checkColumnNames(what string, names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("%s needs at least one column", what)
	}
	if slices.Contains(names, "") {
		return fmt.Errorf("%s has an empty column name", what)
	}
	return nil
}

// newColumnsConstraint builds a primary key, unique or foreign key
// constraint over cols.
func newColumnsConstraint(kind ConstraintKind, what string, cols []string) (Constraint, error) {
	if err := checkColumnNames(what, cols); err != nil {
		return Constraint{}, err
	}
	return Constraint{Kind: kind, Columns: slices.Clone(cols)}, nil
}

func newCheckConstraint(what, expr string) (Constraint, error) {
	if expr == "" {
		return Constraint{}, fmt.Errorf("%s needs a SQL expression", what)
	}
	return Constraint{Kind: CheckConstraint, Check: expr}, nil
}

func (c *Constraint) setName(name string) error {
	if name == "" {
		return errors.New("constraint name is empty")
	}
	c.Name = name
	return nil
}

func (c *Constraint) setReferences(table string, cols []string) error {
	if c.Kind != ForeignKeyConstraint {
		return errors.New("references need a foreign key before them")
	}
	if c.References != nil {
		return errors.New("foreign key already has references")
	}
	ref, err := newReference(table, cols)
	if err != nil {
		return err
	}
	c.References = ref
	return nil
}

func (c *Constraint) setOnDelete(a Action) error {
	if c.Kind != ForeignKeyConstraint {
		return errors.New("on delete needs a foreign key before it")
	}
	if c.References == nil {
		return errors.New("on delete needs references before it")
	}
	if err := a.validate(); err != nil {
		return err
	}
	c.References.OnDelete = a
	return nil
}

// validate runs the checks that only make sense once the whole chain is
// written: a foreign key has to end up with a matching target.
func (c *Constraint) validate() error {
	if c.Kind != ForeignKeyConstraint {
		return nil
	}
	if c.References == nil {
		return fmt.Errorf("foreign key on (%s) has no References", strings.Join(c.Columns, ", "))
	}
	if len(c.References.Columns) != len(c.Columns) {
		return fmt.Errorf("foreign key on (%s) has %d columns but references %d",
			strings.Join(c.Columns, ", "), len(c.Columns), len(c.References.Columns))
	}
	return nil
}

func newReference(table string, cols []string) (*Reference, error) {
	if table == "" {
		return nil, errors.New("references need a table")
	}
	if err := checkColumnNames("References", cols); err != nil {
		return nil, err
	}
	return &Reference{Table: table, Columns: slices.Clone(cols)}, nil
}
