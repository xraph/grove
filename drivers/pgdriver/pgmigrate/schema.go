package pgmigrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
	"github.com/xraph/grove/schema"
)

// driverName is what migrate.Unsupported reports and what the executor
// answers to, so an error names the same driver a caller registered.
const driverName = "pg"

// quoteIdent goes through the dialect the query builders use, so a
// migration and a model-built query quote identifiers the same way.
var quoteIdent = (&pgdriver.PgDialect{}).Quote

// typeNames maps the portable types that need no parameters.
var typeNames = map[migrate.TypeKind]string{
	migrate.KindText:      "TEXT",
	migrate.KindSmallInt:  "SMALLINT",
	migrate.KindInteger:   "INTEGER",
	migrate.KindBigInt:    "BIGINT",
	migrate.KindBigSerial: "BIGSERIAL",
	migrate.KindBoolean:   "BOOLEAN",
	migrate.KindTimestamp: "TIMESTAMPTZ",
	migrate.KindDate:      "DATE",
	migrate.KindJSON:      "JSONB",
	migrate.KindUUID:      "UUID",
	migrate.KindBytes:     "BYTEA",
	migrate.KindFloat:     "DOUBLE PRECISION",
}

// Renderer applies schema operations to Postgres. It is what the pg executor
// registers with the migrate package, so a migration's builders reach it.
type Renderer struct{}

var _ migrate.Renderer = Renderer{}

// Apply renders op and runs each statement through exec in order. It goes
// through the executor the migration was handed, not a connection of its
// own, so a wrapper or an open transaction sees every statement.
func (Renderer) Apply(ctx context.Context, exec migrate.Executor, op migrate.Operation) error {
	stmts, err := Render(op)
	if err != nil {
		return err
	}
	for i, stmt := range stmts {
		if _, execErr := exec.Exec(ctx, stmt); execErr != nil {
			// The earlier statements already ran, so a caller has to know how
			// far an operation got before it stopped.
			if len(stmts) > 1 {
				return fmt.Errorf("pgmigrate: %s (statement %d of %d): %w", describe(op), i+1, len(stmts), execErr)
			}
			return fmt.Errorf("pgmigrate: %s: %w", describe(op), execErr)
		}
	}
	return nil
}

// Render turns op into the Postgres statements that carry it out, in the
// order they have to run. It needs no connection.
func Render(op migrate.Operation) ([]string, error) {
	switch o := op.(type) {
	case *migrate.CreateTable:
		return renderCreateTable(o)
	case *migrate.DropTable:
		return []string{renderDropTable(o)}, nil
	case *migrate.AlterTable:
		return renderAlterTable(o)
	case *migrate.CreateIndex:
		stmt, err := renderCreateIndex(o)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	case *migrate.DropIndex:
		stmt, err := renderDropIndex(o)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	default:
		return nil, migrate.Unsupported(driverName, fmt.Sprintf("operation %T", op))
	}
}

// describe names the statement an error came from.
func describe(op migrate.Operation) string {
	switch o := op.(type) {
	case *migrate.CreateTable:
		return "create table " + o.Table
	case *migrate.DropTable:
		return "drop table " + o.Table
	case *migrate.AlterTable:
		return "alter table " + o.Table
	case *migrate.CreateIndex:
		return "create index " + o.Name
	case *migrate.DropIndex:
		return "drop index " + o.Name
	default:
		return fmt.Sprintf("%T", op)
	}
}

// invalid reports a misused operation. The detail comes first so the message
// reads naturally, and the sentinel is wrapped so callers can match it.
func invalid(detail string) error {
	return fmt.Errorf("pgmigrate: %s: %w", detail, migrate.ErrInvalidOperation)
}

func invalidf(format string, args ...any) error {
	return invalid(fmt.Sprintf(format, args...))
}

func renderCreateTable(op *migrate.CreateTable) ([]string, error) {
	if len(op.Options) > 0 {
		return nil, invalid("postgres takes no driver options")
	}
	if op.Model != nil {
		return renderModelTable(op)
	}
	if len(op.Columns) == 0 {
		return nil, invalidf("create table %q has no columns and no model", op.Table)
	}

	defs := make([]string, 0, len(op.Columns)+len(op.Constraints))
	for i := range op.Columns {
		def, err := columnDef(&op.Columns[i])
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	for i := range op.Constraints {
		def, err := constraintDef(&op.Constraints[i])
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if op.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdent(op.Table))
	b.WriteString(" (\n\t")
	b.WriteString(strings.Join(defs, ",\n\t"))
	b.WriteString("\n)")
	return []string{b.String()}, nil
}

// renderModelTable leaves the columns to the query builder that already knows
// how to read a model's tags. That builder takes the table name from the
// model's grove tag and nothing else, so the name the migration wrote is
// checked against it first. Without the check a mismatch would create a table
// other than the one asked for and still report success.
func renderModelTable(op *migrate.CreateTable) ([]string, error) {
	if len(op.Columns) > 0 || len(op.Constraints) > 0 {
		return nil, invalidf("create table %q mixes a model with columns or constraints", op.Table)
	}
	table, err := schema.NewRegistry().Register(op.Model)
	if err != nil {
		return nil, fmt.Errorf("pgmigrate: create table %q from model: %w: %w", op.Table, err, migrate.ErrInvalidOperation)
	}
	if table.Name != op.Table {
		return nil, invalidf("create table %q: model %T maps to table %q", op.Table, op.Model, table.Name)
	}

	q := pgdriver.New().NewCreateTable(op.Model)
	if op.IfNotExists {
		q.IfNotExists()
	}
	stmt, _, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("pgmigrate: create table %s from model: %w", op.Table, err)
	}
	return []string{stmt}, nil
}

func renderDropTable(op *migrate.DropTable) string {
	var b strings.Builder
	b.WriteString("DROP TABLE ")
	if op.IfExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(quoteIdent(op.Table))
	if op.Cascade {
		b.WriteString(" CASCADE")
	}
	return b.String()
}

// renderAlterTable joins neighbouring actions into one ALTER TABLE so
// Postgres applies them together. A rename gets a statement of its own because
// RENAME can't share one with other subcommands, and it ends the run before it
// so everything still executes in the order written.
func renderAlterTable(op *migrate.AlterTable) ([]string, error) {
	if len(op.Actions) == 0 {
		return nil, invalidf("alter table %q has no actions", op.Table)
	}

	stmts := make([]string, 0, len(op.Actions))
	clauses := make([]string, 0, len(op.Actions))
	// Follows RenameTo: every statement after it has to name the table as it
	// is now, or Postgres can't find the relation.
	table := op.Table
	flush := func() {
		if len(clauses) == 0 {
			return
		}
		stmts = append(stmts, "ALTER TABLE "+quoteIdent(table)+" "+strings.Join(clauses, ", "))
		clauses = clauses[:0]
	}

	for i := range op.Actions {
		a := &op.Actions[i]
		if err := checkAlterNames(a); err != nil {
			return nil, invalidf("alter table %q action %d: %v", op.Table, i+1, err)
		}

		switch a.Kind {
		case migrate.AlterRenameColumn:
			flush()
			stmts = append(stmts, "ALTER TABLE "+quoteIdent(table)+
				" RENAME COLUMN "+quoteIdent(a.Name)+" TO "+quoteIdent(a.NewName))
		case migrate.AlterRenameTo:
			flush()
			stmts = append(stmts, "ALTER TABLE "+quoteIdent(table)+" RENAME TO "+quoteIdent(a.NewName))
			table = a.NewName
		default:
			clause, err := alterClause(a)
			if errors.Is(err, migrate.ErrInvalidOperation) {
				// Column and constraint rendering already explain themselves.
				return nil, err
			}
			if err != nil {
				return nil, invalidf("alter table %q action %d: %v", op.Table, i+1, err)
			}
			clauses = append(clauses, clause)
		}
	}
	flush()
	return stmts, nil
}

// checkAlterNames catches an action built by hand without the name it can't
// do without. The builders never leave one out, but Render is public and an
// empty identifier would only fail later, inside Postgres.
func checkAlterNames(a *migrate.AlterAction) error {
	switch a.Kind {
	case migrate.AlterDropColumn, migrate.AlterSetNotNull, migrate.AlterDropNotNull,
		migrate.AlterSetDefault, migrate.AlterDropDefault, migrate.AlterSetType,
		migrate.AlterDropConstraint, migrate.AlterRenameColumn:
		if a.Name == "" {
			return errors.New("the action has no column or constraint name")
		}
	}
	if (a.Kind == migrate.AlterRenameColumn || a.Kind == migrate.AlterRenameTo) && a.NewName == "" {
		return errors.New("a rename has no new name")
	}
	return nil
}

// alterClause renders one action as it sits inside an ALTER TABLE. Renames
// are not here because they can't be joined with other clauses.
func alterClause(a *migrate.AlterAction) (string, error) {
	switch a.Kind {
	case migrate.AlterAddColumn:
		def, err := columnDef(&a.Column)
		if err != nil {
			return "", err
		}
		return "ADD COLUMN " + optional(a.IfNotExists, "IF NOT EXISTS ") + def, nil
	case migrate.AlterDropColumn:
		return "DROP COLUMN " + optional(a.IfExists, "IF EXISTS ") + quoteIdent(a.Name) + optional(a.Cascade, " CASCADE"), nil
	case migrate.AlterSetNotNull:
		return "ALTER COLUMN " + quoteIdent(a.Name) + " SET NOT NULL", nil
	case migrate.AlterDropNotNull:
		return "ALTER COLUMN " + quoteIdent(a.Name) + " DROP NOT NULL", nil
	case migrate.AlterSetDefault:
		if a.Default == nil {
			return "", fmt.Errorf("column %q has no default to set", a.Name)
		}
		def, err := defaultSQL(a.Default)
		if err != nil {
			return "", fmt.Errorf("column %q default: %w", a.Name, err)
		}
		return "ALTER COLUMN " + quoteIdent(a.Name) + " SET DEFAULT " + def, nil
	case migrate.AlterDropDefault:
		return "ALTER COLUMN " + quoteIdent(a.Name) + " DROP DEFAULT", nil
	case migrate.AlterSetType:
		typ, err := typeSQL(a.Type)
		if err != nil {
			return "", fmt.Errorf("column %q: %w", a.Name, err)
		}
		clause := "ALTER COLUMN " + quoteIdent(a.Name) + " TYPE " + typ
		if a.Using != "" {
			clause += " USING " + a.Using
		}
		return clause, nil
	case migrate.AlterAddConstraint:
		def, err := constraintDef(&a.Constraint)
		if err != nil {
			return "", err
		}
		return "ADD " + def, nil
	case migrate.AlterDropConstraint:
		return "DROP CONSTRAINT " + optional(a.IfExists, "IF EXISTS ") + quoteIdent(a.Name) + optional(a.Cascade, " CASCADE"), nil
	default:
		return "", fmt.Errorf("unknown alter action kind %d", a.Kind)
	}
}

func renderCreateIndex(op *migrate.CreateIndex) (string, error) {
	if op.Filter != nil {
		return "", migrate.Unsupported(driverName, "index filter documents")
	}
	if len(op.Options) > 0 {
		return "", invalid("postgres takes no driver options")
	}
	if op.Name == "" {
		return "", invalid("create index needs a name")
	}
	if op.Table == "" {
		return "", invalidf("create index %q needs a table", op.Name)
	}
	if op.Using != "" && !isBareWord(op.Using) {
		return "", invalidf("create index %q: access method %q is not a plain name", op.Name, op.Using)
	}

	keys, err := indexKeys(op)
	if err != nil {
		return "", err
	}
	if slices.Contains(op.Include, "") {
		return "", invalidf("create index %q has an empty included column name", op.Name)
	}

	var b strings.Builder
	b.WriteString("CREATE ")
	b.WriteString(optional(op.Unique, "UNIQUE "))
	b.WriteString("INDEX ")
	b.WriteString(optional(op.Concurrently, "CONCURRENTLY "))
	b.WriteString(optional(op.IfNotExists, "IF NOT EXISTS "))
	b.WriteString(quoteIdent(op.Name))
	b.WriteString(" ON ")
	b.WriteString(quoteIdent(op.Table))
	if op.Using != "" {
		b.WriteString(" USING ")
		b.WriteString(op.Using)
	}
	b.WriteString(" (")
	b.WriteString(strings.Join(keys, ", "))
	b.WriteByte(')')
	if len(op.Include) > 0 {
		b.WriteString(" INCLUDE (")
		b.WriteString(quoteList(op.Include))
		b.WriteByte(')')
	}
	if op.Where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(op.Where)
	}
	return b.String(), nil
}

// indexKeys renders each key of the index: a column is quoted, an expression
// goes in as written inside its own parentheses, which Postgres requires.
func indexKeys(op *migrate.CreateIndex) ([]string, error) {
	if len(op.Columns) == 0 {
		return nil, invalidf("create index %q has no columns", op.Name)
	}
	keys := make([]string, len(op.Columns))
	for i, c := range op.Columns {
		switch {
		case c.Name != "" && c.Expr != "":
			return nil, invalidf("create index %q key %d has both a column name and an expression", op.Name, i+1)
		case c.Name != "":
			keys[i] = quoteIdent(c.Name)
		case c.Expr != "":
			keys[i] = "(" + c.Expr + ")"
		default:
			return nil, invalidf("create index %q key %d has neither a column name nor an expression", op.Name, i+1)
		}
		keys[i] += optional(c.Desc, " DESC")
	}
	return keys, nil
}

func renderDropIndex(op *migrate.DropIndex) (string, error) {
	if op.Name == "" {
		return "", invalid("drop index needs a name")
	}
	if op.Concurrently && op.Cascade {
		return "", invalidf("drop index %q: postgres does not allow CASCADE with CONCURRENTLY", op.Name)
	}
	// Table is for drivers that scope an index to a table. Postgres names
	// indexes in the schema, so it plays no part here.
	return "DROP INDEX " + optional(op.Concurrently, "CONCURRENTLY ") + optional(op.IfExists, "IF EXISTS ") +
		quoteIdent(op.Name) + optional(op.Cascade, " CASCADE"), nil
}

// optional returns clause when on is set and nothing otherwise, so a statement
// reads as its template with the optional words switched on.
func optional(on bool, clause string) string {
	if on {
		return clause
	}
	return ""
}

// isBareWord reports whether s is a plain unquoted name. An index access
// method goes into the statement as written, so anything that could change
// the statement's shape is refused.
func isBareWord(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
		digit := c >= '0' && c <= '9'
		if !letter && (!digit || i == 0) {
			return false
		}
	}
	return true
}

// columnDef renders one column the way CREATE TABLE and ALTER TABLE ADD
// COLUMN both take it. The clause order is fixed and does not depend on the
// order the options were written in.
func columnDef(c *migrate.Column) (string, error) {
	if c.Identity && c.Generated != "" {
		return "", invalidf("column %q is both identity and generated", c.Name)
	}
	typ, err := typeSQL(c.Type)
	if err != nil {
		return "", invalidf("column %q: %v", c.Name, err)
	}

	var b strings.Builder
	b.WriteString(quoteIdent(c.Name))
	b.WriteByte(' ')
	b.WriteString(typ)

	switch {
	case c.Generated != "":
		b.WriteString(" GENERATED ALWAYS AS (")
		b.WriteString(c.Generated)
		b.WriteString(") STORED")
	case c.Identity:
		b.WriteString(" GENERATED ALWAYS AS IDENTITY")
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	if c.Default != nil {
		def, err := defaultSQL(c.Default)
		if err != nil {
			return "", invalidf("column %q default: %v", c.Name, err)
		}
		b.WriteString(" DEFAULT ")
		b.WriteString(def)
	}
	if c.PrimaryKey {
		b.WriteString(" PRIMARY KEY")
	}
	if c.Unique {
		b.WriteString(" UNIQUE")
	}
	if c.Check != "" {
		b.WriteString(" CHECK (")
		b.WriteString(c.Check)
		b.WriteByte(')')
	}
	if c.References != nil {
		ref, err := referencesSQL(c.References)
		if err != nil {
			return "", invalidf("column %q: %v", c.Name, err)
		}
		b.WriteByte(' ')
		b.WriteString(ref)
	}
	return b.String(), nil
}

// typeSQL maps a portable type to its Postgres name.
func typeSQL(t migrate.ColumnType) (string, error) {
	if name, ok := typeNames[t.Kind]; ok {
		return name, nil
	}
	switch t.Kind {
	case migrate.KindNumeric:
		switch {
		case t.Precision < 0 || t.Scale < 0:
			return "", fmt.Errorf("numeric(%d, %d) has a negative precision or scale", t.Precision, t.Scale)
		case t.Precision == 0 && t.Scale == 0:
			return "NUMERIC", nil
		case t.Precision == 0:
			return "", fmt.Errorf("numeric scale %d needs a precision", t.Scale)
		}
		return fmt.Sprintf("NUMERIC(%d, %d)", t.Precision, t.Scale), nil
	case migrate.KindArray:
		if t.Elem == nil {
			return "", errors.New("array type has no element type")
		}
		elem, err := typeSQL(*t.Elem)
		if err != nil {
			return "", err
		}
		return elem + "[]", nil
	case migrate.KindRaw:
		if t.Raw == "" {
			return "", errors.New("raw type is empty")
		}
		return t.Raw, nil
	default:
		return "", fmt.Errorf("column type is not set (kind %d)", t.Kind)
	}
}

// defaultSQL renders a column default. An expression goes in as written, and
// a literal is quoted by its kind. Anything that isn't a plain string, bool,
// integer or float has no single obvious SQL form, so it is refused and the
// caller reaches for DefaultExpr.
func defaultSQL(d *migrate.DefaultValue) (string, error) {
	if d.Expr != "" {
		return d.Expr, nil
	}
	if d.Value == nil {
		return "NULL", nil
	}

	// Kinds rather than concrete types, so a named string such as a status
	// enum works as a default without a conversion.
	v := reflect.ValueOf(d.Value)
	switch v.Kind() {
	case reflect.String:
		return "'" + strings.ReplaceAll(v.String(), "'", "''") + "'", nil
	case reflect.Bool:
		if v.Bool() {
			return "TRUE", nil
		}
		return "FALSE", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		// NaN and Inf print as bare words that Postgres would read as column
		// references, not numbers.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("float %v has no SQL literal, use DefaultExpr", f)
		}
		bits := 64
		if v.Kind() == reflect.Float32 {
			bits = 32
		}
		return strconv.FormatFloat(f, 'g', -1, bits), nil
	default:
		return "", fmt.Errorf("a %T literal has no SQL form, use DefaultExpr", d.Value)
	}
}

// referencesSQL renders REFERENCES with its optional ON DELETE action. The
// action is checked against the known set because it is spliced into the
// statement as written.
func referencesSQL(r *migrate.Reference) (string, error) {
	if r.Table == "" {
		return "", errors.New("references need a table")
	}
	if len(r.Columns) == 0 {
		return "", errors.New("references need at least one column")
	}

	var b strings.Builder
	b.WriteString("REFERENCES ")
	b.WriteString(quoteIdent(r.Table))
	b.WriteString(" (")
	b.WriteString(quoteList(r.Columns))
	b.WriteByte(')')

	switch r.OnDelete {
	case "":
	case migrate.NoAction, migrate.Restrict, migrate.Cascade, migrate.SetNull, migrate.SetDefault:
		b.WriteString(" ON DELETE ")
		b.WriteString(string(r.OnDelete))
	default:
		return "", fmt.Errorf("unknown referential action %q", string(r.OnDelete))
	}
	return b.String(), nil
}

// constraintDef renders a table constraint, with CONSTRAINT <name> in front
// when it has one. ALTER TABLE ADD takes the same text.
func constraintDef(c *migrate.Constraint) (string, error) {
	body, err := constraintBody(c)
	if err != nil {
		return "", invalid(err.Error())
	}
	if c.Name == "" {
		return body, nil
	}
	return "CONSTRAINT " + quoteIdent(c.Name) + " " + body, nil
}

func constraintBody(c *migrate.Constraint) (string, error) {
	switch c.Kind {
	case migrate.PrimaryKeyConstraint:
		if len(c.Columns) == 0 {
			return "", errors.New("primary key has no columns")
		}
		return "PRIMARY KEY (" + quoteList(c.Columns) + ")", nil
	case migrate.UniqueConstraint:
		if len(c.Columns) == 0 {
			return "", errors.New("unique constraint has no columns")
		}
		return "UNIQUE (" + quoteList(c.Columns) + ")", nil
	case migrate.CheckConstraint:
		if c.Check == "" {
			return "", errors.New("check constraint has no expression")
		}
		return "CHECK (" + c.Check + ")", nil
	case migrate.ForeignKeyConstraint:
		if len(c.Columns) == 0 {
			return "", errors.New("foreign key has no columns")
		}
		if c.References == nil {
			return "", fmt.Errorf("foreign key on (%s) has no references", strings.Join(c.Columns, ", "))
		}
		if len(c.References.Columns) != len(c.Columns) {
			return "", fmt.Errorf("foreign key on (%s) has %d columns but references %d",
				strings.Join(c.Columns, ", "), len(c.Columns), len(c.References.Columns))
		}
		ref, err := referencesSQL(c.References)
		if err != nil {
			return "", err
		}
		return "FOREIGN KEY (" + quoteList(c.Columns) + ") " + ref, nil
	default:
		return "", fmt.Errorf("unknown constraint kind %d", c.Kind)
	}
}

// quoteList quotes each name and joins them for a column list.
func quoteList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdent(name)
	}
	return strings.Join(quoted, ", ")
}
