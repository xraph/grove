package sqlitedriver

import (
	"context"
	"strings"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// CreateTableQuery builds SQLite CREATE TABLE statements.
type CreateTableQuery struct {
	db          *SqliteDB
	table       *schema.Table
	model       any
	ifNotExists bool
	foreignKeys []string // raw FK constraints
	temp        bool     // TEMPORARY table
	err         error
}

// NewCreateTable creates a CREATE TABLE query for the given model.
func (db *SqliteDB) NewCreateTable(model any) *CreateTableQuery {
	q := &CreateTableQuery{
		db:    db,
		model: model,
	}

	table, err := resolveTable(db.registry, model)
	if err != nil {
		q.err = err
		return q
	}
	q.table = table
	return q
}

// IfNotExists adds the IF NOT EXISTS clause.
func (q *CreateTableQuery) IfNotExists() *CreateTableQuery {
	q.ifNotExists = true
	return q
}

// Temp marks the table as TEMPORARY.
func (q *CreateTableQuery) Temp() *CreateTableQuery {
	q.temp = true
	return q
}

// WithForeignKey adds a raw foreign key constraint string.
// Example: "(user_id) REFERENCES users(id) ON DELETE CASCADE"
func (q *CreateTableQuery) WithForeignKey(fk string) *CreateTableQuery {
	q.foreignKeys = append(q.foreignKeys, fk)
	return q
}

// Build generates the SQL and args.
func (q *CreateTableQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	dialect := q.db.dialect

	// CREATE [TEMPORARY] TABLE
	if q.temp {
		buf.WriteString("CREATE TEMPORARY TABLE ")
	} else {
		buf.WriteString("CREATE TABLE ")
	}

	// IF NOT EXISTS
	if q.ifNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	// Table name.
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" (")

	// Collect column definitions and primary key columns.
	first := true
	var pkColumns []string

	for _, f := range q.table.Fields {
		// Skip fields marked ScanOnly or Skip.
		if f.Options.ScanOnly || f.Options.Skip {
			continue
		}

		if !first {
			buf.WriteString(", ")
		}
		first = false

		// Column name.
		buf.WriteString(dialect.Quote(f.Options.Column))
		_ = buf.WriteByte(' ')

		// Column type.
		colType := q.resolveColumnType(f)
		buf.WriteString(colType)

		// NOT NULL constraint.
		if f.Options.NotNull {
			buf.WriteString(" NOT NULL")
		}

		// UNIQUE constraint.
		if f.Options.Unique {
			buf.WriteString(" UNIQUE")
		}

		// DEFAULT value.
		if f.Options.Default != "" {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(formatDefaultValue(f.Options.Default))
		}

		// Track primary key columns.
		if f.Options.IsPK {
			pkColumns = append(pkColumns, f.Options.Column)
		}
	}

	// PRIMARY KEY constraint.
	if len(pkColumns) > 0 {
		buf.WriteString(", PRIMARY KEY (")
		for i, col := range pkColumns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(dialect.Quote(col))
		}
		_ = buf.WriteByte(')')
	}

	// Foreign key constraints.
	for _, fk := range q.foreignKeys {
		buf.WriteString(", FOREIGN KEY ")
		buf.WriteString(fk)
	}

	_ = buf.WriteByte(')')

	return buf.String(), nil, nil
}

// resolveColumnType determines the SQLite column type for a field.
// It checks for explicit SQLType, handles AutoIncrement INTEGER types,
// and falls back to the dialect's GoToDBType mapping.
func (q *CreateTableQuery) resolveColumnType(f *schema.Field) string {
	// Explicit SQL type always wins.
	if f.Options.SQLType != "" {
		return f.Options.SQLType
	}

	// AutoIncrement fields use INTEGER (SQLite uses INTEGER PRIMARY KEY AUTOINCREMENT).
	if f.Options.AutoIncrement {
		return "INTEGER"
	}

	return q.db.dialect.GoToDBType(f.GoType, f.Options)
}

// Exec executes the CREATE TABLE statement.
func (q *CreateTableQuery) Exec(ctx context.Context) (driver.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	return q.db.Exec(ctx, query, args...)
}

// DropTableQuery builds SQLite DROP TABLE statements.
type DropTableQuery struct {
	db       *SqliteDB
	table    *schema.Table
	model    any
	ifExists bool
	err      error
}

// NewDropTable creates a DROP TABLE query for the given model.
func (db *SqliteDB) NewDropTable(model any) *DropTableQuery {
	q := &DropTableQuery{
		db:    db,
		model: model,
	}

	table, err := resolveTable(db.registry, model)
	if err != nil {
		q.err = err
		return q
	}
	q.table = table
	return q
}

// IfExists adds the IF EXISTS clause.
func (q *DropTableQuery) IfExists() *DropTableQuery {
	q.ifExists = true
	return q
}

// Build generates the SQL and args.
func (q *DropTableQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	dialect := q.db.dialect

	buf.WriteString("DROP TABLE ")

	// IF EXISTS
	if q.ifExists {
		buf.WriteString("IF EXISTS ")
	}

	// Table name.
	buf.WriteString(dialect.Quote(q.table.Name))

	return buf.String(), nil, nil
}

// Exec executes the DROP TABLE statement.
func (q *DropTableQuery) Exec(ctx context.Context) (driver.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	return q.db.Exec(ctx, query, args...)
}

// keywordDefaults are bare SQL tokens that are valid DEFAULT expressions on
// their own and must never be quoted as string literals.
var keywordDefaults = map[string]struct{}{
	"true": {}, "false": {}, "null": {},
	"current_timestamp": {}, "current_date": {}, "current_time": {},
	"localtime": {}, "localtimestamp": {},
}

// formatDefaultValue renders a column DEFAULT for the generated DDL.
//
// The struct-tag parser strips the surrounding single quotes from
// `default:'active'`, leaving the bare word `active`. Emitting that verbatim
// produces `DEFAULT active`, but a bare identifier is not a valid SQLite column
// DEFAULT — SQLite wants a literal, a parenthesized constant expression, or one
// of CURRENT_TIME/CURRENT_DATE/CURRENT_TIMESTAMP. Anything that isn't a number,
// boolean/NULL/keyword, cast, or function-call expression is therefore treated
// as a string literal and quoted. Values that already carry quotes are passed
// through untouched.
func formatDefaultValue(v string) string {
	if v == "" {
		return v
	}

	// Already a quoted string literal — leave as-is.
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return v
	}

	// Bare keyword/boolean/NULL default.
	if _, ok := keywordDefaults[strings.ToLower(v)]; ok {
		return v
	}

	// Numeric literal.
	if isNumericLiteral(v) {
		return v
	}

	// Expression: function call (now(), gen_random_uuid()) or a cast
	// (e.g. '{}'::jsonb). Emit verbatim. SQLite is typeless so casts are
	// unlikely, but the rule is harmless.
	if strings.ContainsAny(v, "()") || strings.Contains(v, "::") {
		return v
	}

	// Treat as a string literal, escaping embedded single quotes.
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// isNumericLiteral reports whether v is a plain integer or decimal literal,
// optionally signed — the forms that are valid unquoted SQL DEFAULTs.
func isNumericLiteral(v string) bool {
	if v == "" {
		return false
	}
	i := 0
	if v[0] == '+' || v[0] == '-' {
		i++
		if i == len(v) {
			return false
		}
	}
	dotSeen := false
	digitSeen := false
	for ; i < len(v); i++ {
		switch {
		case v[i] >= '0' && v[i] <= '9':
			digitSeen = true
		case v[i] == '.' && !dotSeen:
			dotSeen = true
		default:
			return false
		}
	}
	return digitSeen
}
