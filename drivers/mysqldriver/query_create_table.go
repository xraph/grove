package mysqldriver

import (
	"context"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// CreateTableQuery builds MySQL CREATE TABLE statements.
type CreateTableQuery struct {
	db          *MysqlDB
	table       *schema.Table
	model       any
	ifNotExists bool
	foreignKeys []string // raw FK constraints
	temp        bool     // TEMPORARY table
	engine      string   // MySQL storage engine (e.g., "InnoDB")
	charset     string   // MySQL character set (e.g., "utf8mb4")
	err         error
}

// NewCreateTable creates a CREATE TABLE query for the given model.
func (db *MysqlDB) NewCreateTable(model any) *CreateTableQuery {
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

// Engine sets the MySQL storage engine (e.g., "InnoDB", "MyISAM").
func (q *CreateTableQuery) Engine(engine string) *CreateTableQuery {
	q.engine = engine
	return q
}

// Charset sets the MySQL character set (e.g., "utf8mb4").
func (q *CreateTableQuery) Charset(charset string) *CreateTableQuery {
	q.charset = charset
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

		// AUTO_INCREMENT for autoincrement fields.
		if f.Options.AutoIncrement {
			buf.WriteString(" AUTO_INCREMENT")
		}

		// UNIQUE constraint.
		if f.Options.Unique {
			buf.WriteString(" UNIQUE")
		}

		// DEFAULT value.
		if f.Options.Default != "" {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(f.Options.Default)
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

	// MySQL table options.
	if q.engine != "" {
		buf.WriteString(" ENGINE=")
		buf.WriteString(q.engine)
	}

	if q.charset != "" {
		buf.WriteString(" DEFAULT CHARSET=")
		buf.WriteString(q.charset)
	}

	return buf.String(), nil, nil
}

// resolveColumnType determines the MySQL column type for a field.
// It checks for explicit SQLType, handles AutoIncrement serial types,
// and falls back to the dialect's GoToDBType mapping.
func (q *CreateTableQuery) resolveColumnType(f *schema.Field) string {
	// Explicit SQL type always wins.
	if f.Options.SQLType != "" {
		return f.Options.SQLType
	}

	// AutoIncrement fields use INT / BIGINT (AUTO_INCREMENT is added separately).
	if f.Options.AutoIncrement {
		goType := f.GoType
		if goType.Kind() == reflect.Ptr {
			goType = goType.Elem()
		}
		switch goType.Kind() {
		case reflect.Int64:
			return "BIGINT"
		default:
			return "INT"
		}
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

// DropTableQuery builds MySQL DROP TABLE statements.
type DropTableQuery struct {
	db       *MysqlDB
	table    *schema.Table
	model    any
	ifExists bool
	cascade  bool
	err      error
}

// NewDropTable creates a DROP TABLE query for the given model.
func (db *MysqlDB) NewDropTable(model any) *DropTableQuery {
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

// Cascade adds the CASCADE clause.
// Note: MySQL's CASCADE in DROP TABLE has limited effect compared to PostgreSQL.
func (q *DropTableQuery) Cascade() *DropTableQuery {
	q.cascade = true
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

	// CASCADE
	if q.cascade {
		buf.WriteString(" CASCADE")
	}

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
