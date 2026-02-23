package clickhousedriver

import (
	"context"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// CreateTableQuery builds ClickHouse CREATE TABLE statements.
// ClickHouse requires an ENGINE clause and ORDER BY for MergeTree family engines.
type CreateTableQuery struct {
	db          *ClickHouseDB
	table       *schema.Table
	model       any
	ifNotExists bool
	engine      string   // "MergeTree()" is default
	orderBy     []string // Required for MergeTree family
	partitionBy string
	ttl         string
	settings    []string
	err         error
}

// NewCreateTable creates a CREATE TABLE query for the given model.
func (db *ClickHouseDB) NewCreateTable(model any) *CreateTableQuery {
	q := &CreateTableQuery{
		db:     db,
		model:  model,
		engine: "MergeTree()",
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

// Engine sets the table engine (e.g., "MergeTree()", "ReplacingMergeTree(version)",
// "SummingMergeTree(amount)", "CollapsingMergeTree(sign)").
func (q *CreateTableQuery) Engine(e string) *CreateTableQuery {
	q.engine = e
	return q
}

// OrderBy sets the ORDER BY columns, which are required for MergeTree family engines.
// This determines the sort order within each data part.
func (q *CreateTableQuery) OrderBy(cols ...string) *CreateTableQuery {
	q.orderBy = append(q.orderBy, cols...)
	return q
}

// PartitionBy sets the PARTITION BY expression.
// Example: "toYYYYMM(created_at)"
func (q *CreateTableQuery) PartitionBy(expr string) *CreateTableQuery {
	q.partitionBy = expr
	return q
}

// TTL sets the TTL expression for automatic data expiration.
// Example: "created_at + INTERVAL 30 DAY"
func (q *CreateTableQuery) TTL(expr string) *CreateTableQuery {
	q.ttl = expr
	return q
}

// Settings adds table-level settings.
// Example: "index_granularity = 8192"
func (q *CreateTableQuery) Settings(s ...string) *CreateTableQuery {
	q.settings = append(q.settings, s...)
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

	// CREATE TABLE
	buf.WriteString("CREATE TABLE ")

	// IF NOT EXISTS
	if q.ifNotExists {
		buf.WriteString("IF NOT EXISTS ")
	}

	// Table name.
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" (")

	// Collect column definitions.
	first := true

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

		// DEFAULT value.
		if f.Options.Default != "" {
			buf.WriteString(" DEFAULT ")
			buf.WriteString(f.Options.Default)
		}
	}

	_ = buf.WriteByte(')')

	// ENGINE clause (required for ClickHouse).
	buf.WriteString(" ENGINE = ")
	buf.WriteString(q.engine)

	// ORDER BY clause (required for MergeTree family).
	if len(q.orderBy) > 0 {
		buf.WriteString(" ORDER BY (")
		for i, col := range q.orderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(dialect.Quote(col))
		}
		_ = buf.WriteByte(')')
	}

	// PARTITION BY clause.
	if q.partitionBy != "" {
		buf.WriteString(" PARTITION BY ")
		buf.WriteString(q.partitionBy)
	}

	// TTL clause.
	if q.ttl != "" {
		buf.WriteString(" TTL ")
		buf.WriteString(q.ttl)
	}

	// SETTINGS clause.
	if len(q.settings) > 0 {
		buf.WriteString(" SETTINGS ")
		for i, s := range q.settings {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
		}
	}

	return buf.String(), nil, nil
}

// resolveColumnType determines the ClickHouse column type for a field.
func (q *CreateTableQuery) resolveColumnType(f *schema.Field) string {
	// Explicit SQL type always wins.
	if f.Options.SQLType != "" {
		return f.Options.SQLType
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

// DropTableQuery builds ClickHouse DROP TABLE statements.
type DropTableQuery struct {
	db       *ClickHouseDB
	table    *schema.Table
	model    any
	ifExists bool
	err      error
}

// NewDropTable creates a DROP TABLE query for the given model.
func (db *ClickHouseDB) NewDropTable(model any) *DropTableQuery {
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
