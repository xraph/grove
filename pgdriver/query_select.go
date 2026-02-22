package pgdriver

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/scan"
	"github.com/xraph/grove/schema"
	"github.com/xraph/grove/stream"
)

// SelectQuery builds PostgreSQL SELECT statements.
type SelectQuery struct {
	baseQuery
	columns         []string
	columnExprs     []columnExpr
	distinctOn      []string
	joins           []joinClause
	orderExprs      []string
	groupExprs      []string
	having          []whereClause
	limit           int
	offset          int
	forUpdate       bool
	forShare        bool
	forUpdateTables []string
	relations       []string // relation names to eager load
	withDeleted     bool     // include soft-deleted rows
	tableExpr       string   // raw FROM expression (overrides model table)
	tableExprArgs   []any    // args for tableExpr placeholders
}

// columnExpr represents a raw column expression with optional args.
type columnExpr struct {
	expr string
	args []any
}

// joinClause represents a JOIN clause.
type joinClause struct {
	joinType string // "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"
	table    string
	on       string
	args     []any
}

// NewSelect creates a new SELECT query. model can be:
//   - *[]User (slice pointer for multi-row)
//   - *User (struct pointer for single row)
//   - (*User)(nil) (nil pointer for table reference without binding)
func (db *PgDB) NewSelect(model ...any) *SelectQuery {
	q := &SelectQuery{}
	q.db = db

	if len(model) > 0 && model[0] != nil {
		q.model = model[0]
		table, err := resolveTable(model[0])
		if err != nil {
			q.err = err
		} else {
			q.table = table
		}
	}

	return q
}

// Column adds specific columns to select. If not called, selects all fields.
func (q *SelectQuery) Column(columns ...string) *SelectQuery {
	q.columns = append(q.columns, columns...)
	return q
}

// ColumnExpr adds a raw column expression.
func (q *SelectQuery) ColumnExpr(expr string, args ...any) *SelectQuery {
	q.columnExprs = append(q.columnExprs, columnExpr{expr: expr, args: args})
	return q
}

// Where adds an AND WHERE clause.
func (q *SelectQuery) Where(query string, args ...any) *SelectQuery {
	q.addWhere("AND", query, args)
	return q
}

// WhereOr adds an OR WHERE clause.
func (q *SelectQuery) WhereOr(query string, args ...any) *SelectQuery {
	q.addWhere("OR", query, args)
	return q
}

// WhereArray adds an AND WHERE clause using a PostgreSQL array operator.
// It generates SQL like: "col" op ($N), e.g. "role" = ANY($1).
// The op parameter should be a SQL operator such as "= ANY" or "<> ALL".
// The arr value is added directly to the query args (pass a Go slice).
func (q *SelectQuery) WhereArray(col string, op string, arr any) *SelectQuery {
	argIdx := countArgs(q.wheres) + 1
	placeholder := q.db.dialect.Placeholder(argIdx)
	clause := q.db.dialect.Quote(col) + " " + op + " (" + placeholder + ")"
	q.addWhere("AND", clause, []any{arr})
	return q
}

// TableExpr sets the FROM clause to a raw SQL expression instead of deriving
// it from the model's table name. This is useful for queries against functions,
// CTEs, or subqueries, e.g.:
//
//	db.NewSelect().TableExpr("generate_series(1, 10) AS s(n)")
func (q *SelectQuery) TableExpr(expr string, args ...any) *SelectQuery {
	q.tableExpr = expr
	q.tableExprArgs = args
	return q
}

// Lateral adds a JOIN LATERAL subquery. It generates:
//
//	JOIN LATERAL (subquery) AS alias ON true
func (q *SelectQuery) Lateral(subquery string, alias string, args ...any) *SelectQuery {
	table := "(" + subquery + ") AS " + q.db.dialect.Quote(alias)
	q.joins = append(q.joins, joinClause{
		joinType: "JOIN LATERAL",
		table:    table,
		on:       "true",
		args:     args,
	})
	return q
}

// WherePK adds WHERE conditions for the model's primary key fields.
// The user must have set the model so that PKFields are available.
// It generates conditions like "table"."pk_col" = $N using sequential placeholders.
func (q *SelectQuery) WherePK() *SelectQuery {
	if q.table == nil {
		q.err = fmt.Errorf("pgdriver: WherePK requires a model with table metadata")
		return q
	}
	if len(q.table.PKFields) == 0 {
		q.err = fmt.Errorf("pgdriver: model has no primary key fields")
		return q
	}

	for _, pk := range q.table.PKFields {
		var col string
		if q.table.Alias != "" {
			col = q.db.dialect.Quote(q.table.Alias) + "." + q.db.dialect.Quote(pk.Options.Column)
		} else {
			col = q.db.dialect.Quote(pk.Options.Column)
		}

		// Count how many args we already have queued to determine the placeholder index.
		argIdx := countArgs(q.wheres) + 1

		// Try to extract the PK value from the model.
		pkVal, ok := extractFieldValue(q.model, pk)
		if ok {
			clause := col + " = " + q.db.dialect.Placeholder(argIdx)
			q.addWhere("AND", clause, []any{pkVal})
		} else {
			// If we can't extract the value (nil pointer model), generate placeholder with no arg.
			clause := col + " = " + q.db.dialect.Placeholder(argIdx)
			q.addWhere("AND", clause, nil)
		}
	}

	return q
}

// Join adds a JOIN clause.
func (q *SelectQuery) Join(joinType, table, on string, args ...any) *SelectQuery {
	q.joins = append(q.joins, joinClause{
		joinType: joinType,
		table:    table,
		on:       on,
		args:     args,
	})
	return q
}

// OrderExpr adds ORDER BY expression.
func (q *SelectQuery) OrderExpr(expr string) *SelectQuery {
	q.orderExprs = append(q.orderExprs, expr)
	return q
}

// GroupExpr adds GROUP BY expression.
func (q *SelectQuery) GroupExpr(expr string) *SelectQuery {
	q.groupExprs = append(q.groupExprs, expr)
	return q
}

// Having adds HAVING clause.
func (q *SelectQuery) Having(query string, args ...any) *SelectQuery {
	q.having = append(q.having, whereClause{query: query, args: args, sep: "AND"})
	return q
}

// Limit sets LIMIT.
func (q *SelectQuery) Limit(n int) *SelectQuery {
	q.limit = n
	return q
}

// Offset sets OFFSET.
func (q *SelectQuery) Offset(n int) *SelectQuery {
	q.offset = n
	return q
}

// DistinctOn adds DISTINCT ON (columns).
func (q *SelectQuery) DistinctOn(columns ...string) *SelectQuery {
	q.distinctOn = append(q.distinctOn, columns...)
	return q
}

// ForUpdate adds FOR UPDATE.
func (q *SelectQuery) ForUpdate(tables ...string) *SelectQuery {
	q.forUpdate = true
	q.forUpdateTables = append(q.forUpdateTables, tables...)
	return q
}

// ForShare adds FOR SHARE.
func (q *SelectQuery) ForShare() *SelectQuery {
	q.forShare = true
	return q
}

// Relation marks a relation for eager loading.
func (q *SelectQuery) Relation(name string) *SelectQuery {
	q.relations = append(q.relations, name)
	return q
}

// WithDeleted includes soft-deleted rows in the result set.
// By default, models with a soft_delete field automatically filter out
// rows where the soft delete column is not NULL.
func (q *SelectQuery) WithDeleted() *SelectQuery {
	q.withDeleted = true
	return q
}

// applySoftDeleteFilter adds a WHERE clause filtering out soft-deleted rows
// if the model has a soft_delete field and WithDeleted was not called.
// This is idempotent — it tracks whether it has already been applied.
func (q *SelectQuery) applySoftDeleteFilter() {
	if q.table == nil || q.table.SoftDelete == nil || q.withDeleted {
		return
	}

	sdCol := q.db.dialect.Quote(q.table.SoftDelete.Options.Column)
	if q.table.Alias != "" {
		sdCol = q.db.dialect.Quote(q.table.Alias) + "." + sdCol
	}
	clause := sdCol + " IS NULL"

	// Check if we already added this filter (idempotency for BuildCount reuse).
	for _, w := range q.wheres {
		if w.query == clause {
			return
		}
	}

	q.addWhere("AND", clause, nil)
}

// Build generates the SQL string and args without executing.
func (q *SelectQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	q.args = q.args[:0] // reset args

	buf.WriteString("SELECT ")

	// DISTINCT ON
	if len(q.distinctOn) > 0 {
		buf.WriteString("DISTINCT ON (")
		for i, col := range q.distinctOn {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}
		buf.WriteString(") ")
	}

	// Columns
	hasColumns := false

	if len(q.columns) > 0 || len(q.columnExprs) > 0 {
		// Custom columns
		idx := 0
		for _, col := range q.columns {
			if idx > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
			idx++
		}
		for _, ce := range q.columnExprs {
			if idx > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(ce.expr)
			q.args = append(q.args, ce.args...)
			idx++
		}
		hasColumns = true
	} else if q.table != nil && len(q.table.Fields) > 0 {
		// Build column list from table fields
		for i, f := range q.table.Fields {
			if i > 0 {
				buf.WriteString(", ")
			}
			if q.table.Alias != "" {
				buf.WriteString(q.db.dialect.Quote(q.table.Alias))
				_ = buf.WriteByte('.')
			}
			buf.WriteString(q.db.dialect.Quote(f.Options.Column))
		}
		hasColumns = true
	}

	if !hasColumns {
		buf.WriteString("*")
	}

	// FROM
	if q.tableExpr != "" {
		buf.WriteString(" FROM ")
		buf.WriteString(q.tableExpr)
		q.args = append(q.args, q.tableExprArgs...)
	} else if q.table != nil {
		buf.WriteString(" FROM ")
		buf.WriteString(q.db.dialect.Quote(q.table.Name))
		if q.table.Alias != "" {
			buf.WriteString(" AS ")
			buf.WriteString(q.db.dialect.Quote(q.table.Alias))
		}
	}

	// JOINs
	for _, j := range q.joins {
		_ = buf.WriteByte(' ')
		buf.WriteString(j.joinType)
		_ = buf.WriteByte(' ')
		buf.WriteString(j.table)
		if j.on != "" {
			buf.WriteString(" ON ")
			buf.WriteString(j.on)
		}
		q.args = append(q.args, j.args...)
	}

	// Auto-add soft delete filter if applicable.
	q.applySoftDeleteFilter()

	// WHERE
	q.appendWheres(buf)

	// GROUP BY
	if len(q.groupExprs) > 0 {
		buf.WriteString(" GROUP BY ")
		for i, expr := range q.groupExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr)
		}
	}

	// HAVING
	if len(q.having) > 0 {
		buf.WriteString(" HAVING ")
		for i, h := range q.having {
			if i > 0 {
				_ = buf.WriteByte(' ')
				buf.WriteString(h.sep)
				_ = buf.WriteByte(' ')
			}
			buf.WriteString(h.query)
			q.args = append(q.args, h.args...)
		}
	}

	// ORDER BY
	if len(q.orderExprs) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, expr := range q.orderExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr)
		}
	}

	// LIMIT
	if q.limit > 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.Itoa(q.limit))
	}

	// OFFSET
	if q.offset > 0 {
		buf.WriteString(" OFFSET ")
		buf.WriteString(strconv.Itoa(q.offset))
	}

	// FOR UPDATE
	if q.forUpdate {
		buf.WriteString(" FOR UPDATE")
		if len(q.forUpdateTables) > 0 {
			buf.WriteString(" OF ")
			for i, t := range q.forUpdateTables {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(t)
			}
		}
	}

	// FOR SHARE
	if q.forShare {
		buf.WriteString(" FOR SHARE")
	}

	return buf.String(), q.args, nil
}

// BuildCount generates a SELECT COUNT(*) query string and args.
func (q *SelectQuery) BuildCount() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	q.args = q.args[:0]

	buf.WriteString("SELECT COUNT(*)")

	// FROM
	if q.tableExpr != "" {
		buf.WriteString(" FROM ")
		buf.WriteString(q.tableExpr)
		q.args = append(q.args, q.tableExprArgs...)
	} else if q.table != nil {
		buf.WriteString(" FROM ")
		buf.WriteString(q.db.dialect.Quote(q.table.Name))
		if q.table.Alias != "" {
			buf.WriteString(" AS ")
			buf.WriteString(q.db.dialect.Quote(q.table.Alias))
		}
	}

	// JOINs
	for _, j := range q.joins {
		_ = buf.WriteByte(' ')
		buf.WriteString(j.joinType)
		_ = buf.WriteByte(' ')
		buf.WriteString(j.table)
		if j.on != "" {
			buf.WriteString(" ON ")
			buf.WriteString(j.on)
		}
		q.args = append(q.args, j.args...)
	}

	// Auto-add soft delete filter if applicable.
	q.applySoftDeleteFilter()

	// WHERE
	q.appendWheres(buf)

	// GROUP BY
	if len(q.groupExprs) > 0 {
		buf.WriteString(" GROUP BY ")
		for i, expr := range q.groupExprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr)
		}
	}

	// HAVING
	if len(q.having) > 0 {
		buf.WriteString(" HAVING ")
		for i, h := range q.having {
			if i > 0 {
				_ = buf.WriteByte(' ')
				buf.WriteString(h.sep)
				_ = buf.WriteByte(' ')
			}
			buf.WriteString(h.query)
			q.args = append(q.args, h.args...)
		}
	}

	return buf.String(), q.args, nil
}

// Scan executes the query and scans results into the model.
func (q *SelectQuery) Scan(ctx context.Context, dest ...any) error {
	// Build the QueryContext for hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		var modelType reflect.Type
		if q.table != nil {
			modelType = q.table.ModelType
		}
		tableName := ""
		if q.table != nil {
			tableName = q.table.Name
		}
		qc = &hook.QueryContext{
			Operation: hook.OpSelect,
			Table:     tableName,
			ModelType: modelType,
		}

		// Run pre-query hooks.
		result, err := q.db.hooks.RunPreQuery(ctx, qc)
		if err != nil {
			return err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("pgdriver: query denied by hook")
		}

		// Inject extra filters from hooks as WHERE clauses.
		if result != nil && len(result.Filters) > 0 {
			for _, f := range result.Filters {
				if f.Clause != "" {
					q.addWhere("AND", f.Clause, f.Args)
				}
			}
		}
	}

	sqlStr, args, err := q.Build()
	if err != nil {
		return err
	}

	// Populate raw query info into QueryContext for post-query hooks.
	if qc != nil {
		qc.RawQuery = sqlStr
		qc.RawArgs = args
	}

	// Determine scan target.
	target := q.model
	if len(dest) > 0 {
		target = dest[0]
	}

	if target == nil {
		return fmt.Errorf("pgdriver: Scan requires a destination; pass a model to NewSelect or provide dest")
	}

	// Resolve the table for scanning.
	table := q.table
	if table == nil {
		table, err = resolveTable(target)
		if err != nil {
			return err
		}
	}

	// Use reflect to determine if we scan multiple rows or a single row.
	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		innerType := targetType.Elem()
		if innerType.Kind() == reflect.Slice {
			// Slice pointer: multi-row scan.
			rows, qerr := q.db.Query(ctx, sqlStr, args...)
			if qerr != nil {
				return qerr
			}
			defer func() { _ = rows.Close() }()
			if err := scan.ScanRows(rows, target, table); err != nil {
				return err
			}
			// Load eager relations after scanning.
			if err := q.loadRelations(ctx, target); err != nil {
				return err
			}
			// Run post-query hooks.
			if q.db.hooks != nil && qc != nil {
				if err := q.db.hooks.RunPostQuery(ctx, qc, target); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Single struct pointer: single-row scan.
	row := q.db.QueryRow(ctx, sqlStr, args...)
	if err := scan.ScanRow(row, target, table); err != nil {
		return err
	}
	// Load eager relations after scanning.
	if err := q.loadRelations(ctx, target); err != nil {
		return err
	}
	// Run post-query hooks.
	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostQuery(ctx, qc, target); err != nil {
			return err
		}
	}
	return nil
}

// Count executes a SELECT COUNT(*) and returns the count.
func (q *SelectQuery) Count(ctx context.Context) (int64, error) {
	sqlStr, args, err := q.BuildCount()
	if err != nil {
		return 0, err
	}

	var count int64
	row := q.db.QueryRow(ctx, sqlStr, args...)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("pgdriver: count: %w", err)
	}
	return count, nil
}

// extractFieldValue extracts the value of a schema.Field from a model instance.
// Returns the value and true if successful, or nil and false if not.
func extractFieldValue(model any, field *schema.Field) (any, bool) {
	if model == nil {
		return nil, false
	}

	v := reflect.ValueOf(model)
	// Dereference pointers.
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	// If it's a slice, we can't extract a single field value.
	if v.Kind() == reflect.Slice {
		return nil, false
	}

	if v.Kind() != reflect.Struct {
		return nil, false
	}

	fv := v
	for _, idx := range field.GoIndex {
		if fv.Kind() != reflect.Struct || idx >= fv.NumField() {
			return nil, false
		}
		fv = fv.Field(idx)
	}

	return fv.Interface(), true
}

// defaultStreamFetchSize is the default number of rows fetched per batch
// when using server-side cursor streaming.
const defaultStreamFetchSize = 100

// Stream executes the query using a PG server-side cursor and returns a
// stream.Stream that yields one model instance at a time. The stream
// transparently fetches rows in batches of 100 from the cursor.
//
// The stream owns a dedicated transaction; it is committed when the
// stream is closed. Always defer stream.Close().
//
//	s, err := db.NewSelect(&users).Where("active = true").Stream(ctx)
//	if err != nil { ... }
//	defer s.Close()
//	for s.Next(ctx) {
//	    user := s.Value()
//	}
func (q *SelectQuery) Stream(ctx context.Context) (*stream.Stream[any], error) {
	return q.StreamBatch(ctx, defaultStreamFetchSize)
}

// StreamBatch executes the query using a PG server-side cursor and returns
// a stream.Stream that yields one model instance at a time. Rows are
// fetched from the server in batches of fetchSize.
//
// The stream owns a dedicated transaction; it is committed when the
// stream is closed. Always defer stream.Close().
func (q *SelectQuery) StreamBatch(ctx context.Context, fetchSize int) (*stream.Stream[any], error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.table == nil {
		return nil, fmt.Errorf("pgdriver: Stream requires a model with table metadata")
	}
	if fetchSize <= 0 {
		fetchSize = defaultStreamFetchSize
	}

	// Run pre-query hooks if configured.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = &hook.QueryContext{
			Operation: hook.OpSelect,
			Table:     q.table.Name,
			ModelType: q.table.ModelType,
		}
		result, err := q.db.hooks.RunPreQuery(ctx, qc)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("pgdriver: query denied by hook")
		}
		if result != nil {
			for _, f := range result.Filters {
				if f.Clause != "" {
					q.addWhere("AND", f.Clause, f.Args)
				}
			}
		}
	}

	sqlStr, args, err := q.Build()
	if err != nil {
		return nil, err
	}

	if qc != nil {
		qc.RawQuery = sqlStr
		qc.RawArgs = args
	}

	// Start a dedicated transaction for the cursor.
	tx, err := q.db.BeginTx(ctx, &driver.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("pgdriver: stream begin tx: %w", err)
	}

	cursor, err := newPgCursor(ctx, tx, sqlStr, args, fetchSize, true)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	// Build the decode function using the table metadata.
	table := q.table
	decode := makeDecodeFunc(table)

	s := stream.New[any](cursor, decode)

	// Attach hooks if configured.
	if q.db.hooks != nil && qc != nil {
		s.WithHooks(&hookAdapter{engine: q.db.hooks}, qc)
	}

	return s, nil
}

// makeDecodeFunc creates a stream.DecodeFunc that scans cursor rows into
// new instances of the model type described by the schema table.
func makeDecodeFunc(table *schema.Table) stream.DecodeFunc[any] {
	var cm *scan.ColumnMap
	var fields []*schema.Field
	var initialized bool

	return func(cursor stream.Cursor) (any, error) {
		// Lazily initialize column mapping on first call.
		if !initialized {
			cols, err := cursor.Columns()
			if err != nil {
				return nil, fmt.Errorf("pgdriver: stream columns: %w", err)
			}
			cm = scan.NewColumnMap(table)
			fields = cm.Resolve(cols)
			initialized = true
		}

		// Create a new instance of the model type.
		elemPtr := reflect.New(table.ModelType)
		elem := elemPtr.Elem()

		ptrs := make([]any, len(fields))
		var discard any
		for i, field := range fields {
			if field != nil {
				ptrs[i] = scan.FieldPtr(elem, field)
			} else {
				ptrs[i] = &discard
			}
		}

		if err := cursor.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("pgdriver: stream scan: %w", err)
		}

		return elemPtr.Interface(), nil
	}
}

// hookAdapter adapts hook.Engine to stream.HookRunner interface.
type hookAdapter struct {
	engine *hook.Engine
}

func (a *hookAdapter) RunStreamRowHook(ctx context.Context, qc any, row any) (int, error) {
	hqc, ok := qc.(*hook.QueryContext)
	if !ok {
		return 0, nil // allow
	}
	return a.engine.RunStreamRowHook(ctx, hqc, row)
}

// countArgs counts the total number of args across all where clauses.
func countArgs(wheres []whereClause) int {
	n := 0
	for _, w := range wheres {
		n += len(w.args)
	}
	return n
}
