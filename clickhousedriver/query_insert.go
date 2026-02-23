package clickhousedriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// InsertQuery builds ClickHouse INSERT statements.
// ClickHouse does not support RETURNING, so there is no Scan method.
type InsertQuery struct {
	db          *ClickHouseDB
	table       *schema.Table
	model       any
	columns     []string // explicit columns
	values      [][]any  // explicit values (for manual inserts)
	useMultiRow bool     // force multi-row VALUES statement instead of prepared-statement loop
	err         error
}

// NewInsert creates an INSERT query.
// model can be a struct pointer or a pointer to a slice (for bulk insert).
func (db *ClickHouseDB) NewInsert(model any) *InsertQuery {
	q := &InsertQuery{
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

// Column specifies which columns to insert.
func (q *InsertQuery) Column(columns ...string) *InsertQuery {
	q.columns = append(q.columns, columns...)
	return q
}

// Value adds explicit values (for manual inserts without model data).
func (q *InsertQuery) Value(values ...any) *InsertQuery {
	q.values = append(q.values, values)
	return q
}

// MultiRow forces the insert to use a single multi-row VALUES statement
// instead of a prepared statement loop.
func (q *InsertQuery) MultiRow() *InsertQuery {
	q.useMultiRow = true
	return q
}

// Build generates the SQL and args.
func (q *InsertQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	dialect := q.db.dialect

	buf.WriteString("INSERT INTO ")
	buf.WriteString(dialect.Quote(q.table.Name))

	// Determine fields to insert.
	fields := q.insertableFields()

	args := make([]any, 0, len(fields))

	// Determine columns.
	columns := q.columns
	if len(columns) == 0 && len(q.values) == 0 {
		columns = make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.Options.Column
		}
	}

	// Write column list.
	buf.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(dialect.Quote(col))
	}
	buf.WriteString(")")

	// Write VALUES.
	buf.WriteString(" VALUES ")

	if len(q.values) > 0 {
		// Explicit values provided.
		for rowIdx, row := range q.values {
			if rowIdx > 0 {
				buf.WriteString(", ")
			}
			_ = buf.WriteByte('(')
			for colIdx, val := range row {
				if colIdx > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("?")
				args = append(args, val)
			}
			_ = buf.WriteByte(')')
		}
	} else {
		// Extract values from model.
		rows, err := q.extractModelValues(fields)
		if err != nil {
			return "", nil, err
		}
		for rowIdx, row := range rows {
			if rowIdx > 0 {
				buf.WriteString(", ")
			}
			_ = buf.WriteByte('(')
			for colIdx, val := range row {
				if colIdx > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("?")
				args = append(args, val)
			}
			_ = buf.WriteByte(')')
		}
	}

	return buf.String(), args, nil
}

// buildInsertHookContext creates a hook.QueryContext for insert operations.
func (q *InsertQuery) buildInsertHookContext() *hook.QueryContext {
	var modelType reflect.Type
	if q.table != nil {
		modelType = q.table.ModelType
	}
	tableName := ""
	if q.table != nil {
		tableName = q.table.Name
	}
	return &hook.QueryContext{
		Operation: hook.OpInsert,
		Table:     tableName,
		ModelType: modelType,
	}
}

// Exec executes the INSERT.
func (q *InsertQuery) Exec(ctx context.Context) (driver.Result, error) {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildInsertHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("clickhousedriver: insert denied by hook")
		}
	}

	// For bulk inserts (slice models), use prepared-statement loop by default.
	if !q.useMultiRow && len(q.values) == 0 {
		val := reflect.ValueOf(q.model)
		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Kind() == reflect.Slice && val.Len() > 0 {
			return q.execPrepared(ctx, qc)
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}

	// Populate raw query info into QueryContext.
	if qc != nil {
		qc.RawQuery = query
		qc.RawArgs = args
	}

	res, err := q.db.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	// Run post-mutation hooks.
	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// execPrepared executes a bulk insert using a prepared statement loop
// within a transaction.
func (q *InsertQuery) execPrepared(ctx context.Context, qc *hook.QueryContext) (driver.Result, error) {
	fields := q.insertableFields()

	singleRowSQL, err := q.buildSingleRowInsert(fields)
	if err != nil {
		return nil, err
	}

	if qc != nil {
		qc.RawQuery = singleRowSQL
	}

	// If already in a transaction, prepare and execute directly.
	if q.db.txConn != nil {
		result, execErr := q.execPreparedWith(ctx, q.db, fields, singleRowSQL)
		if execErr != nil {
			return nil, execErr
		}
		if q.db.hooks != nil && qc != nil {
			if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Otherwise, create a transaction for atomicity.
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: bulk insert begin tx: %w", err)
	}

	stx := &ClickHouseTx{db: q.db, tx: tx}
	txdb := stx.txDB()

	result, execErr := q.execPreparedWith(ctx, txdb, fields, singleRowSQL)
	if execErr != nil {
		_ = tx.Rollback()
		return nil, execErr
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("clickhousedriver: bulk insert commit: %w", err)
	}

	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// execPreparedWith executes a prepared-statement bulk insert using the given db.
func (q *InsertQuery) execPreparedWith(ctx context.Context, db *ClickHouseDB, fields []*schema.Field, query string) (driver.Result, error) {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	n := val.Len()
	rowArgs := make([]any, len(fields))
	var totalAffected int64

	for i := 0; i < n; i++ {
		elem := val.Index(i)
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}

		for j, f := range fields {
			fv := elem
			for _, idx := range f.GoIndex {
				fv = fv.Field(idx)
			}
			rowArgs[j] = fv.Interface()
		}

		res, err := stmt.Exec(ctx, rowArgs...)
		if err != nil {
			return nil, fmt.Errorf("clickhousedriver: bulk insert row %d: %w", i, err)
		}
		affected, _ := res.RowsAffected()
		totalAffected += affected
	}

	return &bulkResult{rowsAffected: totalAffected}, nil
}

// buildSingleRowInsert generates a single-row INSERT statement for prepared-statement use.
func (q *InsertQuery) buildSingleRowInsert(fields []*schema.Field) (string, error) {
	if q.err != nil {
		return "", q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	dialect := q.db.dialect

	buf.WriteString("INSERT INTO ")
	buf.WriteString(dialect.Quote(q.table.Name))

	columns := q.columns
	if len(columns) == 0 {
		columns = make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.Options.Column
		}
	}

	buf.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(dialect.Quote(col))
	}
	buf.WriteString(") VALUES (")
	for i := range columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("?")
	}
	buf.WriteString(")")

	return buf.String(), nil
}

// bulkResult implements driver.Result for prepared-statement bulk inserts.
type bulkResult struct {
	rowsAffected int64
}

func (r *bulkResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }
func (r *bulkResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("clickhousedriver: LastInsertId not available for bulk insert")
}

// insertableFields returns the fields eligible for INSERT.
// It excludes ScanOnly fields and AutoIncrement fields.
func (q *InsertQuery) insertableFields() []*schema.Field {
	columnSet := make(map[string]bool, len(q.columns))
	for _, c := range q.columns {
		columnSet[c] = true
	}

	var fields []*schema.Field
	for _, f := range q.table.Fields {
		if f.Options.ScanOnly {
			continue
		}
		if f.Options.AutoIncrement {
			continue
		}
		if len(columnSet) > 0 && !columnSet[f.Options.Column] {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

// extractModelValues extracts field values from the model.
func (q *InsertQuery) extractModelValues(fields []*schema.Field) ([][]any, error) {
	val := reflect.ValueOf(q.model)

	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("clickhousedriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		n := val.Len()
		if n == 0 {
			return nil, fmt.Errorf("clickhousedriver: empty slice for bulk insert")
		}
		rows := make([][]any, n)
		for i := 0; i < n; i++ {
			elem := val.Index(i)
			for elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			row, err := extractFieldValues(elem, fields)
			if err != nil {
				return nil, err
			}
			rows[i] = row
		}
		return rows, nil
	}

	if val.Kind() == reflect.Struct {
		row, err := extractFieldValues(val, fields)
		if err != nil {
			return nil, err
		}
		return [][]any{row}, nil
	}

	return nil, fmt.Errorf("clickhousedriver: unsupported model type %v", val.Kind())
}

// extractFieldValues extracts field values from a struct value using field index chains.
func extractFieldValues(structVal reflect.Value, fields []*schema.Field) ([]any, error) {
	values := make([]any, len(fields))
	for i, f := range fields {
		fv := structVal
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}
		values[i] = fv.Interface()
	}
	return values, nil
}
