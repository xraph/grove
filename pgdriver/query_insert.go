package pgdriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// InsertQuery builds PostgreSQL INSERT statements.
type InsertQuery struct {
	db         *PgDB
	table      *schema.Table
	model      any
	columns    []string // explicit columns
	values     [][]any  // explicit values (for manual inserts)
	onConflict string   // ON CONFLICT clause
	setClauses []string // SET expressions for upsert
	setArgs    [][]any  // args for each SET expression
	returning  []string // RETURNING columns
	err        error
}

// NewInsert creates an INSERT query.
// model can be a struct pointer or a pointer to a slice (for bulk insert).
func (db *PgDB) NewInsert(model any) *InsertQuery {
	q := &InsertQuery{
		db:    db,
		model: model,
	}

	table, err := resolveTable(model)
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

// OnConflict adds an ON CONFLICT clause (e.g., "(email) DO UPDATE").
func (q *InsertQuery) OnConflict(clause string) *InsertQuery {
	q.onConflict = clause
	return q
}

// Set adds a SET expression for ON CONFLICT DO UPDATE.
func (q *InsertQuery) Set(expr string, args ...any) *InsertQuery {
	q.setClauses = append(q.setClauses, expr)
	q.setArgs = append(q.setArgs, args)
	return q
}

// Returning adds RETURNING columns.
func (q *InsertQuery) Returning(columns ...string) *InsertQuery {
	q.returning = append(q.returning, columns...)
	return q
}

// Build generates the SQL and args.
func (q *InsertQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	var args []any
	argIdx := 0

	dialect := q.db.dialect

	buf.WriteString("INSERT INTO ")
	buf.WriteString(dialect.Quote(q.table.Name))

	// Determine fields to insert.
	fields := q.insertableFields()

	// Determine columns.
	columns := q.columns
	if len(columns) == 0 && len(q.values) == 0 {
		// Use field columns from the model.
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
			buf.WriteByte('(')
			for colIdx, val := range row {
				if colIdx > 0 {
					buf.WriteString(", ")
				}
				argIdx++
				buf.WriteString(dialect.Placeholder(argIdx))
				args = append(args, val)
			}
			buf.WriteByte(')')
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
			buf.WriteByte('(')
			for colIdx, val := range row {
				if colIdx > 0 {
					buf.WriteString(", ")
				}
				argIdx++
				buf.WriteString(dialect.Placeholder(argIdx))
				args = append(args, val)
			}
			buf.WriteByte(')')
		}
	}

	// ON CONFLICT clause.
	if q.onConflict != "" {
		buf.WriteString(" ON CONFLICT ")
		buf.WriteString(q.onConflict)
	}

	// SET clauses for upsert.
	if len(q.setClauses) > 0 {
		buf.WriteString(" SET ")
		for i, expr := range q.setClauses {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr)
			args = append(args, q.setArgs[i]...)
		}
	}

	// RETURNING clause.
	if len(q.returning) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range q.returning {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(dialect.Quote(col))
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
			return nil, fmt.Errorf("pgdriver: insert denied by hook")
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

// Scan executes the INSERT with RETURNING and scans results into dest.
func (q *InsertQuery) Scan(ctx context.Context, dest ...any) error {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildInsertHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("pgdriver: insert denied by hook")
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return err
	}

	// Populate raw query info into QueryContext.
	if qc != nil {
		qc.RawQuery = query
		qc.RawArgs = args
	}

	row := q.db.QueryRow(ctx, query, args...)
	if err := row.Scan(dest...); err != nil {
		return err
	}

	// Run post-mutation hooks.
	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, dest); err != nil {
			return err
		}
	}

	return nil
}

// insertableFields returns the fields eligible for INSERT.
// It excludes ScanOnly fields and AutoIncrement fields.
// If explicit columns were specified, it filters to only those columns.
func (q *InsertQuery) insertableFields() []*schema.Field {
	columnSet := make(map[string]bool, len(q.columns))
	for _, c := range q.columns {
		columnSet[c] = true
	}

	var fields []*schema.Field
	for _, f := range q.table.Fields {
		// Skip ScanOnly fields.
		if f.Options.ScanOnly {
			continue
		}
		// Skip AutoIncrement fields (database generates the value).
		if f.Options.AutoIncrement {
			continue
		}
		// If explicit columns specified, only include matching fields.
		if len(columnSet) > 0 && !columnSet[f.Options.Column] {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

// extractModelValues extracts field values from the model.
// For a struct pointer, returns a single row.
// For a slice, returns multiple rows.
func (q *InsertQuery) extractModelValues(fields []*schema.Field) ([][]any, error) {
	val := reflect.ValueOf(q.model)

	// Dereference pointers.
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("pgdriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		// Bulk insert from slice.
		n := val.Len()
		if n == 0 {
			return nil, fmt.Errorf("pgdriver: empty slice for bulk insert")
		}
		rows := make([][]any, n)
		for i := 0; i < n; i++ {
			elem := val.Index(i)
			// Dereference pointer elements.
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

	return nil, fmt.Errorf("pgdriver: unsupported model type %v", val.Kind())
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
