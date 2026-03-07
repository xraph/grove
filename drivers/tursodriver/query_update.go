package tursodriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// setClause represents a raw SET expression with optional arguments.
type setClause struct {
	expr string
	args []any
}

// UpdateQuery builds Turso/libSQL UPDATE statements.
type UpdateQuery struct {
	db         *TursoDB
	table      *schema.Table
	model      any
	setClauses []setClause
	wheres     []whereClause
	columns    []string // If set, only update these columns from the model
	omitZero   bool     // Skip zero-value fields
	returning  []string
	err        error
}

// NewUpdate creates an UPDATE query.
func (db *TursoDB) NewUpdate(model any) *UpdateQuery {
	q := &UpdateQuery{
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

// Set adds a raw SET expression (e.g., "name = ?", "Alice").
func (q *UpdateQuery) Set(expr string, args ...any) *UpdateQuery {
	q.setClauses = append(q.setClauses, setClause{expr: expr, args: args})
	return q
}

// Column limits which columns to update from the model.
func (q *UpdateQuery) Column(columns ...string) *UpdateQuery {
	q.columns = append(q.columns, columns...)
	return q
}

// OmitZero skips fields with zero values when building SET from model.
func (q *UpdateQuery) OmitZero() *UpdateQuery {
	q.omitZero = true
	return q
}

// Where adds a WHERE clause.
func (q *UpdateQuery) Where(query string, args ...any) *UpdateQuery {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: "AND"})
	return q
}

// WherePK adds WHERE pk = ? using model's primary key values.
func (q *UpdateQuery) WherePK() *UpdateQuery {
	if q.err != nil {
		return q
	}
	if q.table == nil {
		q.err = fmt.Errorf("tursodriver: table not resolved")
		return q
	}
	if len(q.table.PKFields) == 0 {
		q.err = fmt.Errorf("tursodriver: model has no primary key fields")
		return q
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			q.err = fmt.Errorf("tursodriver: nil model pointer for WherePK")
			return q
		}
		val = val.Elem()
	}

	for _, pkField := range q.table.PKFields {
		fv := val
		for _, idx := range pkField.GoIndex {
			fv = fv.Field(idx)
		}
		q.wheres = append(q.wheres, whereClause{
			query: q.db.dialect.Quote(pkField.Options.Column) + " = ?",
			args:  []any{fv.Interface()},
			sep:   "AND",
		})
	}
	return q
}

// Returning adds RETURNING columns.
func (q *UpdateQuery) Returning(columns ...string) *UpdateQuery {
	q.returning = append(q.returning, columns...)
	return q
}

// Build generates the SQL and args.
func (q *UpdateQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	args := make([]any, 0, len(q.table.Fields)+len(q.wheres))
	dialect := q.db.dialect

	buf.WriteString("UPDATE ")
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" SET ")

	if len(q.setClauses) > 0 {
		// Use explicit SET clauses.
		for i, sc := range q.setClauses {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(sc.expr)
			args = append(args, sc.args...)
		}
	} else {
		// Build SET from model fields.
		fields := q.updatableFields()
		val := reflect.ValueOf(q.model)
		for val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		first := true
		for _, f := range fields {
			fv := val
			for _, idx := range f.GoIndex {
				fv = fv.Field(idx)
			}

			// Skip zero values if OmitZero is set.
			if q.omitZero && fv.IsZero() {
				continue
			}

			if !first {
				buf.WriteString(", ")
			}
			first = false
			buf.WriteString(dialect.Quote(f.Options.Column))
			buf.WriteString(" = ?")
			args = append(args, fv.Interface())
		}

		if first {
			return "", nil, fmt.Errorf("tursodriver: no columns to update")
		}
	}

	// WHERE clauses.
	if len(q.wheres) > 0 {
		buf.WriteString(" WHERE ")
		for i, w := range q.wheres {
			if i > 0 {
				_ = buf.WriteByte(' ')
				buf.WriteString(w.sep)
				_ = buf.WriteByte(' ')
			}
			buf.WriteString(w.query)
			args = append(args, w.args...)
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

// buildUpdateHookContext creates a hook.QueryContext for update operations.
func (q *UpdateQuery) buildUpdateHookContext() *hook.QueryContext {
	var modelType reflect.Type
	if q.table != nil {
		modelType = q.table.ModelType
	}
	tableName := ""
	if q.table != nil {
		tableName = q.table.Name
	}
	return &hook.QueryContext{
		Operation: hook.OpUpdate,
		Table:     tableName,
		ModelType: modelType,
	}
}

// Exec executes the UPDATE.
func (q *UpdateQuery) Exec(ctx context.Context) (driver.Result, error) {
	qc := q.buildUpdateHookContext()

	// Run model BeforeUpdate hooks.
	if err := hook.RunModelBeforeUpdate(ctx, qc, q.model); err != nil {
		return nil, err
	}

	// Run operation-level pre-mutation hooks.
	if q.db.hooks != nil {
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("tursodriver: update denied by hook")
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}

	// Populate raw query info into QueryContext.
	qc.RawQuery = query
	qc.RawArgs = args

	res, err := q.db.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	// Run operation-level post-mutation hooks.
	if q.db.hooks != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, res); err != nil {
			return nil, err
		}
	}

	// Run model AfterUpdate hooks.
	if err := hook.RunModelAfterUpdate(ctx, qc, q.model); err != nil {
		return nil, err
	}

	return res, nil
}

// Scan executes the UPDATE with RETURNING and scans results into dest.
func (q *UpdateQuery) Scan(ctx context.Context, dest ...any) error {
	qc := q.buildUpdateHookContext()

	// Run model BeforeUpdate hooks.
	if err := hook.RunModelBeforeUpdate(ctx, qc, q.model); err != nil {
		return err
	}

	// Run operation-level pre-mutation hooks.
	if q.db.hooks != nil {
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("tursodriver: update denied by hook")
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return err
	}

	// Populate raw query info into QueryContext.
	qc.RawQuery = query
	qc.RawArgs = args

	row := q.db.QueryRow(ctx, query, args...)
	if err := row.Scan(dest...); err != nil {
		return err
	}

	// Run operation-level post-mutation hooks.
	if q.db.hooks != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, dest); err != nil {
			return err
		}
	}

	// Run model AfterUpdate hooks.
	if err := hook.RunModelAfterUpdate(ctx, qc, q.model); err != nil {
		return err
	}

	return nil
}

// updatableFields returns the fields eligible for UPDATE.
// It excludes ScanOnly, AutoIncrement, and PK fields.
// If explicit columns were specified, it filters to only those columns.
func (q *UpdateQuery) updatableFields() []*schema.Field {
	columnSet := make(map[string]bool, len(q.columns))
	for _, c := range q.columns {
		columnSet[c] = true
	}

	pkSet := make(map[string]bool, len(q.table.PKFields))
	for _, pk := range q.table.PKFields {
		pkSet[pk.Options.Column] = true
	}

	var fields []*schema.Field
	for _, f := range q.table.Fields {
		// Skip ScanOnly fields.
		if f.Options.ScanOnly {
			continue
		}
		// Skip AutoIncrement fields.
		if f.Options.AutoIncrement {
			continue
		}
		// Skip PK fields (don't update primary keys).
		if pkSet[f.Options.Column] {
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
