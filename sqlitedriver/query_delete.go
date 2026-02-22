package sqlitedriver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// DeleteQuery builds SQLite DELETE statements.
type DeleteQuery struct {
	db          *SqliteDB
	table       *schema.Table
	model       any
	wheres      []whereClause
	returning   []string
	forceDelete bool // bypass soft delete
	args        []any
	err         error
}

// NewDelete creates a DELETE query.
func (db *SqliteDB) NewDelete(model any) *DeleteQuery {
	q := &DeleteQuery{
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

// Where adds a WHERE clause.
func (q *DeleteQuery) Where(query string, args ...any) *DeleteQuery {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: "AND"})
	return q
}

// WhereOr adds an OR WHERE clause.
func (q *DeleteQuery) WhereOr(query string, args ...any) *DeleteQuery {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: "OR"})
	return q
}

// WherePK adds WHERE pk = ? using model's primary key values.
func (q *DeleteQuery) WherePK() *DeleteQuery {
	if q.err != nil {
		return q
	}
	if q.table == nil {
		q.err = fmt.Errorf("sqlitedriver: table not resolved")
		return q
	}
	if len(q.table.PKFields) == 0 {
		q.err = fmt.Errorf("sqlitedriver: model has no primary key fields")
		return q
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			q.err = fmt.Errorf("sqlitedriver: nil model pointer for WherePK")
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
			query: fmt.Sprintf("%s = ?", q.db.dialect.Quote(pkField.Options.Column)),
			args:  []any{fv.Interface()},
			sep:   "AND",
		})
	}
	return q
}

// Returning adds RETURNING columns.
func (q *DeleteQuery) Returning(columns ...string) *DeleteQuery {
	q.returning = append(q.returning, columns...)
	return q
}

// ForceDelete bypasses soft delete, performing a real DELETE even if the model
// has a soft_delete field.
func (q *DeleteQuery) ForceDelete() *DeleteQuery {
	q.forceDelete = true
	return q
}

// Build generates the SQL and args.
func (q *DeleteQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	// If the model has a soft delete field and ForceDelete is not set,
	// generate an UPDATE SET deleted_at = DATETIME('now') instead.
	if q.table.SoftDelete != nil && !q.forceDelete {
		return q.buildSoftDelete()
	}

	return q.buildHardDelete()
}

// buildHardDelete generates a real DELETE FROM statement.
func (q *DeleteQuery) buildHardDelete() (string, []any, error) {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	var args []any
	dialect := q.db.dialect

	buf.WriteString("DELETE FROM ")
	buf.WriteString(dialect.Quote(q.table.Name))

	// WHERE clauses.
	if len(q.wheres) > 0 {
		buf.WriteString(" WHERE ")
		for i, w := range q.wheres {
			if i > 0 {
				buf.WriteByte(' ')
				buf.WriteString(w.sep)
				buf.WriteByte(' ')
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

// buildSoftDelete generates an UPDATE SET <soft_delete_col> = DATETIME('now') statement.
func (q *DeleteQuery) buildSoftDelete() (string, []any, error) {
	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	var args []any
	dialect := q.db.dialect

	buf.WriteString("UPDATE ")
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" SET ")
	buf.WriteString(dialect.Quote(q.table.SoftDelete.Options.Column))
	buf.WriteString(" = DATETIME('now')")

	// WHERE clauses.
	if len(q.wheres) > 0 {
		buf.WriteString(" WHERE ")
		for i, w := range q.wheres {
			if i > 0 {
				buf.WriteByte(' ')
				buf.WriteString(w.sep)
				buf.WriteByte(' ')
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

// buildDeleteHookContext creates a hook.QueryContext for delete operations.
func (q *DeleteQuery) buildDeleteHookContext() *hook.QueryContext {
	var modelType reflect.Type
	if q.table != nil {
		modelType = q.table.ModelType
	}
	tableName := ""
	if q.table != nil {
		tableName = q.table.Name
	}
	return &hook.QueryContext{
		Operation: hook.OpDelete,
		Table:     tableName,
		ModelType: modelType,
	}
}

// Exec executes the DELETE (or soft-delete UPDATE).
func (q *DeleteQuery) Exec(ctx context.Context) (driver.Result, error) {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildDeleteHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("sqlitedriver: delete denied by hook")
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

// Scan executes the DELETE with RETURNING and scans results into dest.
func (q *DeleteQuery) Scan(ctx context.Context, dest ...any) error {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildDeleteHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("sqlitedriver: delete denied by hook")
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
