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

// DeleteQuery builds ClickHouse DELETE statements.
// Internally, this generates ALTER TABLE ... DELETE WHERE syntax because
// ClickHouse uses mutation-based deletes.
// Note: ClickHouse does not support soft deletes in the same way as traditional
// databases, so this always generates a hard delete (ALTER TABLE ... DELETE).
type DeleteQuery struct {
	db     *ClickHouseDB
	table  *schema.Table
	model  any
	wheres []whereClause
	err    error
}

// NewDelete creates a DELETE query.
func (db *ClickHouseDB) NewDelete(model any) *DeleteQuery {
	q := &DeleteQuery{
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
		q.err = fmt.Errorf("clickhousedriver: table not resolved")
		return q
	}
	if len(q.table.PKFields) == 0 {
		q.err = fmt.Errorf("clickhousedriver: model has no primary key fields")
		return q
	}

	val := reflect.ValueOf(q.model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			q.err = fmt.Errorf("clickhousedriver: nil model pointer for WherePK")
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

// Build generates the SQL and args.
// ClickHouse uses ALTER TABLE ... DELETE WHERE syntax.
func (q *DeleteQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	args := make([]any, 0, len(q.wheres))
	dialect := q.db.dialect

	buf.WriteString("ALTER TABLE ")
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" DELETE")

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

// Exec executes the DELETE (ALTER TABLE ... DELETE WHERE).
func (q *DeleteQuery) Exec(ctx context.Context) (driver.Result, error) {
	qc := q.buildDeleteHookContext()

	// Run model BeforeDelete hooks.
	if err := hook.RunModelBeforeDelete(ctx, qc, q.model); err != nil {
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
			return nil, fmt.Errorf("clickhousedriver: delete denied by hook")
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

	// Run model AfterDelete hooks.
	if err := hook.RunModelAfterDelete(ctx, qc, q.model); err != nil {
		return nil, err
	}

	return res, nil
}
