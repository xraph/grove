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

// AlterUpdateQuery builds ALTER TABLE ... UPDATE ... WHERE statements.
// This is ClickHouse's mutation-based update mechanism, which is asynchronous
// by default.
type AlterUpdateQuery struct {
	db         *ClickHouseDB
	table      *schema.Table
	model      any
	setClauses []setClause
	wheres     []whereClause
	err        error
}

// NewAlterUpdate creates an ALTER TABLE ... UPDATE query.
func (db *ClickHouseDB) NewAlterUpdate(model any) *AlterUpdateQuery {
	q := &AlterUpdateQuery{
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

// Set adds a SET expression (e.g., "name = ?", "Alice").
func (q *AlterUpdateQuery) Set(expr string, args ...any) *AlterUpdateQuery {
	q.setClauses = append(q.setClauses, setClause{expr: expr, args: args})
	return q
}

// Where adds a WHERE clause.
func (q *AlterUpdateQuery) Where(query string, args ...any) *AlterUpdateQuery {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: "AND"})
	return q
}

// Build generates the SQL and args.
func (q *AlterUpdateQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}
	if len(q.setClauses) == 0 {
		return "", nil, fmt.Errorf("clickhousedriver: ALTER UPDATE requires at least one SET clause")
	}
	if len(q.wheres) == 0 {
		return "", nil, fmt.Errorf("clickhousedriver: ALTER UPDATE requires a WHERE clause")
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	args := make([]any, 0, len(q.setClauses)+len(q.wheres))
	dialect := q.db.dialect

	buf.WriteString("ALTER TABLE ")
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" UPDATE ")

	for i, sc := range q.setClauses {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(sc.expr)
		args = append(args, sc.args...)
	}

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

	return buf.String(), args, nil
}

// Exec executes the ALTER TABLE ... UPDATE statement.
func (q *AlterUpdateQuery) Exec(ctx context.Context) (driver.Result, error) {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildAlterUpdateHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("clickhousedriver: alter update denied by hook")
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}

	if qc != nil {
		qc.RawQuery = query
		qc.RawArgs = args
	}

	res, err := q.db.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *AlterUpdateQuery) buildAlterUpdateHookContext() *hook.QueryContext {
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

// AlterDeleteQuery builds ALTER TABLE ... DELETE WHERE statements.
// This is ClickHouse's mutation-based delete mechanism, which is asynchronous
// by default.
type AlterDeleteQuery struct {
	db     *ClickHouseDB
	table  *schema.Table
	model  any
	wheres []whereClause
	err    error
}

// NewAlterDelete creates an ALTER TABLE ... DELETE query.
func (db *ClickHouseDB) NewAlterDelete(model any) *AlterDeleteQuery {
	q := &AlterDeleteQuery{
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
func (q *AlterDeleteQuery) Where(query string, args ...any) *AlterDeleteQuery {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: "AND"})
	return q
}

// Build generates the SQL and args.
func (q *AlterDeleteQuery) Build() (string, []any, error) {
	if q.err != nil {
		return "", nil, q.err
	}
	if len(q.wheres) == 0 {
		return "", nil, fmt.Errorf("clickhousedriver: ALTER DELETE requires a WHERE clause")
	}

	buf := pool.GetBuffer()
	defer pool.PutBuffer(buf)

	args := make([]any, 0, len(q.wheres))
	dialect := q.db.dialect

	buf.WriteString("ALTER TABLE ")
	buf.WriteString(dialect.Quote(q.table.Name))
	buf.WriteString(" DELETE WHERE ")

	for i, w := range q.wheres {
		if i > 0 {
			_ = buf.WriteByte(' ')
			buf.WriteString(w.sep)
			_ = buf.WriteByte(' ')
		}
		buf.WriteString(w.query)
		args = append(args, w.args...)
	}

	return buf.String(), args, nil
}

// Exec executes the ALTER TABLE ... DELETE statement.
func (q *AlterDeleteQuery) Exec(ctx context.Context) (driver.Result, error) {
	// Run pre-mutation hooks.
	var qc *hook.QueryContext
	if q.db.hooks != nil {
		qc = q.buildAlterDeleteHookContext()
		result, err := q.db.hooks.RunPreMutation(ctx, qc, q.model)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Decision == hook.Deny {
			if result.Error != nil {
				return nil, result.Error
			}
			return nil, fmt.Errorf("clickhousedriver: alter delete denied by hook")
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}

	if qc != nil {
		qc.RawQuery = query
		qc.RawArgs = args
	}

	res, err := q.db.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if q.db.hooks != nil && qc != nil {
		if err := q.db.hooks.RunPostMutation(ctx, qc, q.model, res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (q *AlterDeleteQuery) buildAlterDeleteHookContext() *hook.QueryContext {
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
