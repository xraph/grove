package tursodriver

import (
	"fmt"
	"reflect"

	"github.com/xraph/grove/internal/pool"
	"github.com/xraph/grove/schema"
)

// whereClause represents a single WHERE condition.
type whereClause struct {
	query string
	args  []any
	sep   string // "AND" or "OR"
}

// baseQuery holds state shared by all query types.
type baseQuery struct {
	db     *TursoDB
	table  *schema.Table
	model  any // The user's model (for scanning)
	wheres []whereClause
	args   []any // All accumulated args
	err    error // Build-time error
}

// addWhere appends a WHERE clause with the given separator.
func (q *baseQuery) addWhere(sep, query string, args []any) {
	q.wheres = append(q.wheres, whereClause{query: query, args: args, sep: sep})
}

// appendWheres appends all WHERE clauses to the buffer.
func (q *baseQuery) appendWheres(buf *pool.Buffer) {
	if len(q.wheres) == 0 {
		return
	}
	buf.WriteString(" WHERE ")
	for i, w := range q.wheres {
		if i > 0 {
			_ = buf.WriteByte(' ')
			buf.WriteString(w.sep)
			_ = buf.WriteByte(' ')
		}
		buf.WriteString(w.query)
		q.args = append(q.args, w.args...)
	}
}

// resolveTable resolves a *schema.Table from the given model value.
// It supports:
//   - *User or (*User)(nil) -> looks up User's table
//   - *[]User -> looks up User's table
//   - User{} -> looks up User's table
func resolveTable(reg *schema.Registry, model any) (*schema.Table, error) {
	if model == nil {
		return nil, fmt.Errorf("tursodriver: nil model")
	}

	typ := reflect.TypeOf(model)

	// Dereference pointers.
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// If it's a slice, get the element type.
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
		// Dereference pointer elements (e.g., []*User -> User).
		for typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("tursodriver: model must be a struct or pointer/slice of struct, got %v", typ.Kind())
	}

	// Create a nil pointer of the struct type for registry lookup (uses sync.Map cache).
	modelPtr := reflect.New(typ).Interface()
	return reg.Register(modelPtr)
}
