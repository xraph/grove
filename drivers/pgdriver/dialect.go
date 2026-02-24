// Package pgdriver provides a PostgreSQL driver for the Grove ORM,
// built on top of pgxpool from github.com/jackc/pgx/v5.
package pgdriver

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/schema"
)

// PgDialect implements driver.Dialect for PostgreSQL.
type PgDialect struct{}

var _ driver.Dialect = (*PgDialect)(nil)

// Name returns the dialect identifier.
func (d *PgDialect) Name() string { return "pg" }

// Quote wraps an identifier in double quotes, escaping any embedded double
// quotes by doubling them. This follows the standard PostgreSQL quoting
// convention for identifiers.
func (d *PgDialect) Quote(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return `"` + escaped + `"`
}

// Placeholder returns a positional parameter placeholder ($N) for PostgreSQL.
// n is 1-indexed.
func (d *PgDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

// GoToDBType maps a Go reflect.Type to the appropriate PostgreSQL column type
// string, taking field options into account.
//
// Mapping rules (in order of precedence):
//  1. If opts.SQLType is set, it is returned verbatim.
//  2. bool            -> "boolean"
//  3. int, int32      -> "integer"
//  4. int64           -> "bigint"
//  5. int16           -> "smallint"
//  6. int8            -> "smallint"
//  7. float32         -> "real"
//  8. float64         -> "double precision"
//  9. string          -> "text" (or "varchar(255)" if Unique is set)
//  10. time.Time      -> "timestamptz"
//  11. *time.Time     -> "timestamptz"
//  12. []byte         -> "bytea"
//  13. map[string]any -> "jsonb"
//  14. default        -> "text"
func (d *PgDialect) GoToDBType(goType reflect.Type, opts schema.FieldOptions) string {
	// Explicit SQL type always wins.
	if opts.SQLType != "" {
		return opts.SQLType
	}

	// Dereference pointer types for matching.
	t := goType
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check for time.Time first (it's a struct with a specific package path).
	if t == reflect.TypeOf(time.Time{}) {
		return "timestamptz"
	}

	// Check for []byte.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "bytea"
	}

	// Check for map[string]any -> jsonb.
	if t.Kind() == reflect.Map &&
		t.Key().Kind() == reflect.String &&
		t.Elem().Kind() == reflect.Interface {
		return "jsonb"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int32:
		return "integer"
	case reflect.Int64:
		return "bigint"
	case reflect.Int16:
		return "smallint"
	case reflect.Int8:
		return "smallint"
	case reflect.Float32:
		return "real"
	case reflect.Float64:
		return "double precision"
	case reflect.String:
		if opts.Unique {
			return "varchar(255)"
		}
		return "text"
	default:
		return "text"
	}
}

// AppendBytes appends a hex-encoded PostgreSQL bytea literal to b and returns
// the extended slice. The format is: '\x<hex>'
func (d *PgDialect) AppendBytes(b []byte, v []byte) []byte {
	b = append(b, '\'')
	b = append(b, '\\', 'x')
	dst := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(dst, v)
	b = append(b, dst...)
	b = append(b, '\'')
	return b
}

// AppendTime appends a time value formatted as RFC3339Nano (wrapped in single
// quotes) to b and returns the extended slice.
func (d *PgDialect) AppendTime(b []byte, t time.Time) []byte {
	b = append(b, '\'')
	b = t.AppendFormat(b, time.RFC3339Nano)
	b = append(b, '\'')
	return b
}
