// Package mysqldriver provides a MySQL driver for the Grove ORM,
// built on top of database/sql with github.com/go-sql-driver/mysql.
package mysqldriver

import (
	"encoding/hex"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/schema"
)

// MysqlDialect implements driver.Dialect for MySQL.
type MysqlDialect struct{}

var _ driver.Dialect = (*MysqlDialect)(nil)

// Name returns the dialect identifier.
func (d *MysqlDialect) Name() string { return "mysql" }

// Quote wraps an identifier in backticks, escaping any embedded backticks
// by doubling them. This follows the standard MySQL quoting convention for
// identifiers.
func (d *MysqlDialect) Quote(ident string) string {
	escaped := strings.ReplaceAll(ident, "`", "``")
	return "`" + escaped + "`"
}

// Placeholder returns a positional parameter placeholder (?) for MySQL.
// MySQL uses ? for all positional params regardless of index.
// n is 1-indexed but ignored.
func (d *MysqlDialect) Placeholder(n int) string {
	return "?"
}

// GoToDBType maps a Go reflect.Type to the appropriate MySQL column type
// string, taking field options into account.
//
// Mapping rules (in order of precedence):
//  1. If opts.SQLType is set, it is returned verbatim.
//  2. bool            -> "BOOLEAN"
//  3. int, int32      -> "INT"
//  4. int64           -> "BIGINT"
//  5. int16           -> "SMALLINT"
//  6. int8            -> "TINYINT"
//  7. float32         -> "FLOAT"
//  8. float64         -> "DOUBLE"
//  9. string          -> "TEXT" (or "VARCHAR(255)" if Unique is set)
//  10. time.Time      -> "DATETIME(6)"
//  11. *time.Time     -> "DATETIME(6)"
//  12. []byte         -> "BLOB"
//  13. map[string]any -> "JSON"
//  14. default        -> "TEXT"
func (d *MysqlDialect) GoToDBType(goType reflect.Type, opts schema.FieldOptions) string {
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
		return "DATETIME(6)"
	}

	// Check for []byte.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "BLOB"
	}

	// Check for map[string]any -> JSON.
	if t.Kind() == reflect.Map &&
		t.Key().Kind() == reflect.String &&
		t.Elem().Kind() == reflect.Interface {
		return "JSON"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int32:
		return "INT"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Int16:
		return "SMALLINT"
	case reflect.Int8:
		return "TINYINT"
	case reflect.Float32:
		return "FLOAT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		if opts.Unique {
			return "VARCHAR(255)"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}

// AppendBytes appends a hex-encoded MySQL byte literal to b and returns
// the extended slice. The format is: X'<hex>'
func (d *MysqlDialect) AppendBytes(b []byte, v []byte) []byte {
	b = append(b, 'X', '\'')
	dst := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(dst, v)
	b = append(b, dst...)
	b = append(b, '\'')
	return b
}

// AppendTime appends a time value formatted for MySQL (wrapped in single
// quotes) to b and returns the extended slice. The format uses microsecond
// precision: '2006-01-02 15:04:05.999999'
func (d *MysqlDialect) AppendTime(b []byte, t time.Time) []byte {
	b = append(b, '\'')
	b = t.AppendFormat(b, "2006-01-02 15:04:05.999999")
	b = append(b, '\'')
	return b
}
