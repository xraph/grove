// Package tursodriver provides a Turso/libSQL driver for the Grove ORM,
// built on top of database/sql. Users must import the libsql driver
// separately in their main package:
//
//	import _ "github.com/tursodatabase/go-libsql"
package tursodriver

import (
	"encoding/hex"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/schema"
)

// TursoDialect implements driver.Dialect for Turso/libSQL.
// Since Turso uses the SQLite dialect, this is functionally identical
// to the SQLite dialect.
type TursoDialect struct{}

var _ driver.Dialect = (*TursoDialect)(nil)

// Name returns the dialect identifier.
func (d *TursoDialect) Name() string { return "turso" }

// Quote wraps an identifier in double quotes, escaping any embedded double
// quotes by doubling them. This follows the standard SQL quoting convention
// for identifiers that SQLite supports.
func (d *TursoDialect) Quote(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return `"` + escaped + `"`
}

// Placeholder returns the Turso/libSQL parameter placeholder. Like SQLite,
// it uses ? for all positional parameters regardless of position.
func (d *TursoDialect) Placeholder(n int) string {
	return "?"
}

// GoToDBType maps a Go reflect.Type to the appropriate SQLite column type
// string, taking field options into account.
//
// Mapping rules (in order of precedence):
//  1. If opts.SQLType is set, it is returned verbatim.
//  2. bool            -> "INTEGER" (SQLite uses 0/1)
//  3. int, int32      -> "INTEGER"
//  4. int64           -> "INTEGER"
//  5. int16           -> "INTEGER"
//  6. int8            -> "INTEGER"
//  7. float32         -> "REAL"
//  8. float64         -> "REAL"
//  9. string          -> "TEXT" (or "TEXT" if Unique is set)
//  10. time.Time      -> "TEXT" (stored as RFC3339)
//  11. *time.Time     -> "TEXT" (stored as RFC3339)
//  12. []byte         -> "BLOB"
//  13. map[string]any -> "TEXT" (JSON stored as text)
//  14. default        -> "TEXT"
func (d *TursoDialect) GoToDBType(goType reflect.Type, opts schema.FieldOptions) string {
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
		return "TEXT"
	}

	// Check for []byte.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "BLOB"
	}

	// Check for map[string]any -> TEXT (JSON stored as text).
	if t.Kind() == reflect.Map &&
		t.Key().Kind() == reflect.String &&
		t.Elem().Kind() == reflect.Interface {
		return "TEXT"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "INTEGER"
	case reflect.Int, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "INTEGER"
	case reflect.Int16:
		return "INTEGER"
	case reflect.Int8:
		return "INTEGER"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "REAL"
	case reflect.String:
		return "TEXT"
	default:
		return "TEXT"
	}
}

// AppendBytes appends a hex-encoded SQLite blob literal to b and returns
// the extended slice. The format is: X'<hex>'
func (d *TursoDialect) AppendBytes(b []byte, v []byte) []byte {
	b = append(b, 'X', '\'')
	dst := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(dst, v)
	b = append(b, dst...)
	b = append(b, '\'')
	return b
}

// AppendTime appends a time value formatted as RFC3339 (wrapped in single
// quotes) to b and returns the extended slice.
func (d *TursoDialect) AppendTime(b []byte, t time.Time) []byte {
	b = append(b, '\'')
	b = t.AppendFormat(b, time.RFC3339)
	b = append(b, '\'')
	return b
}
