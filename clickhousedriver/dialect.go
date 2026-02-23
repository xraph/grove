// Package clickhousedriver provides a ClickHouse driver for the Grove ORM,
// built on top of database/sql with github.com/ClickHouse/clickhouse-go/v2.
package clickhousedriver

import (
	"encoding/hex"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/schema"
)

// ClickHouseDialect implements driver.Dialect for ClickHouse.
type ClickHouseDialect struct{}

var _ driver.Dialect = (*ClickHouseDialect)(nil)

// Name returns the dialect identifier.
func (d *ClickHouseDialect) Name() string { return "clickhouse" }

// Quote wraps an identifier in backticks, escaping any embedded backticks
// by doubling them. ClickHouse uses backtick quoting like MySQL.
func (d *ClickHouseDialect) Quote(ident string) string {
	escaped := strings.ReplaceAll(ident, "`", "``")
	return "`" + escaped + "`"
}

// Placeholder returns the ClickHouse parameter placeholder. ClickHouse uses ?
// for all positional parameters regardless of position.
func (d *ClickHouseDialect) Placeholder(n int) string {
	return "?"
}

// GoToDBType maps a Go reflect.Type to the appropriate ClickHouse column type
// string, taking field options into account.
//
// Mapping rules (in order of precedence):
//  1. If opts.SQLType is set, it is returned verbatim.
//  2. time.Time       -> "DateTime64(3)"
//  3. *time.Time      -> "Nullable(DateTime64(3))"
//  4. uuid.UUID       -> "UUID"
//  5. []byte          -> "String"
//  6. map[string]any  -> "String" (JSON stored as String)
//  7. bool            -> "Bool"
//  8. int8            -> "Int8"
//  9. int16           -> "Int16"
//  10. int32          -> "Int32"
//  11. int, int64     -> "Int64"
//  12. uint8          -> "UInt8"
//  13. uint16         -> "UInt16"
//  14. uint32         -> "UInt32"
//  15. uint, uint64   -> "UInt64"
//  16. float32        -> "Float32"
//  17. float64        -> "Float64"
//  18. string         -> "String"
//  19. default        -> "String"
func (d *ClickHouseDialect) GoToDBType(goType reflect.Type, opts schema.FieldOptions) string {
	// Explicit SQL type always wins.
	if opts.SQLType != "" {
		return opts.SQLType
	}

	// Dereference pointer types for matching.
	t := goType
	isPtr := false
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}

	// Check for time.Time first (it's a struct with a specific package path).
	if t == reflect.TypeOf(time.Time{}) {
		if isPtr {
			return "Nullable(DateTime64(3))"
		}
		return "DateTime64(3)"
	}

	// Check for uuid.UUID (github.com/google/uuid).
	if t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID" {
		return "UUID"
	}

	// Check for []byte.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "String"
	}

	// Check for map[string]any -> String (JSON stored as String).
	if t.Kind() == reflect.Map &&
		t.Key().Kind() == reflect.String &&
		t.Elem().Kind() == reflect.Interface {
		return "String"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "Bool"
	case reflect.Int8:
		return "Int8"
	case reflect.Int16:
		return "Int16"
	case reflect.Int32:
		return "Int32"
	case reflect.Int, reflect.Int64:
		return "Int64"
	case reflect.Uint8:
		return "UInt8"
	case reflect.Uint16:
		return "UInt16"
	case reflect.Uint32:
		return "UInt32"
	case reflect.Uint, reflect.Uint64:
		return "UInt64"
	case reflect.Float32:
		return "Float32"
	case reflect.Float64:
		return "Float64"
	case reflect.String:
		return "String"
	default:
		return "String"
	}
}

// AppendBytes appends a hex-encoded ClickHouse blob literal to b and returns
// the extended slice. The format is: unhex('<hex>')
func (d *ClickHouseDialect) AppendBytes(b []byte, v []byte) []byte {
	b = append(b, "unhex('"...)
	dst := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(dst, v)
	b = append(b, dst...)
	b = append(b, "')"...)
	return b
}

// AppendTime appends a time value formatted as 'YYYY-MM-DD HH:MM:SS.sss'
// (wrapped in single quotes) to b and returns the extended slice.
func (d *ClickHouseDialect) AppendTime(b []byte, t time.Time) []byte {
	b = append(b, '\'')
	b = t.AppendFormat(b, "2006-01-02 15:04:05.999")
	b = append(b, '\'')
	return b
}
