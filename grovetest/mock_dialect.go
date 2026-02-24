package grovetest

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/schema"
)

// MockDialect implements driver.Dialect for testing.
// It uses PostgreSQL-like syntax by default.
type MockDialect struct{}

var _ driver.Dialect = (*MockDialect)(nil)

func (d *MockDialect) Name() string { return "mock" }

func (d *MockDialect) Quote(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return `"` + escaped + `"`
}

func (d *MockDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d *MockDialect) GoToDBType(goType reflect.Type, opts schema.FieldOptions) string {
	if opts.SQLType != "" {
		return opts.SQLType
	}
	t := goType
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == reflect.TypeOf(time.Time{}) {
		return "timestamptz"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int32:
		return "integer"
	case reflect.Int64:
		return "bigint"
	case reflect.String:
		return "text"
	case reflect.Float64:
		return "double precision"
	default:
		return "text"
	}
}

func (d *MockDialect) AppendBytes(b, v []byte) []byte {
	b = append(b, '\'', '\\', 'x')
	dst := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(dst, v)
	b = append(b, dst...)
	b = append(b, '\'')
	return b
}

func (d *MockDialect) AppendTime(b []byte, t time.Time) []byte {
	b = append(b, '\'')
	b = t.AppendFormat(b, time.RFC3339Nano)
	b = append(b, '\'')
	return b
}
