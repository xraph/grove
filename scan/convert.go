package scan

import (
	"fmt"
	"reflect"
	"time"

	"github.com/xraph/grove/schema"
)

// FieldPtr returns a scan destination for the struct field identified by the
// schema Field, suitable for passing to database/sql or pgx Scan. It
// navigates nested structs using the field's GoIndex chain.
//
// time.Time and *time.Time fields are wrapped in sql.Scanner adapters:
// TEXT-affinity drivers (sqlite, turso) return timestamps as RFC3339
// strings, which the raw pointers cannot accept. Drivers that already
// produce time.Time (postgres, clickhouse) pass through the adapter
// unchanged.
//
// v must be the reflect.Value of the struct (not a pointer to it).
func FieldPtr(v reflect.Value, field *schema.Field) any {
	if len(field.GoIndex) == 1 {
		return wrapDest(v.Field(field.GoIndex[0]).Addr().Interface(), field)
	}
	fv := v
	for _, idx := range field.GoIndex {
		fv = fv.Field(idx)
	}
	return wrapDest(fv.Addr().Interface(), field)
}

// wrapDest substitutes scanner adapters for destinations the drivers cannot
// fill directly. Non-adapted destinations are returned as-is.
func wrapDest(ptr any, field *schema.Field) any {
	switch p := ptr.(type) {
	case *time.Time:
		return &timeDest{dst: p}
	case **time.Time:
		return &timePtrDest{dst: p}
	case *string:
		// pgx cannot scan SQL NULL into *string and errors. For a nullable
		// column, wrap the destination so NULL coerces to "" — the same
		// NULL→zero-value treatment timeDest gives non-pointer time.Time. This
		// lets a plain `string` field round-trip a nullable column (e.g.
		// project_id, written as NULL when empty via the nullzero insert path).
		// NOT NULL columns never return NULL, so they keep the raw pointer.
		if field != nil && !field.Options.NotNull {
			return &stringDest{dst: p}
		}
		return ptr
	default:
		return ptr
	}
}

// stringDest scans a driver value into a non-pointer string field, mapping SQL
// NULL to the empty string instead of failing.
type stringDest struct{ dst *string }

func (d *stringDest) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*d.dst = ""
		return nil
	case string:
		*d.dst = v
		return nil
	case []byte:
		*d.dst = string(v)
		return nil
	default:
		return fmt.Errorf("scan: cannot convert %T into string", src)
	}
}

// timeDest scans a driver value into a time.Time field.
type timeDest struct{ dst *time.Time }

func (d *timeDest) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*d.dst = time.Time{}
		return nil
	case time.Time:
		*d.dst = v
		return nil
	case string:
		t, err := parseTimeString(v)
		if err != nil {
			return err
		}
		*d.dst = t
		return nil
	case []byte:
		t, err := parseTimeString(string(v))
		if err != nil {
			return err
		}
		*d.dst = t
		return nil
	default:
		return fmt.Errorf("scan: cannot convert %T into time.Time", src)
	}
}

// timePtrDest scans a driver value into a nullable *time.Time field.
type timePtrDest struct{ dst **time.Time }

func (d *timePtrDest) Scan(src any) error {
	if src == nil {
		*d.dst = nil
		return nil
	}
	var t time.Time
	if err := (&timeDest{dst: &t}).Scan(src); err != nil {
		return err
	}
	*d.dst = &t
	return nil
}

// timeLayouts are tried in order when parsing TEXT timestamps. The Go
// default layout comes first because modernc/sqlite serializes time.Time
// arguments with time.Time.String() unless configured otherwise, so that is
// what existing sqlite rows contain. RFC3339Nano covers the strings grove's
// TEXT dialects write via AppendTime; the rest cover values produced by SQL
// defaults like CURRENT_TIMESTAMP and strftime.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999 -0700 MST",
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999-07:00",
	time.DateTime,
	time.DateOnly,
}

func parseTimeString(s string) (time.Time, error) {
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("scan: cannot parse %q as time.Time", s)
}

// IsNilable returns true if the given type can hold a nil value.
// This includes pointers, interfaces, slices, maps, channels, and functions.
func IsNilable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return true
	default:
		return false
	}
}
