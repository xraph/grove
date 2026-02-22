package scan

import (
	"reflect"

	"github.com/xraph/grove/schema"
)

// FieldPtr returns a pointer to the struct field identified by the schema Field,
// suitable for passing to database/sql Scan. It navigates nested structs using
// the field's GoIndex chain.
//
// v must be the reflect.Value of the struct (not a pointer to it).
func FieldPtr(v reflect.Value, field *schema.Field) any {
	fv := v
	for _, idx := range field.GoIndex {
		fv = fv.Field(idx)
	}
	return fv.Addr().Interface()
}

// IsNilable returns true if the given type can hold a nil value.
// This includes pointers, interfaces, slices, maps, channels, and functions.
func IsNilable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return true
	default:
		return false
	}
}
