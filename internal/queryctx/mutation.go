package queryctx

import (
	"reflect"

	"github.com/xraph/grove/schema"
)

// MutationColumns reports the model fields included in a mutation. The result
// is non-nil even when every field is omitted, so hooks can distinguish an
// empty update from a caller that supplied no column metadata.
func MutationColumns(fields []*schema.Field, model any, omitZero bool) []string {
	columns := make([]string, 0, len(fields))
	for _, f := range fields {
		if omitZero {
			value := reflect.ValueOf(model)
			for _, index := range f.GoIndex {
				for value.IsValid() && value.Kind() == reflect.Pointer {
					if value.IsNil() {
						value = reflect.Value{}
						break
					}
					value = value.Elem()
				}
				if !value.IsValid() || value.Kind() != reflect.Struct {
					value = reflect.Value{}
					break
				}
				value = value.Field(index)
			}
			if !value.IsValid() || value.IsZero() {
				continue
			}
		}
		columns = append(columns, f.Options.Column)
	}
	return columns
}
