package schema

import (
	"fmt"
	"reflect"
	"sync"
)

// baseModelType is the reflect.Type for grove.BaseModel.
// We detect it by name + package path to avoid importing the root grove package
// (which would cause a circular import).
var (
	baseModelDetectOnce sync.Once
	baseModelTypeName   = "BaseModel"
	baseModelPkgSuffix  = "github.com/xraph/grove"
)

// isBaseModelField returns true if the struct field is an embedded grove.BaseModel.
func isBaseModelField(sf reflect.StructField) bool {
	return sf.Anonymous &&
		sf.Type.Kind() == reflect.Struct &&
		sf.Type.Name() == baseModelTypeName &&
		sf.Type.PkgPath() == baseModelPkgSuffix
}

// Table represents metadata about a model's database table.
type Table struct {
	ModelType  reflect.Type // The Go struct type
	Name       string       // Table name (from tag or snake_case of type name)
	Alias      string       // Table alias for queries
	Fields     []*Field     // All mapped fields (excluding skip, relations)
	PKFields   []*Field     // Primary key fields
	Relations  []*Relation  // Declared relations
	SoftDelete *Field       // Soft delete field, if any
}

// ErrInvalidModel is returned when a model is not a valid struct or pointer to struct.
var ErrInvalidModel = fmt.Errorf("grove: invalid model type")

// NewTable builds Table metadata from a model value (e.g., (*User)(nil)).
// Uses reflect to inspect the struct, reads tags, and builds the field list.
func NewTable(model any) (*Table, error) {
	typ := resolveModelType(model)
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: expected struct or pointer to struct, got %T", ErrInvalidModel, model)
	}

	t := &Table{
		ModelType: typ,
	}

	// Default table name is snake_case + "s" plural of the struct name.
	t.Name = ToSnakeCase(typ.Name()) + "s"
	t.Alias = ""

	// Walk struct fields.
	if err := t.processFields(typ, nil); err != nil {
		return nil, err
	}

	return t, nil
}

// processFields walks the struct fields and populates the table metadata.
// indexPrefix tracks the field index chain for embedded structs.
func (t *Table) processFields(typ reflect.Type, indexPrefix []int) error {
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)

		// Build the full index path for this field.
		fullIndex := append(append([]int(nil), indexPrefix...), sf.Index...)

		// Handle BaseModel: extract table-level options.
		if isBaseModelField(sf) {
			t.extractTableOptions(sf)
			continue
		}

		// Handle embedded (anonymous) structs: recurse into them.
		// But skip unexported embedded fields.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			if !sf.IsExported() {
				continue
			}
			if err := t.processFields(sf.Type, fullIndex); err != nil {
				return err
			}
			continue
		}

		// Skip unexported fields.
		if !sf.IsExported() {
			continue
		}

		// Check if this is a relation field.
		raw, _ := ResolveTag(sf)
		if raw != "" {
			parsed := ParseTag(raw)
			if parsed.HasOption("rel") {
				rel, err := ParseRelation(raw, &Field{
					GoName:  sf.Name,
					GoType:  sf.Type,
					GoIndex: fullIndex,
				})
				if err != nil {
					return err
				}
				t.Relations = append(t.Relations, rel)
				continue
			}
		}

		// Regular field.
		field := NewField(sf)
		field.GoIndex = fullIndex

		// Skip fields tagged with "-".
		if field.Options.Skip {
			continue
		}

		t.Fields = append(t.Fields, field)

		if field.Options.IsPK {
			t.PKFields = append(t.PKFields, field)
		}

		if field.Options.SoftDelete {
			t.SoftDelete = field
		}
	}

	return nil
}

// extractTableOptions reads table-level options from the BaseModel field's tag.
func (t *Table) extractTableOptions(sf reflect.StructField) {
	raw, source := ResolveTag(sf)
	if source == TagSourceNone || raw == "" {
		return
	}

	parsed := ParseTag(raw)

	if v := parsed.GetOption("table"); v != "" {
		t.Name = v
	}
	if v := parsed.GetOption("alias"); v != "" {
		t.Alias = v
	}
}

// resolveModelType extracts the reflect.Type of the underlying struct
// from a model value. Supports:
//   - (*Model)(nil)  -> Model type
//   - &Model{}       -> Model type
//   - Model{}        -> Model type
func resolveModelType(model any) reflect.Type {
	if model == nil {
		return nil
	}

	typ := reflect.TypeOf(model)

	// Dereference pointers.
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	return typ
}
