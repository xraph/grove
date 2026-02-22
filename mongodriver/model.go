package mongodriver

import (
	"fmt"
	"reflect"

	"github.com/xraph/grove/schema"
)

// resolveTable resolves a *schema.Table from the given model value.
// It supports:
//   - *User or (*User)(nil) -> looks up User's table
//   - *[]User -> looks up User's table
//   - User{} -> looks up User's table
func resolveTable(model any) (*schema.Table, error) {
	if model == nil {
		return nil, fmt.Errorf("mongodriver: nil model")
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
		return nil, fmt.Errorf("mongodriver: model must be a struct or pointer/slice of struct, got %v", typ.Kind())
	}

	// Create a nil pointer of the struct type for NewTable.
	modelPtr := reflect.New(typ).Interface()
	return schema.NewTable(modelPtr)
}

// collectionName returns the MongoDB collection name for a model.
// In the Grove schema, the table Name maps to the collection name.
func collectionName(table *schema.Table) string {
	return table.Name
}

// extractFieldValues extracts field values from a struct value using field index chains.
func extractFieldValues(structVal reflect.Value, fields []*schema.Field) ([]any, error) {
	values := make([]any, len(fields))
	for i, f := range fields {
		fv := structVal
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}
		values[i] = fv.Interface()
	}
	return values, nil
}

// structToMap converts a struct to a bson.M map using the schema field metadata.
// It includes all fields except those with ScanOnly.
func structToMap(model any, table *schema.Table) (M, error) {
	val := reflect.ValueOf(model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("mongodriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("mongodriver: expected struct, got %v", val.Kind())
	}

	doc := make(M, len(table.Fields))
	for _, f := range table.Fields {
		if f.Options.ScanOnly {
			continue
		}

		fv := val
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}

		// Use the column name as the BSON key.
		// Map "id" to "_id" for MongoDB's primary key convention.
		key := f.Options.Column
		if f.Options.IsPK && key == "id" {
			key = "_id"
		}

		doc[key] = fv.Interface()
	}

	return doc, nil
}

// structToMapInsert converts a struct to a bson.M map for inserts.
// It excludes AutoIncrement fields (MongoDB generates _id automatically).
func structToMapInsert(model any, table *schema.Table) (M, error) {
	val := reflect.ValueOf(model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("mongodriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("mongodriver: expected struct, got %v", val.Kind())
	}

	doc := make(M, len(table.Fields))
	for _, f := range table.Fields {
		if f.Options.ScanOnly {
			continue
		}
		if f.Options.AutoIncrement {
			continue
		}

		fv := val
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}

		// Use the column name as the BSON key.
		key := f.Options.Column
		if f.Options.IsPK && key == "id" {
			key = "_id"
		}

		// Skip zero-value PK fields so MongoDB can auto-generate _id.
		if f.Options.IsPK && fv.IsZero() {
			continue
		}

		doc[key] = fv.Interface()
	}

	return doc, nil
}

// structToUpdateMap converts a struct to a bson.M map for $set updates.
// It excludes PK, ScanOnly, and AutoIncrement fields.
func structToUpdateMap(model any, table *schema.Table) (M, error) {
	val := reflect.ValueOf(model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("mongodriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("mongodriver: expected struct, got %v", val.Kind())
	}

	pkSet := make(map[string]bool, len(table.PKFields))
	for _, pk := range table.PKFields {
		pkSet[pk.Options.Column] = true
	}

	doc := make(M, len(table.Fields))
	for _, f := range table.Fields {
		if f.Options.ScanOnly {
			continue
		}
		if f.Options.AutoIncrement {
			continue
		}
		if pkSet[f.Options.Column] {
			continue
		}

		fv := val
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}

		key := f.Options.Column
		doc[key] = fv.Interface()
	}

	return doc, nil
}
