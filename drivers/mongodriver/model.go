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

// structToPKMap extracts non-zero PK field values from a model.
// Used by upsert operations to populate $setOnInsert so that the caller's
// chosen _id (e.g. a TypeID) is written on insert instead of a MongoDB ObjectID.
func structToPKMap(model any, table *schema.Table) (M, error) {
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

	doc := make(M, len(table.PKFields))
	for _, f := range table.PKFields {
		fv := val
		for _, idx := range f.GoIndex {
			fv = fv.Field(idx)
		}

		if fv.IsZero() {
			continue
		}

		key := f.Options.Column
		if key == "id" {
			key = "_id"
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
