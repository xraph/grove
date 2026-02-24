package esdriver

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
		return nil, fmt.Errorf("esdriver: nil model")
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
		return nil, fmt.Errorf("esdriver: model must be a struct or pointer/slice of struct, got %v", typ.Kind())
	}

	// Create a nil pointer of the struct type for NewTable.
	modelPtr := reflect.New(typ).Interface()
	return schema.NewTable(modelPtr)
}

// indexName returns the Elasticsearch index name for a model.
// In the Grove schema, the table Name maps to the index name.
func indexName(table *schema.Table) string {
	return table.Name
}

// structToDocInsert converts a struct to a map for ES indexing.
// It returns the document body and the extracted document ID separately,
// because Elasticsearch stores _id outside _source.
// If the PK field has a non-zero value, it becomes the ES document _id.
// If zero-valued, ES auto-generates one.
func structToDocInsert(model any, table *schema.Table) (doc M, docID string, err error) {
	val := reflect.ValueOf(model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, "", fmt.Errorf("esdriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, "", fmt.Errorf("esdriver: expected struct, got %v", val.Kind())
	}

	doc = make(M, len(table.Fields))

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

		// Extract PK as document _id (stored outside _source in ES).
		if f.Options.IsPK {
			if !fv.IsZero() {
				docID = fmt.Sprintf("%v", fv.Interface())
			}
			continue
		}

		doc[f.Options.Column] = fv.Interface()
	}

	return doc, docID, nil
}

// structToDocUpdate converts a struct to a map for partial updates.
// It excludes PK, ScanOnly, and AutoIncrement fields.
func structToDocUpdate(model any, table *schema.Table) (M, error) {
	val := reflect.ValueOf(model)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("esdriver: nil model pointer")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("esdriver: expected struct, got %v", val.Kind())
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

		doc[f.Options.Column] = fv.Interface()
	}

	return doc, nil
}
