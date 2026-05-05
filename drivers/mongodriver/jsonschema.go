package mongodriver

import (
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/grove/schema"
)

// buildJSONSchema generates a MongoDB $jsonSchema document from a Grove schema.Table.
// The schema maps Go types to BSON types and respects field constraints (pk, notnull, etc.).
// If additionalProps is non-nil, it sets the additionalProperties constraint.
func buildJSONSchema(table *schema.Table, additionalProps *bool) bson.M {
	properties := bson.M{}
	var required []string

	for _, f := range table.Fields {
		if f.Options.ScanOnly || f.Options.Skip {
			continue
		}

		key := f.Options.Column
		if f.Options.IsPK && key == "id" {
			key = "_id"
		}

		fieldSchema := buildFieldSchema(f)
		properties[key] = fieldSchema

		// Fields that are pk or notnull are required.
		if f.Options.IsPK || f.Options.NotNull {
			required = append(required, key)
		}
	}

	jsonSchema := bson.M{
		"bsonType":   "object",
		"properties": properties,
	}

	if len(required) > 0 {
		jsonSchema["required"] = required
	}

	if additionalProps != nil {
		jsonSchema["additionalProperties"] = *additionalProps
	}

	return jsonSchema
}

// buildFieldSchema creates a BSON schema definition for a single field.
//
// Pointer types (e.g. *time.Time) are nullable in Go semantics: a nil pointer
// is encoded as BSON null on insert. Unless the field is marked notnull, the
// generated schema therefore accepts both the value type and "null" so the
// $jsonSchema validator doesn't reject documents whose nullable pointer fields
// are nil.
func buildFieldSchema(f *schema.Field) bson.M {
	fieldSchema := bson.M{}

	// Determine the base bsonType: explicit override via type tag wins,
	// otherwise infer from the Go type.
	var base string
	if f.Options.SQLType != "" {
		base = f.Options.SQLType
	} else {
		base = goTypeToBSONType(f.GoType)
	}

	// Pointer fields that aren't notnull may legitimately serialize as null.
	if f.GoType != nil && f.GoType.Kind() == reflect.Ptr && !f.Options.NotNull {
		fieldSchema["bsonType"] = bson.A{base, "null"}
		return fieldSchema
	}

	fieldSchema["bsonType"] = base
	return fieldSchema
}

// goTypeToBSONType maps a Go reflect.Type to a MongoDB BSON type string.
func goTypeToBSONType(t reflect.Type) string {
	// Dereference pointers.
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check well-known types first.
	switch t {
	case reflect.TypeOf(time.Time{}):
		return "date"
	case reflect.TypeOf(bson.ObjectID{}):
		return "objectId"
	}

	// Map by kind.
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "int"
	case reflect.Int64:
		return "long"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "int"
	case reflect.Uint64:
		return "long"
	case reflect.Float32:
		return "double"
	case reflect.Float64:
		return "double"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "binData"
		}
		return "array"
	case reflect.Map:
		return "object"
	case reflect.Struct:
		return "object"
	default:
		return "string"
	}
}
