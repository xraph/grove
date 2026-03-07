package mongodriver

import (
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/grove/schema"
)

// testUser is a model for schema generation tests.
type testSchemaUser struct {
	ID        bson.ObjectID  `grove:"id,pk"`
	Name      string         `grove:"name,notnull"`
	Email     string         `grove:"email,notnull,unique"`
	Age       int            `grove:"age"`
	Score     float64        `grove:"score"`
	Active    bool           `grove:"active"`
	CreatedAt time.Time      `grove:"created_at"`
	Data      []byte         `grove:"data"`
	Tags      []string       `grove:"tags"`
	Meta      map[string]any `grove:"meta"`
	Internal  string         `grove:"-"`
	ReadOnly  string         `grove:"read_only,scanonly"`
}

func TestGoTypeToBSONType(t *testing.T) {
	tests := []struct {
		name     string
		model    any
		expected string
	}{
		{"string", "", "string"},
		{"bool", false, "bool"},
		{"int", int(0), "int"},
		{"int32", int32(0), "int"},
		{"int64", int64(0), "long"},
		{"float32", float32(0), "double"},
		{"float64", float64(0), "double"},
		{"time", time.Time{}, "date"},
		{"objectID", bson.ObjectID{}, "objectId"},
		{"bytes", []byte{}, "binData"},
		{"stringSlice", []string{}, "array"},
		{"map", map[string]any{}, "object"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := goTypeToBSONType(typeOf(tt.model))
			if got != tt.expected {
				t.Errorf("goTypeToBSONType(%T) = %q, want %q", tt.model, got, tt.expected)
			}
		})
	}
}

func TestBuildJSONSchema_Basic(t *testing.T) {
	table, err := schema.NewTable((*testSchemaUser)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	s := buildJSONSchema(table, nil)

	// Check top-level type.
	if s["bsonType"] != "object" {
		t.Fatalf("expected bsonType=object, got %v", s["bsonType"])
	}

	// Check properties exist.
	props, ok := s["properties"].(bson.M)
	if !ok {
		t.Fatalf("expected properties to be bson.M")
	}

	// PK "id" should be mapped to "_id".
	if _, exists := props["_id"]; !exists {
		t.Error("expected _id in properties (pk 'id' should be mapped to '_id')")
	}
	if _, exists := props["id"]; exists {
		t.Error("'id' should be mapped to '_id', not present as 'id'")
	}

	// Verify a few expected fields.
	expectedFields := []string{"name", "email", "age", "score", "active", "created_at", "data", "tags", "meta"}
	for _, field := range expectedFields {
		if _, exists := props[field]; !exists {
			t.Errorf("expected field %q in properties", field)
		}
	}

	// Internal (skip) and read_only (scanonly) should be excluded.
	if _, exists := props["internal"]; exists {
		t.Error("field tagged '-' should be excluded")
	}
	if _, exists := props["read_only"]; exists {
		t.Error("scanonly field should be excluded")
	}
}

func TestBuildJSONSchema_Required(t *testing.T) {
	table, err := schema.NewTable((*testSchemaUser)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	s := buildJSONSchema(table, nil)

	required, ok := s["required"].([]string)
	if !ok {
		t.Fatalf("expected required to be []string, got %T", s["required"])
	}

	// PK and notnull fields should be required.
	requiredSet := make(map[string]bool, len(required))
	for _, r := range required {
		requiredSet[r] = true
	}

	if !requiredSet["_id"] {
		t.Error("_id (pk) should be required")
	}
	if !requiredSet["name"] {
		t.Error("name (notnull) should be required")
	}
	if !requiredSet["email"] {
		t.Error("email (notnull) should be required")
	}

	// Non-required fields should not be in the list.
	if requiredSet["age"] {
		t.Error("age should not be required")
	}
}

func TestBuildJSONSchema_AdditionalProperties(t *testing.T) {
	table, err := schema.NewTable((*testSchemaUser)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	// Without additionalProperties.
	s := buildJSONSchema(table, nil)
	if _, exists := s["additionalProperties"]; exists {
		t.Error("additionalProperties should not be set when nil")
	}

	// With additionalProperties = false.
	f := false
	s = buildJSONSchema(table, &f)
	if v, ok := s["additionalProperties"].(bool); !ok || v {
		t.Error("expected additionalProperties=false")
	}

	// With additionalProperties = true.
	tr := true
	s = buildJSONSchema(table, &tr)
	if v, ok := s["additionalProperties"].(bool); !ok || !v {
		t.Error("expected additionalProperties=true")
	}
}

func TestBuildJSONSchema_TypeMapping(t *testing.T) {
	table, err := schema.NewTable((*testSchemaUser)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	s := buildJSONSchema(table, nil)
	props := s["properties"].(bson.M)

	tests := []struct {
		field    string
		bsonType string
	}{
		{"_id", "objectId"},
		{"name", "string"},
		{"age", "int"},
		{"score", "double"},
		{"active", "bool"},
		{"created_at", "date"},
		{"data", "binData"},
		{"tags", "array"},
		{"meta", "object"},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			fieldSchema, ok := props[tt.field].(bson.M)
			if !ok {
				t.Fatalf("field %q not found or not bson.M", tt.field)
			}
			if fieldSchema["bsonType"] != tt.bsonType {
				t.Errorf("field %q: expected bsonType=%q, got %q", tt.field, tt.bsonType, fieldSchema["bsonType"])
			}
		})
	}
}

func TestBuildFieldSchema_ExplicitType(t *testing.T) {
	f := &schema.Field{
		GoName: "Custom",
		Options: schema.FieldOptions{
			Column:  "custom",
			SQLType: "decimal",
		},
	}

	s := buildFieldSchema(f)
	if s["bsonType"] != "decimal" {
		t.Errorf("expected explicit type 'decimal', got %v", s["bsonType"])
	}
}

func TestCreateCollectionQuery_BuildSchema(t *testing.T) {
	db := &MongoDB{}
	q := db.NewCreateCollection((*testSchemaUser)(nil))

	s, err := q.BuildSchema()
	if err != nil {
		t.Fatalf("BuildSchema: %v", err)
	}

	if s["bsonType"] != "object" {
		t.Errorf("expected bsonType=object, got %v", s["bsonType"])
	}

	props, ok := s["properties"].(bson.M)
	if !ok {
		t.Fatal("expected properties to be bson.M")
	}

	if len(props) == 0 {
		t.Error("expected non-empty properties")
	}
}

func TestCreateCollectionQuery_Collection(t *testing.T) {
	db := &MongoDB{}
	q := db.NewCreateCollection((*testSchemaUser)(nil)).Collection("custom_users")
	if q.GetCollection() != "custom_users" {
		t.Errorf("expected collection 'custom_users', got %q", q.GetCollection())
	}
}

func TestCreateCollectionQuery_AdditionalProperties(t *testing.T) {
	db := &MongoDB{}
	q := db.NewCreateCollection((*testSchemaUser)(nil)).AdditionalProperties(false)

	s, err := q.BuildSchema()
	if err != nil {
		t.Fatalf("BuildSchema: %v", err)
	}

	if v, ok := s["additionalProperties"].(bool); !ok || v {
		t.Error("expected additionalProperties=false in schema")
	}
}

func TestDropCollectionQuery_Collection(t *testing.T) {
	db := &MongoDB{}
	q := db.NewDropCollection((*testSchemaUser)(nil))
	if q.GetCollection() == "" {
		t.Error("expected non-empty collection name")
	}

	q = q.Collection("custom_users")
	if q.GetCollection() != "custom_users" {
		t.Errorf("expected 'custom_users', got %q", q.GetCollection())
	}
}

// typeOf is a helper to get reflect.Type from a value.
func typeOf(v any) reflect.Type {
	return reflect.TypeOf(v)
}
