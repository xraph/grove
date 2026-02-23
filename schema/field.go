package schema

import (
	"reflect"
)

// FieldOptions holds all parsed options for a model field.
type FieldOptions struct {
	Column        string    // Column name in the database
	SQLType       string    // Explicit SQL type (e.g., "jsonb", "text[]")
	IsPK          bool      // Primary key
	AutoIncrement bool      // Auto-incrementing
	NotNull       bool      // NOT NULL constraint
	NullZero      bool      // Zero value treated as NULL
	Unique        bool      // UNIQUE constraint
	Default       string    // DEFAULT value
	SoftDelete    bool      // Soft delete timestamp
	ScanOnly      bool      // Read-only, excluded from INSERT/UPDATE
	Skip          bool      // Skip field entirely (tag "-")
	Privacy       string    // Privacy classification (e.g., "pii", "sensitive")
	DriverHint    string    // Driver-specific hint (e.g., "pg", "pg,mongo")
	Index         string    // Named index
	CompositeIdx  string    // Composite index group name
	CRDTType      string    // CRDT type (e.g., "lww", "counter", "set")
	TagSource     TagSource // Which tag was used
}

// Field represents a single column in a table, derived from a struct field.
type Field struct {
	GoName  string       // Go struct field name
	GoType  reflect.Type // Go type
	GoIndex []int        // Struct field index (for nested)
	Options FieldOptions // Parsed options
}

// NewField creates a Field from a reflect.StructField.
// It resolves the tag (grove > bun > snake_case), parses all options,
// and populates the Field struct accordingly.
func NewField(sf reflect.StructField) *Field {
	f := &Field{
		GoName:  sf.Name,
		GoType:  sf.Type,
		GoIndex: sf.Index,
	}

	raw, source := ResolveTag(sf)
	f.Options.TagSource = source

	// Handle the skip case.
	if raw == "-" {
		f.Options.Skip = true
		return f
	}

	if source == TagSourceNone {
		// No tag at all: column name is the snake_case of the Go field name.
		f.Options.Column = ToSnakeCase(sf.Name)
		return f
	}

	// Parse the tag.
	tag := ParseTag(raw)

	// Column name: use the tag's Name if present, otherwise snake_case of Go field name.
	if tag.Name != "" {
		f.Options.Column = tag.Name
	} else {
		f.Options.Column = ToSnakeCase(sf.Name)
	}

	// Boolean options.
	if tag.HasOption("pk") {
		f.Options.IsPK = true
	}
	if tag.HasOption("autoincrement") {
		f.Options.AutoIncrement = true
	}
	if tag.HasOption("notnull") {
		f.Options.NotNull = true
	}
	if tag.HasOption("nullzero") {
		f.Options.NullZero = true
	}
	if tag.HasOption("unique") {
		f.Options.Unique = true
	}
	if tag.HasOption("soft_delete") {
		f.Options.SoftDelete = true
	}
	if tag.HasOption("scanonly") {
		f.Options.ScanOnly = true
	}

	// Key:value options.
	if v := tag.GetOption("type"); v != "" {
		f.Options.SQLType = v
	}
	if v := tag.GetOption("default"); v != "" {
		f.Options.Default = v
	}
	if v := tag.GetOption("privacy"); v != "" {
		f.Options.Privacy = v
	}
	if v := tag.GetOption("driver"); v != "" {
		f.Options.DriverHint = v
	}
	if v := tag.GetOption("index"); v != "" {
		f.Options.Index = v
	}
	if v := tag.GetOption("composite"); v != "" {
		f.Options.CompositeIdx = v
	}
	if v := tag.GetOption("crdt"); v != "" {
		f.Options.CRDTType = v
	}

	return f
}
