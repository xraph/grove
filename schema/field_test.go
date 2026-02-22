package schema

import (
	"reflect"
	"testing"
	"time"
)

func TestNewField(t *testing.T) {
	type testStruct struct {
		ID        int64      `grove:"id,pk,autoincrement"`
		Name      string     `grove:"name,notnull"`
		Email     string     `grove:"email,notnull,unique"`
		Data      []byte     `grove:"data,type:jsonb"`
		Status    string     `grove:"status,default:'active'"`
		SSN       string     `grove:"ssn,privacy:pii"`
		Score     float64    `grove:"score,nullzero"`
		Notes     string     `grove:"notes,scanonly"`
		Internal  string     `grove:"-"`
		DeletedAt *time.Time `grove:"deleted_at,soft_delete"`
		PGOnly    string     `grove:"pg_only,driver:pg"`
		Idx       string     `grove:"idx_field,index:idx_name"`
		Comp      string     `grove:"comp_field,composite:comp_group"`
		NoTag     string
		BunField  string `bun:"bun_col,pk"`
	}

	typ := reflect.TypeOf(testStruct{})

	tests := []struct {
		fieldName string
		check     func(t *testing.T, f *Field)
	}{
		{
			fieldName: "ID",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "id" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "id")
				}
				if !f.Options.IsPK {
					t.Error("expected IsPK to be true")
				}
				if !f.Options.AutoIncrement {
					t.Error("expected AutoIncrement to be true")
				}
				if f.Options.TagSource != TagSourceGrove {
					t.Errorf("TagSource = %d, want TagSourceGrove", f.Options.TagSource)
				}
			},
		},
		{
			fieldName: "Name",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "name" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "name")
				}
				if !f.Options.NotNull {
					t.Error("expected NotNull to be true")
				}
				if f.Options.IsPK {
					t.Error("expected IsPK to be false")
				}
			},
		},
		{
			fieldName: "Email",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "email" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "email")
				}
				if !f.Options.NotNull {
					t.Error("expected NotNull to be true")
				}
				if !f.Options.Unique {
					t.Error("expected Unique to be true")
				}
			},
		},
		{
			fieldName: "Data",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "data" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "data")
				}
				if f.Options.SQLType != "jsonb" {
					t.Errorf("SQLType = %q, want %q", f.Options.SQLType, "jsonb")
				}
			},
		},
		{
			fieldName: "Status",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "status" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "status")
				}
				if f.Options.Default != "active" {
					t.Errorf("Default = %q, want %q", f.Options.Default, "active")
				}
			},
		},
		{
			fieldName: "SSN",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "ssn" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "ssn")
				}
				if f.Options.Privacy != "pii" {
					t.Errorf("Privacy = %q, want %q", f.Options.Privacy, "pii")
				}
			},
		},
		{
			fieldName: "Score",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "score" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "score")
				}
				if !f.Options.NullZero {
					t.Error("expected NullZero to be true")
				}
			},
		},
		{
			fieldName: "Notes",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "notes" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "notes")
				}
				if !f.Options.ScanOnly {
					t.Error("expected ScanOnly to be true")
				}
			},
		},
		{
			fieldName: "Internal",
			check: func(t *testing.T, f *Field) {
				if !f.Options.Skip {
					t.Error("expected Skip to be true for '-' tag")
				}
			},
		},
		{
			fieldName: "DeletedAt",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "deleted_at" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "deleted_at")
				}
				if !f.Options.SoftDelete {
					t.Error("expected SoftDelete to be true")
				}
			},
		},
		{
			fieldName: "PGOnly",
			check: func(t *testing.T, f *Field) {
				if f.Options.DriverHint != "pg" {
					t.Errorf("DriverHint = %q, want %q", f.Options.DriverHint, "pg")
				}
			},
		},
		{
			fieldName: "Idx",
			check: func(t *testing.T, f *Field) {
				if f.Options.Index != "idx_name" {
					t.Errorf("Index = %q, want %q", f.Options.Index, "idx_name")
				}
			},
		},
		{
			fieldName: "Comp",
			check: func(t *testing.T, f *Field) {
				if f.Options.CompositeIdx != "comp_group" {
					t.Errorf("CompositeIdx = %q, want %q", f.Options.CompositeIdx, "comp_group")
				}
			},
		},
		{
			fieldName: "NoTag",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "no_tag" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "no_tag")
				}
				if f.Options.TagSource != TagSourceNone {
					t.Errorf("TagSource = %d, want TagSourceNone", f.Options.TagSource)
				}
			},
		},
		{
			fieldName: "BunField",
			check: func(t *testing.T, f *Field) {
				if f.Options.Column != "bun_col" {
					t.Errorf("Column = %q, want %q", f.Options.Column, "bun_col")
				}
				if !f.Options.IsPK {
					t.Error("expected IsPK to be true via bun tag")
				}
				if f.Options.TagSource != TagSourceBun {
					t.Errorf("TagSource = %d, want TagSourceBun", f.Options.TagSource)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.fieldName, func(t *testing.T) {
			sf, ok := typ.FieldByName(tt.fieldName)
			if !ok {
				t.Fatalf("field %q not found in testStruct", tt.fieldName)
			}
			f := NewField(sf)
			if f.GoName != tt.fieldName {
				t.Errorf("GoName = %q, want %q", f.GoName, tt.fieldName)
			}
			tt.check(t, f)
		})
	}
}

func TestNewFieldGoType(t *testing.T) {
	type sample struct {
		Age int `grove:"age"`
	}
	sf, _ := reflect.TypeOf(sample{}).FieldByName("Age")
	f := NewField(sf)
	if f.GoType != reflect.TypeOf(0) {
		t.Errorf("GoType = %v, want int", f.GoType)
	}
}

func TestNewFieldColumnFromTagName(t *testing.T) {
	// When the tag has a Name but no explicit column, the tag Name is the column.
	type sample struct {
		MyField string `grove:"my_column"`
	}
	sf, _ := reflect.TypeOf(sample{}).FieldByName("MyField")
	f := NewField(sf)
	if f.Options.Column != "my_column" {
		t.Errorf("Column = %q, want %q", f.Options.Column, "my_column")
	}
}

func TestNewFieldColumnFallbackSnakeCase(t *testing.T) {
	// When the tag is key:value only (no Name), column falls back to snake_case.
	type sample struct {
		MyField string `grove:"type:text"`
	}
	sf, _ := reflect.TypeOf(sample{}).FieldByName("MyField")
	f := NewField(sf)
	if f.Options.Column != "my_field" {
		t.Errorf("Column = %q, want %q", f.Options.Column, "my_field")
	}
	if f.Options.SQLType != "text" {
		t.Errorf("SQLType = %q, want %q", f.Options.SQLType, "text")
	}
}
