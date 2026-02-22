package scan

import (
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/schema"
)

// ---------- Test models for column tests ----------

type ColUser struct {
	grove.BaseModel `grove:"table:users"`

	ID        int64      `grove:"id,pk,autoincrement"`
	Name      string     `grove:"name,notnull"`
	Email     string     `grove:"email,notnull,unique"`
	DeletedAt *time.Time `grove:"deleted_at,soft_delete"`
}

type ColMinimal struct {
	grove.BaseModel `grove:"table:minimal"`

	ID int64 `grove:"id,pk"`
}

// ---------- NewColumnMap tests ----------

func TestNewColumnMap(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)

	if len(cm.fields) != len(table.Fields) {
		t.Errorf("fields count = %d, want %d", len(cm.fields), len(table.Fields))
	}
	if len(cm.fieldMap) != len(table.Fields) {
		t.Errorf("fieldMap count = %d, want %d", len(cm.fieldMap), len(table.Fields))
	}

	// Verify all fields are in the map.
	for _, f := range table.Fields {
		if _, ok := cm.fieldMap[f.Options.Column]; !ok {
			t.Errorf("column %q not found in fieldMap", f.Options.Column)
		}
	}
}

func TestNewColumnMap_Minimal(t *testing.T) {
	table, err := schema.NewTable((*ColMinimal)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)

	if len(cm.fieldMap) != 1 {
		t.Errorf("fieldMap count = %d, want 1", len(cm.fieldMap))
	}

	f, ok := cm.fieldMap["id"]
	if !ok {
		t.Fatal("expected 'id' in fieldMap")
	}
	if f.GoName != "ID" {
		t.Errorf("GoName = %q, want %q", f.GoName, "ID")
	}
}

// ---------- Resolve tests ----------

func TestResolve_MatchingColumns(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	columns := []string{"id", "name", "email", "deleted_at"}
	fields := cm.Resolve(columns)

	if len(fields) != 4 {
		t.Fatalf("resolved fields count = %d, want 4", len(fields))
	}

	expectedGoNames := []string{"ID", "Name", "Email", "DeletedAt"}
	for i, want := range expectedGoNames {
		if fields[i] == nil {
			t.Errorf("fields[%d] is nil, want GoName=%q", i, want)
			continue
		}
		if fields[i].GoName != want {
			t.Errorf("fields[%d].GoName = %q, want %q", i, fields[i].GoName, want)
		}
	}
}

func TestResolve_ReorderedColumns(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	// Columns in different order than the struct.
	columns := []string{"email", "id", "name"}
	fields := cm.Resolve(columns)

	if len(fields) != 3 {
		t.Fatalf("resolved fields count = %d, want 3", len(fields))
	}

	expectedGoNames := []string{"Email", "ID", "Name"}
	for i, want := range expectedGoNames {
		if fields[i] == nil {
			t.Errorf("fields[%d] is nil, want GoName=%q", i, want)
			continue
		}
		if fields[i].GoName != want {
			t.Errorf("fields[%d].GoName = %q, want %q", i, fields[i].GoName, want)
		}
	}
}

func TestResolve_MissingColumns(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	columns := []string{"id", "nonexistent_column", "name"}
	fields := cm.Resolve(columns)

	if len(fields) != 3 {
		t.Fatalf("resolved fields count = %d, want 3", len(fields))
	}

	if fields[0] == nil || fields[0].GoName != "ID" {
		t.Errorf("fields[0]: expected ID, got %v", fields[0])
	}
	if fields[1] != nil {
		t.Errorf("fields[1]: expected nil for unknown column, got %v", fields[1])
	}
	if fields[2] == nil || fields[2].GoName != "Name" {
		t.Errorf("fields[2]: expected Name, got %v", fields[2])
	}
}

func TestResolve_ExtraColumns(t *testing.T) {
	table, err := schema.NewTable((*ColMinimal)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	// More columns than the model has fields.
	columns := []string{"id", "extra1", "extra2"}
	fields := cm.Resolve(columns)

	if len(fields) != 3 {
		t.Fatalf("resolved fields count = %d, want 3", len(fields))
	}

	if fields[0] == nil || fields[0].GoName != "ID" {
		t.Errorf("fields[0]: expected ID, got %v", fields[0])
	}
	if fields[1] != nil {
		t.Errorf("fields[1]: expected nil, got GoName=%q", fields[1].GoName)
	}
	if fields[2] != nil {
		t.Errorf("fields[2]: expected nil, got GoName=%q", fields[2].GoName)
	}
}

func TestResolve_EmptyColumns(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	fields := cm.Resolve([]string{})

	if len(fields) != 0 {
		t.Errorf("resolved fields count = %d, want 0", len(fields))
	}
}

func TestResolve_AllMissing(t *testing.T) {
	table, err := schema.NewTable((*ColUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	cm := NewColumnMap(table)
	columns := []string{"foo", "bar", "baz"}
	fields := cm.Resolve(columns)

	if len(fields) != 3 {
		t.Fatalf("resolved fields count = %d, want 3", len(fields))
	}

	for i, f := range fields {
		if f != nil {
			t.Errorf("fields[%d]: expected nil, got GoName=%q", i, f.GoName)
		}
	}
}
