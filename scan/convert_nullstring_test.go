package scan

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/schema"
)

// nullStrModel has a nullable string column (project_id, like the twinos
// resource tables) and a NOT NULL one.
type nullStrModel struct {
	grove.BaseModel `grove:"table:nullstr"`
	ID              string `grove:"id,pk"`
	ProjectID       string `grove:"project_id,nullzero"` // nullable
	WorkspaceID     string `grove:"workspace_id,notnull"`
}

func fieldByName(t *testing.T, table *schema.Table, name string) *schema.Field {
	t.Helper()
	for _, f := range table.Fields {
		if f.GoName == name {
			return f
		}
	}
	t.Fatalf("field %q not found", name)
	return nil
}

// A nullable string destination coerces SQL NULL to "" instead of failing, and
// still accepts a real value.
func TestFieldPtr_NullableStringScansNull(t *testing.T) {
	table, err := schema.NewTable((*nullStrModel)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	var m nullStrModel
	v := reflect.ValueOf(&m).Elem()

	ptr := FieldPtr(v, fieldByName(t, table, "ProjectID"))
	sc, ok := ptr.(sql.Scanner)
	if !ok {
		t.Fatalf("ProjectID: expected sql.Scanner adapter, got %T", ptr)
	}

	if err := sc.Scan(nil); err != nil {
		t.Fatalf("scan NULL: %v", err)
	}
	if m.ProjectID != "" {
		t.Errorf("after NULL scan, ProjectID = %q, want \"\"", m.ProjectID)
	}

	if err := sc.Scan("proj_123"); err != nil {
		t.Fatalf("scan value: %v", err)
	}
	if m.ProjectID != "proj_123" {
		t.Errorf("after value scan, ProjectID = %q, want %q", m.ProjectID, "proj_123")
	}
}

// A NOT NULL string column keeps the raw *string pointer — it can never be NULL,
// so it needs no adapter (and stays directly modifiable).
func TestFieldPtr_NotNullStringStaysRaw(t *testing.T) {
	table, err := schema.NewTable((*nullStrModel)(nil))
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	var m nullStrModel
	v := reflect.ValueOf(&m).Elem()

	ptr := FieldPtr(v, fieldByName(t, table, "WorkspaceID"))
	p, ok := ptr.(*string)
	if !ok {
		t.Fatalf("WorkspaceID: expected raw *string, got %T", ptr)
	}
	*p = "ws_1"
	if m.WorkspaceID != "ws_1" {
		t.Errorf("WorkspaceID = %q, want %q", m.WorkspaceID, "ws_1")
	}
}
