package schema

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove"
)

// ---------- Test model structs ----------

type TestUser struct {
	grove.BaseModel `grove:"table:users,alias:u"`

	ID        int64      `grove:"id,pk,autoincrement"`
	Name      string     `grove:"name,notnull"`
	Email     string     `grove:"email,notnull,unique"`
	SSN       string     `grove:"ssn,privacy:pii"`
	DeletedAt *time.Time `grove:"deleted_at,soft_delete"`
}

type TestPost struct {
	grove.BaseModel `grove:"table:posts"`

	ID     int64  `grove:"id,pk,autoincrement"`
	Title  string `grove:"title,notnull"`
	Body   string `grove:"body"`
	UserID int64  `grove:"user_id,notnull"`
}

type TestNoBaseModel struct {
	ID   int64  `grove:"id,pk"`
	Name string `grove:"name"`
}

type TestBunModel struct {
	grove.BaseModel `bun:"table:bun_items,alias:bi"`

	ID   int64  `bun:"id,pk,autoincrement"`
	Name string `bun:"name,notnull"`
}

type TestSkipField struct {
	grove.BaseModel `grove:"table:skip_test"`

	ID       int64  `grove:"id,pk"`
	Internal string `grove:"-"`
	Name     string `grove:"name"`
}

type TestEmbedded struct {
	Timestamps
	grove.BaseModel `grove:"table:embedded_test"`

	ID   int64  `grove:"id,pk"`
	Name string `grove:"name"`
}

type Timestamps struct {
	CreatedAt time.Time  `grove:"created_at,notnull"`
	UpdatedAt time.Time  `grove:"updated_at,notnull"`
	DeletedAt *time.Time `grove:"deleted_at,soft_delete"`
}

type TestRelationModel struct {
	grove.BaseModel `grove:"table:authors"`

	ID    int64      `grove:"id,pk,autoincrement"`
	Name  string     `grove:"name,notnull"`
	Posts []TestPost `grove:"rel:has-many,join:id=user_id"`
}

// ---------- NewTable tests ----------

func TestNewTable(t *testing.T) {
	tests := []struct {
		name           string
		model          any
		wantTableName  string
		wantAlias      string
		wantFieldCount int
		wantPKCount    int
		wantSoftDelete bool
		wantErr        bool
	}{
		{
			name:           "full user model",
			model:          (*TestUser)(nil),
			wantTableName:  "users",
			wantAlias:      "u",
			wantFieldCount: 5,
			wantPKCount:    1,
			wantSoftDelete: true,
		},
		{
			name:           "post model",
			model:          (*TestPost)(nil),
			wantTableName:  "posts",
			wantAlias:      "",
			wantFieldCount: 4,
			wantPKCount:    1,
			wantSoftDelete: false,
		},
		{
			name:           "no base model uses default naming",
			model:          (*TestNoBaseModel)(nil),
			wantTableName:  "test_no_base_models",
			wantAlias:      "",
			wantFieldCount: 2,
			wantPKCount:    1,
			wantSoftDelete: false,
		},
		{
			name:           "bun fallback model",
			model:          (*TestBunModel)(nil),
			wantTableName:  "bun_items",
			wantAlias:      "bi",
			wantFieldCount: 2,
			wantPKCount:    1,
			wantSoftDelete: false,
		},
		{
			name:           "skip field model",
			model:          (*TestSkipField)(nil),
			wantTableName:  "skip_test",
			wantAlias:      "",
			wantFieldCount: 2, // ID and Name; Internal is skipped
			wantPKCount:    1,
			wantSoftDelete: false,
		},
		{
			name:    "nil model",
			model:   nil,
			wantErr: true,
		},
		{
			name:    "non-struct model",
			model:   (*int)(nil),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, err := NewTable(tt.model)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if table.Name != tt.wantTableName {
				t.Errorf("Name = %q, want %q", table.Name, tt.wantTableName)
			}
			if table.Alias != tt.wantAlias {
				t.Errorf("Alias = %q, want %q", table.Alias, tt.wantAlias)
			}
			if len(table.Fields) != tt.wantFieldCount {
				t.Errorf("Fields count = %d, want %d", len(table.Fields), tt.wantFieldCount)
				for i, f := range table.Fields {
					t.Logf("  field[%d]: %s (col=%s)", i, f.GoName, f.Options.Column)
				}
			}
			if len(table.PKFields) != tt.wantPKCount {
				t.Errorf("PKFields count = %d, want %d", len(table.PKFields), tt.wantPKCount)
			}
			if tt.wantSoftDelete && table.SoftDelete == nil {
				t.Error("expected SoftDelete field to be set")
			}
			if !tt.wantSoftDelete && table.SoftDelete != nil {
				t.Error("expected SoftDelete field to be nil")
			}
		})
	}
}

func TestNewTableModelType(t *testing.T) {
	table, err := NewTable((*TestUser)(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table.ModelType != reflect.TypeOf(TestUser{}) {
		t.Errorf("ModelType = %v, want %v", table.ModelType, reflect.TypeOf(TestUser{}))
	}
}

func TestNewTableFieldDetails(t *testing.T) {
	table, err := NewTable((*TestUser)(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check specific field details.
	fieldMap := make(map[string]*Field)
	for _, f := range table.Fields {
		fieldMap[f.GoName] = f
	}

	// ID field.
	idField, ok := fieldMap["ID"]
	if !ok {
		t.Fatal("ID field not found")
	}
	if !idField.Options.IsPK {
		t.Error("ID: expected IsPK")
	}
	if !idField.Options.AutoIncrement {
		t.Error("ID: expected AutoIncrement")
	}

	// Email field.
	emailField, ok := fieldMap["Email"]
	if !ok {
		t.Fatal("Email field not found")
	}
	if !emailField.Options.Unique {
		t.Error("Email: expected Unique")
	}
	if !emailField.Options.NotNull {
		t.Error("Email: expected NotNull")
	}

	// SSN field.
	ssnField, ok := fieldMap["SSN"]
	if !ok {
		t.Fatal("SSN field not found")
	}
	if ssnField.Options.Privacy != "pii" {
		t.Errorf("SSN: Privacy = %q, want %q", ssnField.Options.Privacy, "pii")
	}

	// DeletedAt field.
	delField, ok := fieldMap["DeletedAt"]
	if !ok {
		t.Fatal("DeletedAt field not found")
	}
	if !delField.Options.SoftDelete {
		t.Error("DeletedAt: expected SoftDelete")
	}
}

func TestNewTableEmbeddedStruct(t *testing.T) {
	table, err := NewTable((*TestEmbedded)(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if table.Name != "embedded_test" {
		t.Errorf("Name = %q, want %q", table.Name, "embedded_test")
	}

	// Should have ID, Name from TestEmbedded + CreatedAt, UpdatedAt, DeletedAt from Timestamps = 5.
	if len(table.Fields) != 5 {
		t.Errorf("Fields count = %d, want 5", len(table.Fields))
		for i, f := range table.Fields {
			t.Logf("  field[%d]: %s (col=%s)", i, f.GoName, f.Options.Column)
		}
	}

	if table.SoftDelete == nil {
		t.Error("expected SoftDelete from embedded Timestamps")
	}
}

func TestNewTableRelations(t *testing.T) {
	table, err := NewTable((*TestRelationModel)(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(table.Relations) != 1 {
		t.Fatalf("Relations count = %d, want 1", len(table.Relations))
	}

	rel := table.Relations[0]
	if rel.Type != HasMany {
		t.Errorf("Relation Type = %v, want HasMany", rel.Type)
	}
	if rel.BaseColumn != "id" {
		t.Errorf("BaseColumn = %q, want %q", rel.BaseColumn, "id")
	}
	if rel.JoinColumn != "user_id" {
		t.Errorf("JoinColumn = %q, want %q", rel.JoinColumn, "user_id")
	}
}

func TestNewTableValueTypes(t *testing.T) {
	// Ensure we can pass value types, not just pointers.
	table, err := NewTable(TestUser{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if table.Name != "users" {
		t.Errorf("Name = %q, want %q", table.Name, "users")
	}
}
