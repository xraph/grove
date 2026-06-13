package scan

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/schema"
)

// ---------- Test models for convert tests ----------

type ConvUser struct {
	grove.BaseModel `grove:"table:users"`

	ID    int64  `grove:"id,pk,autoincrement"`
	Name  string `grove:"name,notnull"`
	Email string `grove:"email,notnull"`
}

type ConvEmbedded struct {
	Audit
	grove.BaseModel `grove:"table:embedded"`

	ID   int64  `grove:"id,pk"`
	Name string `grove:"name"`
}

type Audit struct {
	CreatedAt time.Time `grove:"created_at,notnull"`
	UpdatedAt time.Time `grove:"updated_at,notnull"`
}

// ---------- FieldPtr tests ----------

func TestFieldPtr_DirectFields(t *testing.T) {
	table, err := schema.NewTable((*ConvUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	user := ConvUser{
		ID:    42,
		Name:  "Alice",
		Email: "alice@example.com",
	}
	v := reflect.ValueOf(&user).Elem()

	for _, field := range table.Fields {
		ptr := FieldPtr(v, field)
		if ptr == nil {
			t.Errorf("FieldPtr returned nil for field %q", field.GoName)
			continue
		}

		switch field.GoName {
		case "ID":
			p, ok := ptr.(*int64)
			if !ok {
				t.Errorf("ID: expected *int64, got %T", ptr)
				continue
			}
			if *p != 42 {
				t.Errorf("ID: *p = %d, want 42", *p)
			}
		case "Name":
			p, ok := ptr.(*string)
			if !ok {
				t.Errorf("Name: expected *string, got %T", ptr)
				continue
			}
			if *p != "Alice" {
				t.Errorf("Name: *p = %q, want %q", *p, "Alice")
			}
		case "Email":
			p, ok := ptr.(*string)
			if !ok {
				t.Errorf("Email: expected *string, got %T", ptr)
				continue
			}
			if *p != "alice@example.com" {
				t.Errorf("Email: *p = %q, want %q", *p, "alice@example.com")
			}
		}
	}
}

func TestFieldPtr_CanModify(t *testing.T) {
	table, err := schema.NewTable((*ConvUser)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	var user ConvUser
	v := reflect.ValueOf(&user).Elem()

	// Find the Name field and modify it through the pointer.
	for _, field := range table.Fields {
		if field.GoName == "Name" {
			ptr := FieldPtr(v, field)
			p := ptr.(*string)
			*p = "Modified"
			break
		}
	}

	if user.Name != "Modified" {
		t.Errorf("Name = %q, want %q", user.Name, "Modified")
	}
}

func TestFieldPtr_NestedStructFields(t *testing.T) {
	table, err := schema.NewTable((*ConvEmbedded)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	item := ConvEmbedded{
		Audit: Audit{
			CreatedAt: now,
			UpdatedAt: now,
		},
		ID:   99,
		Name: "Nested",
	}
	v := reflect.ValueOf(&item).Elem()

	// Build field map for easy lookup.
	fieldMap := make(map[string]*schema.Field)
	for _, f := range table.Fields {
		fieldMap[f.GoName] = f
	}

	// Test accessing the nested CreatedAt field. Time destinations are
	// returned as sql.Scanner adapters; writing through one must land on
	// the nested field.
	if f, ok := fieldMap["CreatedAt"]; ok {
		ptr := FieldPtr(v, f)
		sc, ok := ptr.(sql.Scanner)
		if !ok {
			t.Fatalf("CreatedAt: expected sql.Scanner, got %T", ptr)
		}
		later := now.Add(time.Hour)
		if err := sc.Scan(later); err != nil {
			t.Fatalf("CreatedAt: scan: %v", err)
		}
		if !item.CreatedAt.Equal(later) {
			t.Errorf("CreatedAt = %v, want %v", item.CreatedAt, later)
		}
	} else {
		t.Fatal("CreatedAt field not found in table")
	}

	// Test accessing the direct ID field.
	if f, ok := fieldMap["ID"]; ok {
		ptr := FieldPtr(v, f)
		p, ok := ptr.(*int64)
		if !ok {
			t.Fatalf("ID: expected *int64, got %T", ptr)
		}
		if *p != 99 {
			t.Errorf("ID = %d, want 99", *p)
		}
	} else {
		t.Fatal("ID field not found in table")
	}

	// Test modifying nested field through the scanner adapter.
	if f, ok := fieldMap["UpdatedAt"]; ok {
		sc := FieldPtr(v, f).(sql.Scanner)
		newTime := now.Add(time.Hour)
		if err := sc.Scan(newTime); err != nil {
			t.Fatalf("UpdatedAt: scan: %v", err)
		}
		if !item.UpdatedAt.Equal(newTime) {
			t.Errorf("UpdatedAt = %v, want %v", item.UpdatedAt, newTime)
		}
	}
}

// ---------- IsNilable tests ----------

func TestIsNilable(t *testing.T) {
	tests := []struct {
		name string
		typ  reflect.Type
		want bool
	}{
		{
			name: "pointer",
			typ:  reflect.TypeOf((*int)(nil)),
			want: true,
		},
		{
			name: "interface",
			typ:  reflect.TypeOf((*error)(nil)).Elem(),
			want: true,
		},
		{
			name: "slice",
			typ:  reflect.TypeOf([]int{}),
			want: true,
		},
		{
			name: "map",
			typ:  reflect.TypeOf(map[string]int{}),
			want: true,
		},
		{
			name: "chan",
			typ:  reflect.TypeOf(make(chan int)),
			want: true,
		},
		{
			name: "func",
			typ:  reflect.TypeOf(func() {}),
			want: true,
		},
		{
			name: "int",
			typ:  reflect.TypeOf(0),
			want: false,
		},
		{
			name: "string",
			typ:  reflect.TypeOf(""),
			want: false,
		},
		{
			name: "struct",
			typ:  reflect.TypeOf(time.Time{}),
			want: false,
		},
		{
			name: "bool",
			typ:  reflect.TypeOf(false),
			want: false,
		},
		{
			name: "float64",
			typ:  reflect.TypeOf(0.0),
			want: false,
		},
		{
			name: "array",
			typ:  reflect.TypeOf([3]int{}),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsNilable(tt.typ)
			if got != tt.want {
				t.Errorf("IsNilable(%v) = %v, want %v", tt.typ, got, tt.want)
			}
		})
	}
}

// ---------- time destination wrapping ----------

type ConvTimed struct {
	grove.BaseModel `grove:"table:timed"`

	ID    int64      `grove:"id,pk"`
	At    time.Time  `grove:"at,notnull"`
	Maybe *time.Time `grove:"maybe"`
}

// TestFieldPtr_TimeFromString locks in the TEXT-driver contract: sqlite and
// turso store timestamps as RFC3339 strings, so time destinations must be
// returned as sql.Scanner implementations that parse them. Without this,
// every model read with a time field fails on those drivers.
func TestFieldPtr_TimeFromString(t *testing.T) {
	table, err := schema.NewTable((*ConvTimed)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	var m ConvTimed
	v := reflect.ValueOf(&m).Elem()
	want := time.Date(2026, 6, 12, 10, 30, 0, 500_000_000, time.UTC)

	for _, field := range table.Fields {
		ptr := FieldPtr(v, field)
		switch field.GoName {
		case "At", "Maybe":
			sc, ok := ptr.(sql.Scanner)
			if !ok {
				t.Fatalf("%s: expected sql.Scanner dest for time field, got %T", field.GoName, ptr)
			}
			if scanErr := sc.Scan("2026-06-12T10:30:00.5Z"); scanErr != nil {
				t.Fatalf("%s: scan string: %v", field.GoName, scanErr)
			}
		}
	}

	if !m.At.Equal(want) {
		t.Errorf("At = %v, want %v", m.At, want)
	}
	if m.Maybe == nil || !m.Maybe.Equal(want) {
		t.Errorf("Maybe = %v, want %v", m.Maybe, want)
	}
}

// TestFieldPtr_TimePassthroughAndNil verifies drivers that already produce
// time.Time (postgres, clickhouse) pass through unchanged, []byte sources
// parse, and NULL clears the destination.
func TestFieldPtr_TimePassthroughAndNil(t *testing.T) {
	table, err := schema.NewTable((*ConvTimed)(nil))
	if err != nil {
		t.Fatalf("NewTable failed: %v", err)
	}

	stale := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	m := ConvTimed{Maybe: &stale}
	v := reflect.ValueOf(&m).Elem()
	want := time.Date(2026, 6, 12, 10, 30, 0, 0, time.UTC)

	for _, field := range table.Fields {
		switch field.GoName {
		case "At":
			sc := FieldPtr(v, field).(sql.Scanner)
			if err := sc.Scan(want); err != nil {
				t.Fatalf("At: scan time.Time: %v", err)
			}
		case "Maybe":
			sc := FieldPtr(v, field).(sql.Scanner)
			if err := sc.Scan(nil); err != nil {
				t.Fatalf("Maybe: scan nil: %v", err)
			}
		}
	}

	if !m.At.Equal(want) {
		t.Errorf("At = %v, want %v", m.At, want)
	}
	if m.Maybe != nil {
		t.Errorf("Maybe = %v, want nil after NULL scan", m.Maybe)
	}

	// []byte sources (some drivers hand TEXT back as bytes) must parse too.
	for _, field := range table.Fields {
		if field.GoName == "At" {
			sc := FieldPtr(v, field).(sql.Scanner)
			if err := sc.Scan([]byte("2026-06-12T10:30:00Z")); err != nil {
				t.Fatalf("At: scan []byte: %v", err)
			}
		}
	}
}
