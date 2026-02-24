package tursodriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove/schema"
)

func TestTursoDialect_Name(t *testing.T) {
	d := &TursoDialect{}
	if got := d.Name(); got != "turso" {
		t.Errorf("Name() = %q, want %q", got, "turso")
	}
}

func TestTursoDialect_Placeholder(t *testing.T) {
	d := &TursoDialect{}

	tests := []struct {
		n    int
		want string
	}{
		{1, "?"},
		{2, "?"},
		{10, "?"},
		{100, "?"},
	}

	for _, tt := range tests {
		got := d.Placeholder(tt.n)
		if got != tt.want {
			t.Errorf("Placeholder(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestTursoDialect_Quote(t *testing.T) {
	d := &TursoDialect{}

	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"my_table", `"my_table"`},
		{`my"table`, `"my""table"`},
		{`a""b`, `"a""""b"`},
		{"", `""`},
		{"CamelCase", `"CamelCase"`},
	}

	for _, tt := range tests {
		got := d.Quote(tt.input)
		if got != tt.want {
			t.Errorf("Quote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestTursoDialect_GoToDBType(t *testing.T) {
	d := &TursoDialect{}

	tests := []struct {
		name   string
		goType reflect.Type
		opts   schema.FieldOptions
		want   string
	}{
		// Explicit SQL type takes precedence.
		{
			name:   "explicit sql type",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{SQLType: "VARCHAR(100)"},
			want:   "VARCHAR(100)",
		},
		// Boolean -> INTEGER.
		{
			name:   "bool",
			goType: reflect.TypeOf(false),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		// Integer types -> INTEGER.
		{
			name:   "int",
			goType: reflect.TypeOf(int(0)),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		{
			name:   "int32",
			goType: reflect.TypeOf(int32(0)),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		{
			name:   "int64",
			goType: reflect.TypeOf(int64(0)),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		{
			name:   "int16",
			goType: reflect.TypeOf(int16(0)),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		{
			name:   "int8",
			goType: reflect.TypeOf(int8(0)),
			opts:   schema.FieldOptions{},
			want:   "INTEGER",
		},
		// Float types -> REAL.
		{
			name:   "float32",
			goType: reflect.TypeOf(float32(0)),
			opts:   schema.FieldOptions{},
			want:   "REAL",
		},
		{
			name:   "float64",
			goType: reflect.TypeOf(float64(0)),
			opts:   schema.FieldOptions{},
			want:   "REAL",
		},
		// String types -> TEXT.
		{
			name:   "string",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{},
			want:   "TEXT",
		},
		{
			name:   "string with unique",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{Unique: true},
			want:   "TEXT",
		},
		// Time types -> TEXT (RFC3339).
		{
			name:   "time.Time",
			goType: reflect.TypeOf(time.Time{}),
			opts:   schema.FieldOptions{},
			want:   "TEXT",
		},
		{
			name:   "*time.Time",
			goType: reflect.TypeOf((*time.Time)(nil)),
			opts:   schema.FieldOptions{},
			want:   "TEXT",
		},
		// Byte slice -> BLOB.
		{
			name:   "[]byte",
			goType: reflect.TypeOf([]byte{}),
			opts:   schema.FieldOptions{},
			want:   "BLOB",
		},
		// Map type -> TEXT (JSON stored as text).
		{
			name:   "map[string]any",
			goType: reflect.TypeOf(map[string]any{}),
			opts:   schema.FieldOptions{},
			want:   "TEXT",
		},
		// Unknown type falls back to TEXT.
		{
			name:   "struct",
			goType: reflect.TypeOf(struct{}{}),
			opts:   schema.FieldOptions{},
			want:   "TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := d.GoToDBType(tt.goType, tt.opts)
			if got != tt.want {
				t.Errorf("GoToDBType(%v, %+v) = %q, want %q", tt.goType, tt.opts, got, tt.want)
			}
		})
	}
}

func TestTursoDialect_AppendTime(t *testing.T) {
	d := &TursoDialect{}

	// Use a fixed time in UTC.
	ts := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-01-15T12:30:45Z'"

	if got != want {
		t.Errorf("AppendTime = %q, want %q", got, want)
	}
}

func TestTursoDialect_AppendTime_WithTimezone(t *testing.T) {
	d := &TursoDialect{}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("cannot load timezone: %v", err)
	}
	ts := time.Date(2024, 6, 15, 14, 0, 0, 0, loc)

	got := string(d.AppendTime(nil, ts))
	// Should contain the timezone offset.
	if len(got) < 2 || got[0] != '\'' || got[len(got)-1] != '\'' {
		t.Errorf("AppendTime result should be single-quoted, got %q", got)
	}
}

func TestTursoDialect_AppendBytes(t *testing.T) {
	d := &TursoDialect{}

	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "simple bytes",
			input: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:  `X'deadbeef'`,
		},
		{
			name:  "empty bytes",
			input: []byte{},
			want:  `X''`,
		},
		{
			name:  "hello",
			input: []byte("hello"),
			want:  `X'68656c6c6f'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(d.AppendBytes(nil, tt.input))
			if got != tt.want {
				t.Errorf("AppendBytes(%x) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestTursoDialect_AppendBytes_Appends(t *testing.T) {
	d := &TursoDialect{}

	prefix := []byte("SELECT ")
	got := string(d.AppendBytes(prefix, []byte{0xCA, 0xFE}))
	want := `SELECT X'cafe'`
	if got != want {
		t.Errorf("AppendBytes with prefix = %q, want %q", got, want)
	}
}
