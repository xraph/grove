package pgdriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove/schema"
)

func TestPgDialect_Name(t *testing.T) {
	d := &PgDialect{}
	if got := d.Name(); got != "pg" {
		t.Errorf("Name() = %q, want %q", got, "pg")
	}
}

func TestPgDialect_Placeholder(t *testing.T) {
	d := &PgDialect{}

	tests := []struct {
		n    int
		want string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
		{100, "$100"},
	}

	for _, tt := range tests {
		got := d.Placeholder(tt.n)
		if got != tt.want {
			t.Errorf("Placeholder(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestPgDialect_Quote(t *testing.T) {
	d := &PgDialect{}

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

func TestPgDialect_GoToDBType(t *testing.T) {
	d := &PgDialect{}

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
			opts:   schema.FieldOptions{SQLType: "uuid"},
			want:   "uuid",
		},
		// Boolean.
		{
			name:   "bool",
			goType: reflect.TypeOf(false),
			opts:   schema.FieldOptions{},
			want:   "boolean",
		},
		// Integer types.
		{
			name:   "int",
			goType: reflect.TypeOf(int(0)),
			opts:   schema.FieldOptions{},
			want:   "integer",
		},
		{
			name:   "int32",
			goType: reflect.TypeOf(int32(0)),
			opts:   schema.FieldOptions{},
			want:   "integer",
		},
		{
			name:   "int64",
			goType: reflect.TypeOf(int64(0)),
			opts:   schema.FieldOptions{},
			want:   "bigint",
		},
		{
			name:   "int16",
			goType: reflect.TypeOf(int16(0)),
			opts:   schema.FieldOptions{},
			want:   "smallint",
		},
		{
			name:   "int8",
			goType: reflect.TypeOf(int8(0)),
			opts:   schema.FieldOptions{},
			want:   "smallint",
		},
		// Float types.
		{
			name:   "float32",
			goType: reflect.TypeOf(float32(0)),
			opts:   schema.FieldOptions{},
			want:   "real",
		},
		{
			name:   "float64",
			goType: reflect.TypeOf(float64(0)),
			opts:   schema.FieldOptions{},
			want:   "double precision",
		},
		// String types.
		{
			name:   "string",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{},
			want:   "text",
		},
		{
			name:   "string with unique",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{Unique: true},
			want:   "varchar(255)",
		},
		// Time types.
		{
			name:   "time.Time",
			goType: reflect.TypeOf(time.Time{}),
			opts:   schema.FieldOptions{},
			want:   "timestamptz",
		},
		{
			name:   "*time.Time",
			goType: reflect.TypeOf((*time.Time)(nil)),
			opts:   schema.FieldOptions{},
			want:   "timestamptz",
		},
		// Byte slice.
		{
			name:   "[]byte",
			goType: reflect.TypeOf([]byte{}),
			opts:   schema.FieldOptions{},
			want:   "bytea",
		},
		// Map type -> jsonb.
		{
			name:   "map[string]any",
			goType: reflect.TypeOf(map[string]any{}),
			opts:   schema.FieldOptions{},
			want:   "jsonb",
		},
		// Unknown type falls back to text.
		{
			name:   "struct",
			goType: reflect.TypeOf(struct{}{}),
			opts:   schema.FieldOptions{},
			want:   "text",
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

func TestPgDialect_AppendTime(t *testing.T) {
	d := &PgDialect{}

	// Use a fixed time in UTC.
	ts := time.Date(2024, 1, 15, 12, 30, 45, 123456789, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-01-15T12:30:45.123456789Z'"

	if got != want {
		t.Errorf("AppendTime = %q, want %q", got, want)
	}
}

func TestPgDialect_AppendTime_WithTimezone(t *testing.T) {
	d := &PgDialect{}

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

func TestPgDialect_AppendBytes(t *testing.T) {
	d := &PgDialect{}

	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "simple bytes",
			input: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:  `'\xdeadbeef'`,
		},
		{
			name:  "empty bytes",
			input: []byte{},
			want:  `'\x'`,
		},
		{
			name:  "hello",
			input: []byte("hello"),
			want:  `'\x68656c6c6f'`,
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

func TestPgDialect_AppendBytes_Appends(t *testing.T) {
	d := &PgDialect{}

	prefix := []byte("SELECT ")
	got := string(d.AppendBytes(prefix, []byte{0xCA, 0xFE}))
	want := `SELECT '\xcafe'`
	if got != want {
		t.Errorf("AppendBytes with prefix = %q, want %q", got, want)
	}
}
