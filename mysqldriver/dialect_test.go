package mysqldriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove/schema"
)

func TestMysqlDialect_Name(t *testing.T) {
	d := &MysqlDialect{}
	if got := d.Name(); got != "mysql" {
		t.Errorf("Name() = %q, want %q", got, "mysql")
	}
}

func TestMysqlDialect_Placeholder(t *testing.T) {
	d := &MysqlDialect{}

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

func TestMysqlDialect_Quote(t *testing.T) {
	d := &MysqlDialect{}

	tests := []struct {
		input string
		want  string
	}{
		{"users", "`users`"},
		{"my_table", "`my_table`"},
		{"my`table", "`my``table`"},
		{"a``b", "`a````b`"},
		{"", "``"},
		{"CamelCase", "`CamelCase`"},
		{"group", "`group`"}, // reserved word
	}

	for _, tt := range tests {
		got := d.Quote(tt.input)
		if got != tt.want {
			t.Errorf("Quote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestMysqlDialect_GoToDBType(t *testing.T) {
	d := &MysqlDialect{}

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
			opts:   schema.FieldOptions{SQLType: "CHAR(36)"},
			want:   "CHAR(36)",
		},
		// Boolean.
		{
			name:   "bool",
			goType: reflect.TypeOf(false),
			opts:   schema.FieldOptions{},
			want:   "BOOLEAN",
		},
		// Integer types.
		{
			name:   "int",
			goType: reflect.TypeOf(int(0)),
			opts:   schema.FieldOptions{},
			want:   "INT",
		},
		{
			name:   "int32",
			goType: reflect.TypeOf(int32(0)),
			opts:   schema.FieldOptions{},
			want:   "INT",
		},
		{
			name:   "int64",
			goType: reflect.TypeOf(int64(0)),
			opts:   schema.FieldOptions{},
			want:   "BIGINT",
		},
		{
			name:   "int16",
			goType: reflect.TypeOf(int16(0)),
			opts:   schema.FieldOptions{},
			want:   "SMALLINT",
		},
		{
			name:   "int8",
			goType: reflect.TypeOf(int8(0)),
			opts:   schema.FieldOptions{},
			want:   "TINYINT",
		},
		// Float types.
		{
			name:   "float32",
			goType: reflect.TypeOf(float32(0)),
			opts:   schema.FieldOptions{},
			want:   "FLOAT",
		},
		{
			name:   "float64",
			goType: reflect.TypeOf(float64(0)),
			opts:   schema.FieldOptions{},
			want:   "DOUBLE",
		},
		// String types.
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
			want:   "VARCHAR(255)",
		},
		// Time types.
		{
			name:   "time.Time",
			goType: reflect.TypeOf(time.Time{}),
			opts:   schema.FieldOptions{},
			want:   "DATETIME(6)",
		},
		{
			name:   "*time.Time",
			goType: reflect.TypeOf((*time.Time)(nil)),
			opts:   schema.FieldOptions{},
			want:   "DATETIME(6)",
		},
		// Byte slice.
		{
			name:   "[]byte",
			goType: reflect.TypeOf([]byte{}),
			opts:   schema.FieldOptions{},
			want:   "BLOB",
		},
		// Map type -> JSON.
		{
			name:   "map[string]any",
			goType: reflect.TypeOf(map[string]any{}),
			opts:   schema.FieldOptions{},
			want:   "JSON",
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

func TestMysqlDialect_AppendTime(t *testing.T) {
	d := &MysqlDialect{}

	// Use a fixed time in UTC.
	ts := time.Date(2024, 1, 15, 12, 30, 45, 123456000, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-01-15 12:30:45.123456'"

	if got != want {
		t.Errorf("AppendTime = %q, want %q", got, want)
	}
}

func TestMysqlDialect_AppendTime_NoFractional(t *testing.T) {
	d := &MysqlDialect{}

	ts := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-06-15 14:00:00'"

	if got != want {
		t.Errorf("AppendTime = %q, want %q", got, want)
	}
}

func TestMysqlDialect_AppendBytes(t *testing.T) {
	d := &MysqlDialect{}

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

func TestMysqlDialect_AppendBytes_Appends(t *testing.T) {
	d := &MysqlDialect{}

	prefix := []byte("SELECT ")
	got := string(d.AppendBytes(prefix, []byte{0xCA, 0xFE}))
	want := `SELECT X'cafe'`
	if got != want {
		t.Errorf("AppendBytes with prefix = %q, want %q", got, want)
	}
}
