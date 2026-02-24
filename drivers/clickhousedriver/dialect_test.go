package clickhousedriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/xraph/grove/schema"
)

func TestClickHouseDialect_Name(t *testing.T) {
	d := &ClickHouseDialect{}
	if got := d.Name(); got != "clickhouse" {
		t.Errorf("Name() = %q, want %q", got, "clickhouse")
	}
}

func TestClickHouseDialect_Placeholder(t *testing.T) {
	d := &ClickHouseDialect{}

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

func TestClickHouseDialect_Quote(t *testing.T) {
	d := &ClickHouseDialect{}

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
	}

	for _, tt := range tests {
		got := d.Quote(tt.input)
		if got != tt.want {
			t.Errorf("Quote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestClickHouseDialect_GoToDBType(t *testing.T) {
	d := &ClickHouseDialect{}

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
			opts:   schema.FieldOptions{SQLType: "FixedString(36)"},
			want:   "FixedString(36)",
		},
		// Boolean -> Bool.
		{
			name:   "bool",
			goType: reflect.TypeOf(false),
			opts:   schema.FieldOptions{},
			want:   "Bool",
		},
		// Integer types.
		{
			name:   "int",
			goType: reflect.TypeOf(int(0)),
			opts:   schema.FieldOptions{},
			want:   "Int64",
		},
		{
			name:   "int8",
			goType: reflect.TypeOf(int8(0)),
			opts:   schema.FieldOptions{},
			want:   "Int8",
		},
		{
			name:   "int16",
			goType: reflect.TypeOf(int16(0)),
			opts:   schema.FieldOptions{},
			want:   "Int16",
		},
		{
			name:   "int32",
			goType: reflect.TypeOf(int32(0)),
			opts:   schema.FieldOptions{},
			want:   "Int32",
		},
		{
			name:   "int64",
			goType: reflect.TypeOf(int64(0)),
			opts:   schema.FieldOptions{},
			want:   "Int64",
		},
		// Unsigned integer types.
		{
			name:   "uint8",
			goType: reflect.TypeOf(uint8(0)),
			opts:   schema.FieldOptions{},
			want:   "UInt8",
		},
		{
			name:   "uint16",
			goType: reflect.TypeOf(uint16(0)),
			opts:   schema.FieldOptions{},
			want:   "UInt16",
		},
		{
			name:   "uint32",
			goType: reflect.TypeOf(uint32(0)),
			opts:   schema.FieldOptions{},
			want:   "UInt32",
		},
		{
			name:   "uint64",
			goType: reflect.TypeOf(uint64(0)),
			opts:   schema.FieldOptions{},
			want:   "UInt64",
		},
		// Float types.
		{
			name:   "float32",
			goType: reflect.TypeOf(float32(0)),
			opts:   schema.FieldOptions{},
			want:   "Float32",
		},
		{
			name:   "float64",
			goType: reflect.TypeOf(float64(0)),
			opts:   schema.FieldOptions{},
			want:   "Float64",
		},
		// String types.
		{
			name:   "string",
			goType: reflect.TypeOf(""),
			opts:   schema.FieldOptions{},
			want:   "String",
		},
		// Time types.
		{
			name:   "time.Time",
			goType: reflect.TypeOf(time.Time{}),
			opts:   schema.FieldOptions{},
			want:   "DateTime64(3)",
		},
		{
			name:   "*time.Time nullable",
			goType: reflect.TypeOf((*time.Time)(nil)),
			opts:   schema.FieldOptions{},
			want:   "Nullable(DateTime64(3))",
		},
		// Byte slice -> String.
		{
			name:   "[]byte",
			goType: reflect.TypeOf([]byte{}),
			opts:   schema.FieldOptions{},
			want:   "String",
		},
		// Map type -> String (JSON stored as String).
		{
			name:   "map[string]any",
			goType: reflect.TypeOf(map[string]any{}),
			opts:   schema.FieldOptions{},
			want:   "String",
		},
		// Unknown type falls back to String.
		{
			name:   "struct",
			goType: reflect.TypeOf(struct{}{}),
			opts:   schema.FieldOptions{},
			want:   "String",
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

func TestClickHouseDialect_AppendTime(t *testing.T) {
	d := &ClickHouseDialect{}

	// Use a fixed time in UTC.
	ts := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-01-15 12:30:45'"

	if got != want {
		t.Errorf("AppendTime = %q, want %q", got, want)
	}
}

func TestClickHouseDialect_AppendTime_WithMillis(t *testing.T) {
	d := &ClickHouseDialect{}

	ts := time.Date(2024, 6, 15, 14, 0, 0, 123000000, time.UTC)

	got := string(d.AppendTime(nil, ts))
	want := "'2024-06-15 14:00:00.123'"

	if got != want {
		t.Errorf("AppendTime with millis = %q, want %q", got, want)
	}
}

func TestClickHouseDialect_AppendBytes(t *testing.T) {
	d := &ClickHouseDialect{}

	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "simple bytes",
			input: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:  `unhex('deadbeef')`,
		},
		{
			name:  "empty bytes",
			input: []byte{},
			want:  `unhex('')`,
		},
		{
			name:  "hello",
			input: []byte("hello"),
			want:  `unhex('68656c6c6f')`,
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

func TestClickHouseDialect_AppendBytes_Appends(t *testing.T) {
	d := &ClickHouseDialect{}

	prefix := []byte("SELECT ")
	got := string(d.AppendBytes(prefix, []byte{0xCA, 0xFE}))
	want := `SELECT unhex('cafe')`
	if got != want {
		t.Errorf("AppendBytes with prefix = %q, want %q", got, want)
	}
}
