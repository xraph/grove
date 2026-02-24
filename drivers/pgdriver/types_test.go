package pgdriver

import (
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------
// JSONMap tests
// ---------------------------------------------------------------------------

func TestJSONMap_Value(t *testing.T) {
	m := JSONMap{"key": "value", "num": float64(42)}
	v, err := m.Value()
	if err != nil {
		t.Fatalf("JSONMap.Value() error = %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("JSONMap.Value() returned %T, want []byte", v)
	}

	// Unmarshal to verify valid JSON.
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	if decoded["key"] != "value" {
		t.Errorf("expected key=value, got key=%v", decoded["key"])
	}
	if decoded["num"] != float64(42) {
		t.Errorf("expected num=42, got num=%v", decoded["num"])
	}
}

func TestJSONMap_Value_Nil(t *testing.T) {
	var m JSONMap
	v, err := m.Value()
	if err != nil {
		t.Fatalf("JSONMap.Value() error = %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for nil JSONMap, got %v", v)
	}
}

func TestJSONMap_Scan_Bytes(t *testing.T) {
	var m JSONMap
	input := []byte(`{"hello":"world","count":3}`)
	if err := m.Scan(input); err != nil {
		t.Fatalf("JSONMap.Scan([]byte) error = %v", err)
	}
	if m["hello"] != "world" {
		t.Errorf("expected hello=world, got %v", m["hello"])
	}
	if m["count"] != float64(3) {
		t.Errorf("expected count=3, got %v", m["count"])
	}
}

func TestJSONMap_Scan_String(t *testing.T) {
	var m JSONMap
	if err := m.Scan(`{"a":"b"}`); err != nil {
		t.Fatalf("JSONMap.Scan(string) error = %v", err)
	}
	if m["a"] != "b" {
		t.Errorf("expected a=b, got %v", m["a"])
	}
}

func TestJSONMap_Scan_Nil(t *testing.T) {
	m := JSONMap{"existing": "data"}
	if err := m.Scan(nil); err != nil {
		t.Fatalf("JSONMap.Scan(nil) error = %v", err)
	}
	if m != nil {
		t.Errorf("expected nil after scanning nil, got %v", m)
	}
}

func TestJSONMap_Scan_UnsupportedType(t *testing.T) {
	var m JSONMap
	if err := m.Scan(42); err == nil {
		t.Error("expected error when scanning unsupported type, got nil")
	}
}

func TestJSONMap_Scan_InvalidJSON(t *testing.T) {
	var m JSONMap
	if err := m.Scan([]byte(`not json`)); err == nil {
		t.Error("expected error when scanning invalid JSON, got nil")
	}
}

func TestJSONMap_RoundTrip(t *testing.T) {
	original := JSONMap{
		"name":   "test",
		"active": true,
		"tags":   []any{"a", "b"},
	}

	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned JSONMap
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if scanned["name"] != "test" {
		t.Errorf("name: got %v, want test", scanned["name"])
	}
	if scanned["active"] != true {
		t.Errorf("active: got %v, want true", scanned["active"])
	}
}

func TestJSONMap_Empty(t *testing.T) {
	m := JSONMap{}
	v, err := m.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned JSONMap
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(scanned) != 0 {
		t.Errorf("expected empty map, got %v", scanned)
	}
}

// ---------------------------------------------------------------------------
// StringArray tests
// ---------------------------------------------------------------------------

func TestStringArray_Value(t *testing.T) {
	tests := []struct {
		name  string
		input StringArray
		want  string
		isNil bool
	}{
		{
			name:  "simple",
			input: StringArray{"a", "b", "c"},
			want:  `{"a","b","c"}`,
		},
		{
			name:  "single element",
			input: StringArray{"hello"},
			want:  `{"hello"}`,
		},
		{
			name:  "empty array",
			input: StringArray{},
			want:  `{}`,
		},
		{
			name:  "nil",
			input: nil,
			isNil: true,
		},
		{
			name:  "with quotes",
			input: StringArray{`say "hello"`},
			want:  `{"say \"hello\""}`,
		},
		{
			name:  "with backslash",
			input: StringArray{`path\to\file`},
			want:  `{"path\\to\\file"}`,
		},
		{
			name:  "with comma",
			input: StringArray{"a,b", "c"},
			want:  `{"a,b","c"}`,
		},
		{
			name:  "with spaces",
			input: StringArray{"hello world", "foo bar"},
			want:  `{"hello world","foo bar"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.input.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.isNil {
				if v != nil {
					t.Errorf("expected nil, got %v", v)
				}
				return
			}
			got, ok := v.(string)
			if !ok {
				t.Fatalf("Value() returned %T, want string", v)
			}
			if got != tt.want {
				t.Errorf("Value() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStringArray_Scan(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  []string
		isNil bool
	}{
		{
			name:  "simple unquoted",
			input: "{a,b,c}",
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "quoted",
			input: `{"hello","world"}`,
			want:  []string{"hello", "world"},
		},
		{
			name:  "empty",
			input: "{}",
			want:  []string{},
		},
		{
			name:  "nil",
			input: nil,
			isNil: true,
		},
		{
			name:  "bytes input",
			input: []byte(`{x,y,z}`),
			want:  []string{"x", "y", "z"},
		},
		{
			name:  "with escaped quote",
			input: `{"say \"hi\""}`,
			want:  []string{`say "hi"`},
		},
		{
			name:  "with escaped backslash",
			input: `{"path\\to"}`,
			want:  []string{`path\to`},
		},
		{
			name:  "single element",
			input: "{hello}",
			want:  []string{"hello"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var a StringArray
			if err := a.Scan(tt.input); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if tt.isNil {
				if a != nil {
					t.Errorf("expected nil, got %v", a)
				}
				return
			}
			if len(a) != len(tt.want) {
				t.Fatalf("Scan() len = %d, want %d; got %v", len(a), len(tt.want), a)
			}
			for i := range tt.want {
				if a[i] != tt.want[i] {
					t.Errorf("element %d: got %q, want %q", i, a[i], tt.want[i])
				}
			}
		})
	}
}

func TestStringArray_Scan_UnsupportedType(t *testing.T) {
	var a StringArray
	if err := a.Scan(42); err == nil {
		t.Error("expected error for unsupported type, got nil")
	}
}

func TestStringArray_Scan_InvalidFormat(t *testing.T) {
	var a StringArray
	if err := a.Scan("not an array"); err == nil {
		t.Error("expected error for invalid format, got nil")
	}
}

func TestStringArray_RoundTrip(t *testing.T) {
	original := StringArray{"hello", "world", "test"}
	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned StringArray
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(scanned) != len(original) {
		t.Fatalf("round-trip len = %d, want %d", len(scanned), len(original))
	}
	for i := range original {
		if scanned[i] != original[i] {
			t.Errorf("round-trip element %d: got %q, want %q", i, scanned[i], original[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Int64Array tests
// ---------------------------------------------------------------------------

func TestInt64Array_Value(t *testing.T) {
	tests := []struct {
		name  string
		input Int64Array
		want  string
		isNil bool
	}{
		{
			name:  "simple",
			input: Int64Array{1, 2, 3},
			want:  "{1,2,3}",
		},
		{
			name:  "single element",
			input: Int64Array{42},
			want:  "{42}",
		},
		{
			name:  "empty",
			input: Int64Array{},
			want:  "{}",
		},
		{
			name:  "nil",
			input: nil,
			isNil: true,
		},
		{
			name:  "negative numbers",
			input: Int64Array{-10, 0, 10},
			want:  "{-10,0,10}",
		},
		{
			name:  "large numbers",
			input: Int64Array{9223372036854775807, -9223372036854775808},
			want:  "{9223372036854775807,-9223372036854775808}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.input.Value()
			if err != nil {
				t.Fatalf("Value() error = %v", err)
			}
			if tt.isNil {
				if v != nil {
					t.Errorf("expected nil, got %v", v)
				}
				return
			}
			got, ok := v.(string)
			if !ok {
				t.Fatalf("Value() returned %T, want string", v)
			}
			if got != tt.want {
				t.Errorf("Value() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInt64Array_Scan(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  []int64
		isNil bool
	}{
		{
			name:  "simple",
			input: "{1,2,3}",
			want:  []int64{1, 2, 3},
		},
		{
			name:  "single element",
			input: "{42}",
			want:  []int64{42},
		},
		{
			name:  "empty",
			input: "{}",
			want:  []int64{},
		},
		{
			name:  "nil",
			input: nil,
			isNil: true,
		},
		{
			name:  "bytes input",
			input: []byte("{10,20,30}"),
			want:  []int64{10, 20, 30},
		},
		{
			name:  "negative",
			input: "{-1,-2,-3}",
			want:  []int64{-1, -2, -3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var a Int64Array
			if err := a.Scan(tt.input); err != nil {
				t.Fatalf("Scan() error = %v", err)
			}
			if tt.isNil {
				if a != nil {
					t.Errorf("expected nil, got %v", a)
				}
				return
			}
			if len(a) != len(tt.want) {
				t.Fatalf("Scan() len = %d, want %d; got %v", len(a), len(tt.want), a)
			}
			for i := range tt.want {
				if a[i] != tt.want[i] {
					t.Errorf("element %d: got %d, want %d", i, a[i], tt.want[i])
				}
			}
		})
	}
}

func TestInt64Array_Scan_UnsupportedType(t *testing.T) {
	var a Int64Array
	if err := a.Scan(3.14); err == nil {
		t.Error("expected error for unsupported type, got nil")
	}
}

func TestInt64Array_Scan_InvalidNumber(t *testing.T) {
	var a Int64Array
	if err := a.Scan("{1,abc,3}"); err == nil {
		t.Error("expected error for invalid number, got nil")
	}
}

func TestInt64Array_Scan_InvalidFormat(t *testing.T) {
	var a Int64Array
	if err := a.Scan("not an array"); err == nil {
		t.Error("expected error for invalid format, got nil")
	}
}

func TestInt64Array_RoundTrip(t *testing.T) {
	original := Int64Array{100, 200, 300, -400}
	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned Int64Array
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(scanned) != len(original) {
		t.Fatalf("round-trip len = %d, want %d", len(scanned), len(original))
	}
	for i := range original {
		if scanned[i] != original[i] {
			t.Errorf("round-trip element %d: got %d, want %d", i, scanned[i], original[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Hstore tests
// ---------------------------------------------------------------------------

func strPtr(s string) *string {
	return &s
}

func TestHstore_Value_Nil(t *testing.T) {
	var h Hstore
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for nil Hstore, got %v", v)
	}
}

func TestHstore_Value_Empty(t *testing.T) {
	h := Hstore{}
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Hstore.Value() returned %T, want string", v)
	}
	if got != "" {
		t.Errorf("Hstore.Value() on empty map = %q, want %q", got, "")
	}
}

func TestHstore_Value_WithValues(t *testing.T) {
	h := Hstore{
		"key1": strPtr("value1"),
	}
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Hstore.Value() returned %T, want string", v)
	}
	// Single key: should produce "key1"=>"value1"
	expected := `"key1"=>"value1"`
	if got != expected {
		t.Errorf("Hstore.Value() = %q, want %q", got, expected)
	}
}

func TestHstore_Value_WithNullValues(t *testing.T) {
	h := Hstore{
		"key1": nil,
	}
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Hstore.Value() returned %T, want string", v)
	}
	expected := `"key1"=>NULL`
	if got != expected {
		t.Errorf("Hstore.Value() = %q, want %q", got, expected)
	}
}

func TestHstore_Value_MixedNullAndValues(t *testing.T) {
	// Since map iteration order is non-deterministic, we test that the output
	// contains both pairs and parses back correctly.
	h := Hstore{
		"a": strPtr("hello"),
		"b": nil,
	}
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Hstore.Value() returned %T, want string", v)
	}
	// Verify both pairs are present.
	if !containsAll(got, `"a"=>"hello"`, `"b"=>NULL`) {
		t.Errorf("Hstore.Value() = %q, expected to contain both key-value pairs", got)
	}
}

// containsAll checks whether s contains all specified substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestHstore_Value_EscapedCharacters(t *testing.T) {
	h := Hstore{
		`k"ey`: strPtr(`v"al`),
	}
	v, err := h.Value()
	if err != nil {
		t.Fatalf("Hstore.Value() error = %v", err)
	}
	got, ok := v.(string)
	if !ok {
		t.Fatalf("Hstore.Value() returned %T, want string", v)
	}
	// Escaped quotes.
	expected := `"k\"ey"=>"v\"al"`
	if got != expected {
		t.Errorf("Hstore.Value() = %q, want %q", got, expected)
	}
}

func TestHstore_Scan_Nil(t *testing.T) {
	h := Hstore{"existing": strPtr("data")}
	if err := h.Scan(nil); err != nil {
		t.Fatalf("Hstore.Scan(nil) error = %v", err)
	}
	if h != nil {
		t.Errorf("expected nil after scanning nil, got %v", h)
	}
}

func TestHstore_Scan_String(t *testing.T) {
	var h Hstore
	input := `"key1"=>"value1", "key2"=>"value2"`
	if err := h.Scan(input); err != nil {
		t.Fatalf("Hstore.Scan(string) error = %v", err)
	}
	if len(h) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(h))
	}
	if h["key1"] == nil || *h["key1"] != "value1" {
		t.Errorf("key1: got %v, want 'value1'", h["key1"])
	}
	if h["key2"] == nil || *h["key2"] != "value2" {
		t.Errorf("key2: got %v, want 'value2'", h["key2"])
	}
}

func TestHstore_Scan_Bytes(t *testing.T) {
	var h Hstore
	input := []byte(`"name"=>"alice"`)
	if err := h.Scan(input); err != nil {
		t.Fatalf("Hstore.Scan([]byte) error = %v", err)
	}
	if len(h) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(h))
	}
	if h["name"] == nil || *h["name"] != "alice" {
		t.Errorf("name: got %v, want 'alice'", h["name"])
	}
}

func TestHstore_Scan_NullValues(t *testing.T) {
	var h Hstore
	input := `"a"=>"hello", "b"=>NULL`
	if err := h.Scan(input); err != nil {
		t.Fatalf("Hstore.Scan() error = %v", err)
	}
	if len(h) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(h))
	}
	if h["a"] == nil || *h["a"] != "hello" {
		t.Errorf("a: got %v, want 'hello'", h["a"])
	}
	if h["b"] != nil {
		t.Errorf("b: expected nil (NULL), got %v", *h["b"])
	}
}

func TestHstore_Scan_EmptyString(t *testing.T) {
	var h Hstore
	if err := h.Scan(""); err != nil {
		t.Fatalf("Hstore.Scan('') error = %v", err)
	}
	if len(h) != 0 {
		t.Errorf("expected empty Hstore, got %v", h)
	}
}

func TestHstore_Scan_UnsupportedType(t *testing.T) {
	var h Hstore
	if err := h.Scan(42); err == nil {
		t.Error("expected error for unsupported type, got nil")
	}
}

func TestHstore_Scan_InvalidFormat(t *testing.T) {
	var h Hstore
	if err := h.Scan("not a valid hstore"); err == nil {
		t.Error("expected error for invalid hstore format, got nil")
	}
}

func TestHstore_RoundTrip(t *testing.T) {
	original := Hstore{
		"name":  strPtr("alice"),
		"role":  strPtr("admin"),
		"extra": nil,
	}

	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned Hstore
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(scanned) != len(original) {
		t.Fatalf("round-trip len = %d, want %d", len(scanned), len(original))
	}
	for k, origVal := range original {
		scanVal, ok := scanned[k]
		if !ok {
			t.Errorf("round-trip: missing key %q", k)
			continue
		}
		if origVal == nil {
			if scanVal != nil {
				t.Errorf("round-trip key %q: got %v, want nil", k, *scanVal)
			}
		} else {
			if scanVal == nil {
				t.Errorf("round-trip key %q: got nil, want %q", k, *origVal)
			} else if *scanVal != *origVal {
				t.Errorf("round-trip key %q: got %q, want %q", k, *scanVal, *origVal)
			}
		}
	}
}

func TestHstore_RoundTrip_WithEscapes(t *testing.T) {
	original := Hstore{
		`key"with"quotes`:    strPtr(`val"with"quotes`),
		`key\with\backslash`: strPtr(`val\with\backslash`),
	}

	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value() error = %v", err)
	}

	var scanned Hstore
	if err := scanned.Scan(v); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(scanned) != len(original) {
		t.Fatalf("round-trip len = %d, want %d", len(scanned), len(original))
	}
	for k, origVal := range original {
		scanVal, ok := scanned[k]
		if !ok {
			t.Errorf("round-trip: missing key %q", k)
			continue
		}
		if origVal == nil {
			if scanVal != nil {
				t.Errorf("round-trip key %q: got %v, want nil", k, *scanVal)
			}
		} else if scanVal == nil {
			t.Errorf("round-trip key %q: got nil, want %q", k, *origVal)
		} else if *scanVal != *origVal {
			t.Errorf("round-trip key %q: got %q, want %q", k, *scanVal, *origVal)
		}
	}
}
