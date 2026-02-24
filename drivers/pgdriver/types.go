package pgdriver

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// JSONMap is a map[string]any that implements database/sql/driver.Valuer and
// sql.Scanner for transparent serialization to/from PostgreSQL jsonb columns.
type JSONMap map[string]any

// Value marshals the map to JSON for storage.
func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: JSONMap.Value: %w", err)
	}
	return b, nil
}

// Scan unmarshals JSON data from the database into the map. Accepts []byte or
// string source values.
func (m *JSONMap) Scan(src any) error {
	if src == nil {
		*m = nil
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("pgdriver: JSONMap.Scan: unsupported type %T", src)
	}

	result := make(JSONMap)
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("pgdriver: JSONMap.Scan: %w", err)
	}
	*m = result
	return nil
}

// StringArray is a []string that implements database/sql/driver.Valuer and
// sql.Scanner for transparent serialization to/from PostgreSQL text[] columns.
type StringArray []string

// Value serializes the string slice to the PostgreSQL array literal format:
// {"elem1","elem2","elem3"}.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	var b strings.Builder
	b.WriteByte('{')
	for i, s := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		// Escape backslashes and double quotes within array elements.
		for _, c := range s {
			switch c {
			case '\\':
				b.WriteString(`\\`)
			case '"':
				b.WriteString(`\"`)
			default:
				b.WriteRune(c)
			}
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String(), nil
}

// Scan parses a PostgreSQL text[] array literal into a string slice.
// The expected format is: {elem1,elem2,elem3} or {"elem1","elem2","elem3"}.
func (a *StringArray) Scan(src any) error {
	if src == nil {
		*a = nil
		return nil
	}

	var raw string
	switch v := src.(type) {
	case []byte:
		raw = string(v)
	case string:
		raw = v
	default:
		return fmt.Errorf("pgdriver: StringArray.Scan: unsupported type %T", src)
	}

	parsed, err := parsePostgresArray(raw)
	if err != nil {
		return fmt.Errorf("pgdriver: StringArray.Scan: %w", err)
	}
	*a = parsed
	return nil
}

// Int64Array is a []int64 that implements database/sql/driver.Valuer and
// sql.Scanner for transparent serialization to/from PostgreSQL bigint[] columns.
type Int64Array []int64

// Value serializes the int64 slice to the PostgreSQL array literal format:
// {1,2,3}.
func (a Int64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	var b strings.Builder
	b.WriteByte('{')
	for i, v := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(v, 10))
	}
	b.WriteByte('}')
	return b.String(), nil
}

// Scan parses a PostgreSQL bigint[] array literal into an int64 slice.
// The expected format is: {1,2,3}.
func (a *Int64Array) Scan(src any) error {
	if src == nil {
		*a = nil
		return nil
	}

	var raw string
	switch v := src.(type) {
	case []byte:
		raw = string(v)
	case string:
		raw = v
	default:
		return fmt.Errorf("pgdriver: Int64Array.Scan: unsupported type %T", src)
	}

	parsed, err := parsePostgresArray(raw)
	if err != nil {
		return fmt.Errorf("pgdriver: Int64Array.Scan: %w", err)
	}

	result := make(Int64Array, len(parsed))
	for i, s := range parsed {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("pgdriver: Int64Array.Scan: element %d: %w", i, err)
		}
		result[i] = n
	}
	*a = result
	return nil
}

// Hstore represents a PostgreSQL hstore column (key-value string map).
// NULL values are represented as nil *string entries.
type Hstore map[string]*string

// Value serializes the Hstore to the PostgreSQL hstore literal format:
// "key1"=>"value1","key2"=>"value2". Keys with nil values are encoded as
// "key"=>NULL.
func (h Hstore) Value() (driver.Value, error) {
	if h == nil {
		return nil, nil
	}

	var b strings.Builder
	first := true
	for k, v := range h {
		if !first {
			b.WriteByte(',')
		}
		first = false

		b.WriteByte('"')
		b.WriteString(hstoreEscape(k))
		b.WriteByte('"')
		b.WriteString("=>")
		if v == nil {
			b.WriteString("NULL")
		} else {
			b.WriteByte('"')
			b.WriteString(hstoreEscape(*v))
			b.WriteByte('"')
		}
	}
	return b.String(), nil
}

// Scan parses a PostgreSQL hstore string representation into the map.
// Accepts []byte or string source values. The expected format is:
// "key1"=>"value1", "key2"=>NULL
func (h *Hstore) Scan(src any) error {
	if src == nil {
		*h = nil
		return nil
	}

	var raw string
	switch v := src.(type) {
	case []byte:
		raw = string(v)
	case string:
		raw = v
	default:
		return fmt.Errorf("pgdriver: Hstore.Scan: unsupported type %T", src)
	}

	result, err := parseHstore(raw)
	if err != nil {
		return fmt.Errorf("pgdriver: Hstore.Scan: %w", err)
	}
	*h = result
	return nil
}

// hstoreEscape escapes backslashes and double quotes for hstore string encoding.
func hstoreEscape(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// parseHstore parses a PostgreSQL hstore literal into a map.
// Example input: "key1"=>"value1", "key2"=>NULL
func parseHstore(s string) (Hstore, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Hstore{}, nil
	}

	result := make(Hstore)
	i := 0

	for i < len(s) {
		// Skip whitespace and commas.
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			break
		}

		// Parse key (must be quoted).
		if s[i] != '"' {
			return nil, fmt.Errorf("expected '\"' at position %d in %q", i, s)
		}
		key, newPos, err := hstoreReadQuoted(s, i)
		if err != nil {
			return nil, err
		}
		i = newPos

		// Skip whitespace.
		for i < len(s) && s[i] == ' ' {
			i++
		}

		// Expect "=>".
		if i+2 > len(s) || s[i:i+2] != "=>" {
			return nil, fmt.Errorf("expected '=>' at position %d in %q", i, s)
		}
		i += 2

		// Skip whitespace.
		for i < len(s) && s[i] == ' ' {
			i++
		}

		// Parse value: either NULL or a quoted string.
		if i+4 <= len(s) && strings.EqualFold(s[i:i+4], "NULL") {
			// Check that NULL is followed by end, comma, or whitespace.
			endPos := i + 4
			if endPos < len(s) && s[endPos] != ',' && s[endPos] != ' ' {
				return nil, fmt.Errorf("unexpected character after NULL at position %d in %q", endPos, s)
			}
			result[key] = nil
			i = endPos
		} else if i < len(s) && s[i] == '"' {
			val, newPos, err := hstoreReadQuoted(s, i)
			if err != nil {
				return nil, err
			}
			result[key] = &val
			i = newPos
		} else {
			return nil, fmt.Errorf("expected '\"' or NULL at position %d in %q", i, s)
		}
	}

	return result, nil
}

// hstoreReadQuoted reads a double-quoted hstore string starting at position pos,
// handling backslash escapes. It returns the unescaped string and the position
// after the closing quote.
func hstoreReadQuoted(s string, pos int) (string, int, error) {
	if pos >= len(s) || s[pos] != '"' {
		return "", pos, fmt.Errorf("expected '\"' at position %d", pos)
	}
	pos++ // skip opening quote

	var b strings.Builder
	for pos < len(s) {
		if s[pos] == '\\' && pos+1 < len(s) {
			pos++
			b.WriteByte(s[pos])
			pos++
		} else if s[pos] == '"' {
			pos++ // skip closing quote
			return b.String(), pos, nil
		} else {
			b.WriteByte(s[pos])
			pos++
		}
	}
	return "", pos, fmt.Errorf("unterminated quoted string starting at position %d", pos)
}

// parsePostgresArray parses a PostgreSQL array literal string like
// {a,b,c} or {"a","b","c"} into a slice of strings.
func parsePostgresArray(s string) ([]string, error) {
	// Trim outer braces.
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, fmt.Errorf("invalid array format: %q", s)
	}
	inner := s[1 : len(s)-1]

	// Empty array.
	if len(strings.TrimSpace(inner)) == 0 {
		return []string{}, nil
	}

	var result []string
	i := 0
	for i < len(inner) {
		// Skip whitespace.
		for i < len(inner) && inner[i] == ' ' {
			i++
		}
		if i >= len(inner) {
			break
		}

		if inner[i] == '"' {
			// Quoted element: scan until closing quote, handling escapes.
			i++ // skip opening quote
			var elem strings.Builder
			for i < len(inner) {
				if inner[i] == '\\' && i+1 < len(inner) {
					// Escaped character.
					i++
					elem.WriteByte(inner[i])
					i++
				} else if inner[i] == '"' {
					i++ // skip closing quote
					break
				} else {
					elem.WriteByte(inner[i])
					i++
				}
			}
			result = append(result, elem.String())
		} else {
			// Unquoted element: scan until comma or end.
			start := i
			for i < len(inner) && inner[i] != ',' {
				i++
			}
			elem := strings.TrimSpace(inner[start:i])
			result = append(result, elem)
		}

		// Skip comma separator.
		if i < len(inner) && inner[i] == ',' {
			i++
		}
	}

	return result, nil
}
