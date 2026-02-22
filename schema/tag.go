package schema

import (
	"reflect"
	"strings"
	"unicode"

	"github.com/xraph/grove/internal/tagparser"
)

// TagSource indicates which tag was used for a field.
type TagSource int

const (
	// TagSourceGrove indicates grove:"..." was present and used.
	TagSourceGrove TagSource = iota
	// TagSourceBun indicates bun:"..." fallback was used.
	TagSourceBun
	// TagSourceNone indicates no tag was found; field name is used as column (snake_case).
	TagSourceNone
)

// ResolveTag determines which tag to use for a struct field.
// Resolution order: grove > bun > snake_case of field name.
func ResolveTag(field reflect.StructField) (tag string, source TagSource) {
	if gTag, ok := field.Tag.Lookup("grove"); ok {
		return gTag, TagSourceGrove
	}
	if bTag, ok := field.Tag.Lookup("bun"); ok {
		return bTag, TagSourceBun
	}
	return "", TagSourceNone
}

// ParseTag parses a raw tag string into structured Tag with all options.
func ParseTag(raw string) tagparser.Tag {
	return tagparser.Parse(raw)
}

// ToSnakeCase converts a CamelCase string to snake_case.
// It handles acronyms like "ID", "HTML", "URL" correctly:
//
//	"UserID"     -> "user_id"
//	"HTMLParser" -> "html_parser"
//	"APIKeyURL"  -> "api_key_url"
//	"SimpleTest" -> "simple_test"
//	"ID"         -> "id"
func ToSnakeCase(s string) string {
	if s == "" {
		return ""
	}

	var buf strings.Builder
	buf.Grow(len(s) + 4) // pre-allocate with some room for underscores

	runes := []rune(s)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]

		if !unicode.IsUpper(r) {
			buf.WriteRune(unicode.ToLower(r))
			continue
		}

		// Current rune is uppercase.
		// Determine if we need an underscore before it.
		if i > 0 {
			prev := runes[i-1]
			if unicode.IsLower(prev) || unicode.IsDigit(prev) {
				// Transition from lower/digit to upper: add underscore.
				buf.WriteByte('_')
			} else if unicode.IsUpper(prev) && i+1 < n && unicode.IsLower(runes[i+1]) {
				// We are inside an acronym run and the next char is lowercase,
				// meaning this char starts a new word. E.g., "HTMLParser" at 'P'.
				buf.WriteByte('_')
			}
		}

		buf.WriteRune(unicode.ToLower(r))
	}

	return buf.String()
}
