// Package safe provides SQL identifier quoting and sanitization utilities to
// prevent SQL injection in dynamically constructed queries. It implements
// PostgreSQL-style double-quote quoting by default; dialect-specific quoting
// is handled at the Dialect layer.
package safe

import "strings"

// QuoteIdent quotes an identifier (table or column name) safely.
// It wraps the identifier in double quotes and escapes any embedded double
// quotes by doubling them. This follows the PostgreSQL quoting convention.
//
// Examples:
//
//	QuoteIdent("users")        => `"users"`
//	QuoteIdent(`my"table`)     => `"my""table"`
//	QuoteIdent("")             => `""`
func QuoteIdent(ident string) string {
	// Escape embedded double quotes by replacing " with "".
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	var b strings.Builder
	b.Grow(len(escaped) + 2)
	b.WriteByte('"')
	b.WriteString(escaped)
	b.WriteByte('"')
	return b.String()
}

// IsValidIdent checks if a string is a valid unquoted SQL identifier.
// Valid identifiers contain only ASCII letters, digits, and underscores,
// and must start with a letter or underscore. An empty string is not valid.
func IsValidIdent(ident string) bool {
	if ident == "" {
		return false
	}
	for i := 0; i < len(ident); i++ {
		c := ident[i]
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		isUnderscore := c == '_'

		if i == 0 {
			// First character must be a letter or underscore.
			if !isLetter && !isUnderscore {
				return false
			}
		} else {
			if !isLetter && !isDigit && !isUnderscore {
				return false
			}
		}
	}
	return true
}

// Sanitize removes any characters that are not safe for SQL identifiers.
// It preserves only ASCII letters, digits, and underscores. If the result
// would start with a digit, a leading underscore is prepended.
// Returns an empty string if no safe characters remain.
func Sanitize(ident string) string {
	var b strings.Builder
	b.Grow(len(ident))
	for i := 0; i < len(ident); i++ {
		c := ident[i]
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		isUnderscore := c == '_'

		if isLetter || isDigit || isUnderscore {
			b.WriteByte(c)
		}
	}
	result := b.String()

	// If the result starts with a digit, prepend an underscore.
	if result != "" && result[0] >= '0' && result[0] <= '9' {
		result = "_" + result
	}

	return result
}
