// Package tagparser provides a high-performance struct tag parser for grove:"..."
// and bun:"..." tags. It extracts column names, boolean options, and key:value pairs
// from comma-separated tag strings.
package tagparser

import "strings"

// Tag represents a parsed struct tag.
type Tag struct {
	Name    string            // Column name or empty
	Options map[string]string // Key-value options (e.g., "pk" -> "", "type" -> "jsonb", "privacy" -> "pii")
}

// Parse parses a raw struct tag value like "column_name,pk,notnull,type:jsonb,default:'active'"
// into a Tag. It handles:
//   - First non-option token is the column name
//   - Boolean options like "pk", "notnull", "autoincrement" -> key="pk" value=""
//   - Key:value options like "type:jsonb", "privacy:pii" -> key="type" value="jsonb"
//   - Quoted values like "default:'hello world'" -> key="default" value="hello world"
//   - Table-level options like "table:users,alias:u"
func Parse(tag string) Tag {
	t := Tag{
		Options: make(map[string]string),
	}

	tag = strings.TrimSpace(tag)
	if tag == "" {
		return t
	}

	tokens := splitTokens(tag)
	if len(tokens) == 0 {
		return t
	}

	// Process each token. The first token that is not a key:value pair is the
	// column name. However, if the first token is itself a key:value pair
	// (e.g., "table:users"), there is no column name.
	nameAssigned := false
	for i, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}

		key, value, hasColon := parseToken(tok)

		if hasColon {
			// Key:value option.
			t.Options[key] = value
		} else if i == 0 && !nameAssigned {
			// First plain token is the column name.
			t.Name = key
			nameAssigned = true
		} else {
			// Subsequent plain tokens are boolean options.
			t.Options[key] = ""
		}
	}

	return t
}

// splitTokens splits the tag string on commas, but respects single-quoted values
// so that "default:'hello, world'" is not split inside the quotes.
func splitTokens(tag string) []string {
	var tokens []string
	var buf strings.Builder
	inQuote := false

	for i := 0; i < len(tag); i++ {
		ch := tag[i]
		switch {
		case ch == '\'' && !inQuote:
			inQuote = true
			_ = buf.WriteByte(ch)
		case ch == '\'' && inQuote:
			inQuote = false
			_ = buf.WriteByte(ch)
		case ch == ',' && !inQuote:
			tokens = append(tokens, buf.String())
			buf.Reset()
		default:
			_ = buf.WriteByte(ch)
		}
	}

	// Flush the remaining buffer.
	if buf.Len() > 0 {
		tokens = append(tokens, buf.String())
	}

	return tokens
}

// parseToken parses a single token. If it contains a colon, it returns the key
// and value separated by the first colon. Quoted values have their surrounding
// single quotes stripped. If there is no colon, key is the entire token, value
// is empty, and hasColon is false.
func parseToken(tok string) (key, value string, hasColon bool) {
	idx := strings.IndexByte(tok, ':')
	if idx < 0 {
		return tok, "", false
	}

	key = tok[:idx]
	value = tok[idx+1:]

	// Strip surrounding single quotes from the value.
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		value = value[1 : len(value)-1]
	}

	return key, value, true
}

// HasOption checks if a tag has a specific option.
func (t Tag) HasOption(key string) bool {
	_, ok := t.Options[key]
	return ok
}

// GetOption returns the value of a named option, or empty string.
func (t Tag) GetOption(key string) string {
	return t.Options[key]
}
