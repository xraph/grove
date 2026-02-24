package keyspace

import "strings"

// ComposeKey builds a composite key from segments joined by the given separator.
func ComposeKey(separator string, segments ...string) string {
	return strings.Join(segments, separator)
}

// ParseKey splits a composite key into segments by the given separator.
func ParseKey(key, separator string) []string {
	return strings.Split(key, separator)
}

// Join builds a composite key from segments using ":" as the separator.
func Join(segments ...string) string {
	return ComposeKey(":", segments...)
}
