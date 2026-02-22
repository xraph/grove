package driver

import (
	"reflect"
	"time"

	"github.com/xraph/grove/schema"
)

// Dialect encapsulates database-specific syntax rules.
// Each concrete driver provides its own Dialect implementation so that the
// query builder can emit correct SQL (or equivalent) for the target database.
type Dialect interface {
	// Name returns the dialect name (e.g., "pg", "mysql", "sqlite").
	Name() string

	// Quote quotes an identifier (table or column name) using the dialect's
	// quoting convention. For example, PostgreSQL uses double quotes while
	// MySQL uses backticks.
	Quote(ident string) string

	// Placeholder returns the nth parameter placeholder for prepared
	// statements. n is 1-indexed.
	// Examples:
	//   PostgreSQL -> "$1", "$2", ...
	//   MySQL      -> "?", "?", ...
	Placeholder(n int) string

	// GoToDBType maps a Go reflect.Type to the appropriate database-native
	// column type string, taking field options (e.g., explicit SQLType) into
	// account.
	GoToDBType(goType reflect.Type, opts schema.FieldOptions) string

	// AppendBytes appends a byte-escaped representation of v to the byte
	// slice b and returns the extended slice. The encoding is
	// dialect-specific (e.g., hex encoding for PostgreSQL bytea).
	AppendBytes(b []byte, v []byte) []byte

	// AppendTime appends a time value formatted for the dialect to b and
	// returns the extended slice.
	AppendTime(b []byte, t time.Time) []byte
}
