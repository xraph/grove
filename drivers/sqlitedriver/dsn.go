package sqlitedriver

import (
	"net/url"
	"strings"
)

// foreignKeysPragma is the DSN parameter modernc.org/sqlite runs as
// "PRAGMA foreign_keys(1)" on each connection it opens.
const foreignKeysPragma = "_pragma=foreign_keys%281%29"

// withForeignKeys returns dsn with a _pragma parameter that turns foreign keys
// on for every connection opened from it. Running the pragma once after
// sql.Open only reaches the one pooled connection it ran on.
//
// The caller's parameters stay as written. If the DSN already sets
// foreign_keys through a _pragma parameter, on or off, it is returned
// unchanged. An empty DSN becomes "file:" so modernc.org/sqlite, which only
// reads parameters after a non-empty path, still sees the pragma; both name a
// private temporary database.
func withForeignKeys(dsn string) string {
	if dsn == "" {
		dsn = "file:"
	}
	base, query, hasQuery := strings.Cut(dsn, "?")
	if !hasQuery {
		return dsn + "?" + foreignKeysPragma
	}
	if setsForeignKeys(query) {
		return dsn
	}
	if query == "" || strings.HasSuffix(query, "&") {
		return base + "?" + query + foreignKeysPragma
	}
	return base + "?" + query + "&" + foreignKeysPragma
}

// setsForeignKeys reports whether a DSN query already has a _pragma parameter
// for foreign_keys, in either the foreign_keys(N) or the foreign_keys=N form.
func setsForeignKeys(query string) bool {
	q, err := url.ParseQuery(query)
	if err != nil {
		// modernc.org/sqlite rejects a query it can't parse when it opens the
		// first connection, which reports the real problem. Appending to it
		// changes nothing.
		return false
	}
	for _, p := range q["_pragma"] {
		name := strings.ToLower(strings.TrimSpace(p))
		if strings.HasPrefix(name, "foreign_keys") {
			return true
		}
	}
	return false
}
