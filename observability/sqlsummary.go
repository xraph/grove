package observability

import (
	"strings"
	"sync"
	"sync/atomic"
)

// SQLSummary is what SummarizeSQL reads out of a statement: the operation, the
// first table it works on, and the two joined as the OTel db.query.summary.
// None of the three ever holds a value from the statement.
type SQLSummary struct {
	// Operation is the db.operation.name: an upper-case keyword such as
	// SELECT, INSERT, COMMIT or CREATE TABLE. Empty when the statement starts
	// with something we don't recognise.
	Operation string

	// Collection is the db.collection.name: the first table the statement
	// reads or writes, as written (schema prefix kept, quotes dropped). Empty
	// when there isn't one, or when it's a subquery.
	Collection string

	// Summary is Operation and Collection joined by a space, or Operation
	// alone. Drivers use it as the span name.
	Summary string
}

// maxIdentifierLen bounds a table name we'll report. Anything longer is
// almost certainly not a table name, and we'd rather drop it than put it on a
// metric label.
const maxIdentifierLen = 128

// The summary cache. Queries built by grove's builders repeat exactly, so most
// lookups hit. It stops growing at maxCachedSummaries entries, and statements
// longer than maxCachedQueryLen (multi-row inserts, mostly) are never stored.
const (
	maxCachedSummaries = 4096
	maxCachedQueryLen  = 2048
)

var summaryCache struct {
	m sync.Map // string -> sqlShape
	n atomic.Int32
}

// sqlShape is a SQLSummary plus what the metric label needs: the table
// without its schema or database qualifier.
type sqlShape struct {
	SQLSummary

	// table is the last part of Collection, so "tenant_42"."orders" gives
	// orders. It comes from the scanner rather than a split on '.', which
	// keeps a quoted name holding a dot whole.
	table string
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokWord
	tokQuoted // a quoted identifier, quotes removed
	tokString // a string literal; its text is never kept
	tokPunct
)

type token struct {
	kind tokKind
	text string
}

// sqlScanner hands out SQL tokens, skipping whitespace, comments, string
// literals and Postgres dollar-quoted bodies.
type sqlScanner struct {
	s string
	i int
}

// SummarizeSQL reads the operation and the first table out of a SQL
// statement. It reads the statement's shape, never its values, so the result
// is safe to put on a span or a metric label: SELECT, INSERT INTO, UPDATE,
// DELETE FROM, MERGE INTO, the CTE behind a WITH, and DDL on tables.
//
// It is a scanner, not a parser. Comments and quoted strings are skipped, and
// a table inside parentheses (a subquery) is never reported.
func SummarizeSQL(query string) SQLSummary {
	return summarizeCached(query).SQLSummary
}

// summarizeCached is SummarizeSQL with the unqualified table kept, through
// the summary cache.
func summarizeCached(query string) sqlShape {
	if len(query) <= maxCachedQueryLen {
		if v, ok := summaryCache.m.Load(query); ok {
			s, _ := v.(sqlShape) //nolint:errcheck // only sqlShape values are stored
			return s
		}
	}

	s := summarize(query)

	if len(query) <= maxCachedQueryLen && claim(&summaryCache.n, maxCachedSummaries) {
		summaryCache.m.Store(query, s)
	}

	return s
}

func summarize(query string) sqlShape {
	sc := &sqlScanner{s: query}

	tok := sc.next()
	for tok.kind == tokPunct && tok.text == "(" {
		tok = sc.next()
	}
	if tok.kind != tokWord {
		return sqlShape{}
	}

	op := strings.ToUpper(tok.text)
	if op == "WITH" {
		op = sc.mainAfterWith()
		if op == "" {
			return sqlShape{}
		}
	}

	var operation, collection, table string

	switch op {
	case "SELECT":
		operation = op
		if sc.seekWord("FROM") {
			sc.skipWords("ONLY")
			collection, table = sc.tableName()
		}
	case "INSERT", "REPLACE", "UPSERT":
		operation = op
		if sc.seekWordWithin("INTO", 4) {
			sc.skipWords("TABLE")
			collection, table = sc.tableName()
		}
	case "UPDATE":
		operation = op
		sc.skipUpdateModifiers()
		collection, table = sc.tableName()
	case "DELETE":
		operation = op
		if sc.seekWord("FROM") {
			sc.skipWords("ONLY")
			collection, table = sc.tableName()
		}
	case "MERGE":
		operation = op
		if sc.seekWordWithin("INTO", 1) {
			collection, table = sc.tableName()
		}
	case "CREATE", "DROP", "ALTER":
		operation, collection, table = sc.ddl(op)
	case "TRUNCATE":
		operation = op
		sc.skipWords("TABLE")
		collection, table = sc.tableName()
	default:
		if _, ok := plainOperations[op]; !ok {
			return sqlShape{}
		}
		operation = op
	}

	s := sqlShape{
		SQLSummary: SQLSummary{Operation: operation, Collection: collection, Summary: operation},
		table:      table,
	}
	if collection != "" {
		s.Summary = operation + " " + collection
	}
	return s
}

// plainOperations are statements we report by their first keyword alone.
// Anything not here, and not handled above, gets no operation at all, so a
// garbled statement can't mint new metric label values.
var plainOperations = map[string]struct{}{
	"ANALYZE": {}, "ATTACH": {}, "BEGIN": {}, "CALL": {}, "CHECKPOINT": {},
	"CLOSE": {}, "CLUSTER": {}, "COMMENT": {}, "COMMIT": {}, "COPY": {},
	"DEALLOCATE": {}, "DECLARE": {}, "DESCRIBE": {}, "DETACH": {}, "DISCARD": {},
	"DO": {}, "END": {}, "EXECUTE": {}, "EXPLAIN": {}, "FETCH": {}, "GRANT": {},
	"KILL": {}, "LISTEN": {}, "LOCK": {}, "MOVE": {}, "NOTIFY": {},
	"OPTIMIZE": {}, "PRAGMA": {}, "PREPARE": {}, "REFRESH": {}, "REINDEX": {},
	"RELEASE": {}, "RESET": {}, "REVOKE": {}, "ROLLBACK": {}, "SAVEPOINT": {},
	"SET": {}, "SHOW": {}, "START": {}, "SYSTEM": {}, "UNLISTEN": {}, "USE": {},
	"VACUUM": {}, "VALUES": {},
}

// ddlObjects are the object kinds we name in a DDL operation ("CREATE TABLE").
// Only TABLE and INDEX statements report a collection.
var ddlObjects = map[string]struct{}{
	"DATABASE": {}, "EXTENSION": {}, "FUNCTION": {}, "INDEX": {}, "PROCEDURE": {},
	"ROLE": {}, "SCHEMA": {}, "SEQUENCE": {}, "TABLE": {}, "TRIGGER": {},
	"TYPE": {}, "USER": {}, "VIEW": {},
}

func (sc *sqlScanner) ddl(verb string) (operation, collection, table string) {
	// CREATE [OR REPLACE] [UNIQUE] [TEMP | TEMPORARY | UNLOGGED | ...] <object>
	sc.skipWords("OR", "REPLACE", "UNIQUE", "TEMP", "TEMPORARY", "UNLOGGED",
		"GLOBAL", "LOCAL", "MATERIALIZED", "VIRTUAL", "RECURSIVE", "IF", "NOT", "EXISTS")

	tok := sc.next()
	if tok.kind != tokWord {
		return verb, "", ""
	}
	object := strings.ToUpper(tok.text)
	if _, ok := ddlObjects[object]; !ok {
		return verb, "", ""
	}
	operation = verb + " " + object

	switch object {
	case "TABLE":
		sc.skipWords("IF", "NOT", "EXISTS", "ONLY")
		collection, table = sc.tableName()
		return operation, collection, table
	case "INDEX":
		// CREATE INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON [ONLY] table
		if verb == "CREATE" && sc.seekWordWithin("ON", 6) {
			sc.skipWords("ONLY")
			collection, table = sc.tableName()
			return operation, collection, table
		}
	}

	return operation, "", ""
}

// mainAfterWith skips the CTEs of a WITH statement and returns the keyword
// of the statement that follows them. CTE bodies sit inside parentheses, so
// the first data keyword at depth zero is the main statement.
func (sc *sqlScanner) mainAfterWith() string {
	depth := 0
	for {
		tok := sc.next()
		switch tok.kind {
		case tokEOF:
			return ""
		case tokPunct:
			switch tok.text {
			case "(":
				depth++
			case ")":
				depth--
			}
		case tokWord:
			if depth != 0 {
				continue
			}
			switch kw := strings.ToUpper(tok.text); kw {
			case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE":
				return kw
			}
		}
	}
}

// seekWord moves past the first occurrence of kw at parenthesis depth zero
// (relative to where the scan started) and reports whether it found one.
func (sc *sqlScanner) seekWord(kw string) bool {
	return sc.seekWordWithin(kw, -1)
}

// seekWordWithin is seekWord that gives up after limit tokens at depth zero.
// A negative limit means no limit.
func (sc *sqlScanner) seekWordWithin(kw string, limit int) bool {
	depth := 0
	seen := 0
	for {
		tok := sc.next()
		switch tok.kind {
		case tokEOF:
			return false
		case tokPunct:
			switch tok.text {
			case "(":
				depth++
			case ")":
				depth--
				if depth < 0 {
					return false
				}
			case ";":
				if depth == 0 {
					return false
				}
			}
		case tokWord:
			if depth == 0 && strings.EqualFold(tok.text, kw) {
				return true
			}
		}
		if depth == 0 {
			seen++
			if limit >= 0 && seen > limit {
				return false
			}
		}
	}
}

// skipWords consumes any run of the given keywords, in any order.
func (sc *sqlScanner) skipWords(words ...string) {
	for {
		save := sc.i
		tok := sc.next()
		if tok.kind != tokWord || !equalFoldAny(tok.text, words) {
			sc.i = save
			return
		}
	}
}

// skipUpdateModifiers consumes what may sit between UPDATE and its table:
// SQLite's OR <conflict>, MySQL's LOW_PRIORITY and IGNORE, Postgres's ONLY.
func (sc *sqlScanner) skipUpdateModifiers() {
	for {
		save := sc.i
		tok := sc.next()
		if tok.kind != tokWord {
			sc.i = save
			return
		}
		switch strings.ToUpper(tok.text) {
		case "OR":
			sc.next() // the conflict clause: ROLLBACK, ABORT, REPLACE, FAIL, IGNORE
		case "LOW_PRIORITY", "IGNORE", "ONLY":
		default:
			sc.i = save
			return
		}
	}
}

// tableName reads a possibly qualified, possibly quoted table name. It returns
// the name as written, parts joined by dots and quotes dropped, and its last
// part on its own: dispatch.jobs and jobs. Both are "" for a subquery, a
// missing name or one too long to be real.
func (sc *sqlScanner) tableName() (name, table string) {
	var b strings.Builder
	for {
		tok := sc.next()
		if tok.kind != tokWord && tok.kind != tokQuoted {
			return "", ""
		}
		if b.Len()+len(tok.text) > maxIdentifierLen {
			return "", ""
		}
		last := b.Len()
		b.WriteString(tok.text)

		save := sc.i
		dot := sc.next()
		if dot.kind != tokPunct || dot.text != "." {
			sc.i = save
			name = b.String()
			return name, name[last:]
		}
		b.WriteByte('.')
	}
}

func equalFoldAny(s string, words []string) bool {
	for _, w := range words {
		if strings.EqualFold(s, w) {
			return true
		}
	}
	return false
}

func (sc *sqlScanner) next() token {
	sc.skipSpaceAndComments()
	if sc.i >= len(sc.s) {
		return token{kind: tokEOF}
	}

	c := sc.s[sc.i]
	switch {
	case isWordStart(c):
		start := sc.i
		for sc.i < len(sc.s) && isWordPart(sc.s[sc.i]) {
			sc.i++
		}
		return token{kind: tokWord, text: sc.s[start:sc.i]}

	case c == '"' || c == '`':
		return sc.quoted(c, c)

	case c == '[':
		return sc.quoted('[', ']')

	case c == '\'':
		sc.skipString('\'')
		return token{kind: tokString}

	case c == '$':
		return sc.dollar()

	case c >= '0' && c <= '9':
		for sc.i < len(sc.s) && (isWordPart(sc.s[sc.i]) || sc.s[sc.i] == '.') {
			sc.i++
		}
		return token{kind: tokString}
	}

	sc.i++
	return token{kind: tokPunct, text: sc.s[sc.i-1 : sc.i]}
}

func (sc *sqlScanner) skipSpaceAndComments() {
	for sc.i < len(sc.s) {
		c := sc.s[sc.i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			sc.i++
		case c == '-' && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '-':
			end := strings.IndexByte(sc.s[sc.i:], '\n')
			if end < 0 {
				sc.i = len(sc.s)
				return
			}
			sc.i += end + 1
		case c == '/' && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '*':
			end := strings.Index(sc.s[sc.i+2:], "*/")
			if end < 0 {
				sc.i = len(sc.s)
				return
			}
			sc.i += end + 4
		default:
			return
		}
	}
}

// quoted reads a quoted identifier. A doubled closing quote is an escaped one.
func (sc *sqlScanner) quoted(open, closeQuote byte) token {
	sc.i++ // the opening quote
	var b strings.Builder
	for sc.i < len(sc.s) {
		c := sc.s[sc.i]
		sc.i++
		if c != closeQuote {
			b.WriteByte(c)
			continue
		}
		if open == closeQuote && sc.i < len(sc.s) && sc.s[sc.i] == closeQuote {
			b.WriteByte(c)
			sc.i++
			continue
		}
		return token{kind: tokQuoted, text: b.String()}
	}
	return token{kind: tokEOF}
}

// skipString moves past a string literal. A doubled quote is an escaped one.
func (sc *sqlScanner) skipString(q byte) {
	sc.i++
	for sc.i < len(sc.s) {
		if sc.s[sc.i] == q {
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == q {
				sc.i += 2
				continue
			}
			sc.i++
			return
		}
		sc.i++
	}
}

// dollar reads a Postgres placeholder ($1) or skips a dollar-quoted body
// ($$...$$ or $tag$...$tag$).
func (sc *sqlScanner) dollar() token {
	start := sc.i
	sc.i++
	if sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
		for sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
			sc.i++
		}
		return token{kind: tokString}
	}

	j := sc.i
	for j < len(sc.s) && isTagChar(sc.s[j]) {
		j++
	}
	if j >= len(sc.s) || sc.s[j] != '$' {
		return token{kind: tokPunct, text: "$"}
	}

	tag := sc.s[start : j+1]
	end := strings.Index(sc.s[j+1:], tag)
	if end < 0 {
		sc.i = len(sc.s)
		return token{kind: tokEOF}
	}
	sc.i = j + 1 + end + len(tag)
	return token{kind: tokString}
}

func isWordStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isWordPart(c byte) bool {
	return isWordStart(c) || (c >= '0' && c <= '9') || c == '$'
}

// isTagChar reports whether c can sit inside a dollar-quote tag. Unlike a
// word, a tag can't contain '$', which is what closes it.
func isTagChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
