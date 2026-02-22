package grovetest

import (
	"strings"
	"testing"
)

// AssertQueryContains asserts that at least one recorded query contains the given substring.
func AssertQueryContains(t *testing.T, d *MockDriver, substring string) {
	t.Helper()
	for _, q := range d.Queries() {
		if strings.Contains(q.Query, substring) {
			return
		}
	}
	t.Errorf("no query contains %q; recorded queries: %v", substring, querySummary(d))
}

// AssertQueryCount asserts that exactly n queries were recorded.
func AssertQueryCount(t *testing.T, d *MockDriver, n int) {
	t.Helper()
	actual := len(d.Queries())
	if actual != n {
		t.Errorf("expected %d queries, got %d; recorded: %v", n, actual, querySummary(d))
	}
}

// AssertLastQuery asserts the last query matches the given string exactly.
func AssertLastQuery(t *testing.T, d *MockDriver, expected string) {
	t.Helper()
	last := d.LastQuery()
	if last == nil {
		t.Error("no queries recorded")
		return
	}
	if last.Query != expected {
		t.Errorf("last query:\n  got:  %s\n  want: %s", last.Query, expected)
	}
}

// AssertLastArgs asserts the args of the last query match.
func AssertLastArgs(t *testing.T, d *MockDriver, expected ...any) {
	t.Helper()
	last := d.LastQuery()
	if last == nil {
		t.Error("no queries recorded")
		return
	}
	if len(last.Args) != len(expected) {
		t.Errorf("last query args length: got %d, want %d", len(last.Args), len(expected))
		return
	}
	for i, exp := range expected {
		if last.Args[i] != exp {
			t.Errorf("arg[%d]: got %v, want %v", i, last.Args[i], exp)
		}
	}
}

// AssertNoQueries asserts that no queries were recorded.
func AssertNoQueries(t *testing.T, d *MockDriver) {
	t.Helper()
	if len(d.Queries()) > 0 {
		t.Errorf("expected no queries, got %d: %v", len(d.Queries()), querySummary(d))
	}
}

func querySummary(d *MockDriver) []string {
	var summaries []string
	for _, q := range d.Queries() {
		summaries = append(summaries, q.Method+": "+q.Query)
	}
	return summaries
}
