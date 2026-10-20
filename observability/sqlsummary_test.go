package observability

import (
	"math"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSummarizeSQL(t *testing.T) {
	tests := []struct {
		name  string
		query string
		op    string
		coll  string
	}{
		// What grove's builders produce.
		{"pg select", `SELECT "u"."id", "u"."email" FROM "users" AS "u" WHERE ("u"."id" = $1)`, "SELECT", "users"},
		{"pg insert returning", `INSERT INTO "users" ("id", "email") VALUES ($1, $2) RETURNING "id"`, "INSERT", "users"},
		{"pg update", `UPDATE "users" SET "email" = $1 WHERE ("id" = $2)`, "UPDATE", "users"},
		{"pg delete", `DELETE FROM "users" WHERE ("id" = $1)`, "DELETE", "users"},
		{"mysql backticks", "SELECT `id` FROM `orders` WHERE `id` = ?", "SELECT", "orders"},
		{"sqlserver brackets", "SELECT [id] FROM [dbo].[orders]", "SELECT", "dbo.orders"},
		{"schema qualified", `SELECT id FROM public.jobs WHERE state = 'pending'`, "SELECT", "public.jobs"},
		{"quoted schema", `SELECT 1 FROM "dispatch"."jobs"`, "SELECT", "dispatch.jobs"},
		{"lower case", `select * from jobs`, "SELECT", "jobs"},
		{"leading comment", "-- poll\n/* worker */ SELECT id FROM jobs", "SELECT", "jobs"},

		// Things that must not be taken for the table.
		{"subquery in from", `SELECT * FROM (SELECT id FROM jobs) AS j`, "SELECT", ""},
		{"from inside a function", `SELECT EXTRACT(YEAR FROM created_at) FROM events`, "SELECT", "events"},
		{"from inside a string", `SELECT 'x FROM y' AS s FROM real_table`, "SELECT", "real_table"},
		{"from in a column subquery", `SELECT (SELECT max(id) FROM a) AS m FROM b`, "SELECT", "b"},
		{"no from", `SELECT pg_advisory_lock($1)`, "SELECT", ""},
		{"select one", `SELECT 1`, "SELECT", ""},
		{"dollar quoted body", `SELECT $tag$ FROM nope $tag$ FROM yes`, "SELECT", "yes"},
		{"empty dollar quote", `SELECT $$ FROM nope $$ FROM yes`, "SELECT", "yes"},

		// CTEs: the main statement wins.
		{"with select", `WITH recent AS (SELECT id FROM jobs WHERE x > 1) SELECT * FROM recent`, "SELECT", "recent"},
		{"with recursive", `WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT n FROM t`, "SELECT", "t"},
		{"with update", `WITH c AS (SELECT id FROM jobs LIMIT 10 FOR UPDATE SKIP LOCKED) UPDATE jobs SET state = 'running' FROM c WHERE jobs.id = c.id`, "UPDATE", "jobs"},
		{"with delete", `WITH old AS (SELECT id FROM e) DELETE FROM events USING old`, "DELETE", "events"},

		// Dialect modifiers.
		{"sqlite insert or replace", `INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)`, "INSERT", "kv"},
		{"mysql insert ignore", `INSERT IGNORE INTO t (a) VALUES (?)`, "INSERT", "t"},
		{"mysql replace", `REPLACE INTO t (a) VALUES (?)`, "REPLACE", "t"},
		{"clickhouse insert into table", `INSERT INTO TABLE metrics (a) VALUES (?)`, "INSERT", "metrics"},
		{"sqlite update or ignore", `UPDATE OR IGNORE t SET a = 1`, "UPDATE", "t"},
		{"mysql update low priority", `UPDATE LOW_PRIORITY IGNORE t SET a = 1`, "UPDATE", "t"},
		{"pg update only", `UPDATE ONLY parent SET a = 1`, "UPDATE", "parent"},
		{"pg delete only", `DELETE FROM ONLY parent WHERE a = 1`, "DELETE", "parent"},
		{"merge", `MERGE INTO target t USING src s ON t.id = s.id`, "MERGE", "target"},
		{"parenthesised select", `(SELECT id FROM a) UNION (SELECT id FROM b)`, "SELECT", "a"},

		// DDL.
		{"create table", `CREATE TABLE IF NOT EXISTS "users" ("id" bigint)`, "CREATE TABLE", "users"},
		{"create temp table", `CREATE TEMPORARY TABLE scratch (id int)`, "CREATE TABLE", "scratch"},
		{"drop table", `DROP TABLE IF EXISTS users CASCADE`, "DROP TABLE", "users"},
		{"alter table", `ALTER TABLE users ADD COLUMN name text`, "ALTER TABLE", "users"},
		{"create index", `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email)`, "CREATE INDEX", "users"},
		{"create index concurrently", `CREATE INDEX CONCURRENTLY idx ON ONLY jobs (state)`, "CREATE INDEX", "jobs"},
		{"drop index", `DROP INDEX idx_users_email`, "DROP INDEX", ""},
		{"create view", `CREATE OR REPLACE VIEW v AS SELECT 1`, "CREATE VIEW", ""},
		{"create extension", `CREATE EXTENSION IF NOT EXISTS pgcrypto`, "CREATE EXTENSION", ""},
		{"create unknown object", `CREATE POLICY p ON t`, "CREATE", ""},
		{"truncate", `TRUNCATE TABLE jobs`, "TRUNCATE", "jobs"},

		// Statements reported by their keyword alone.
		{"begin", `BEGIN`, "BEGIN", ""},
		{"commit", `COMMIT`, "COMMIT", ""},
		{"savepoint", `SAVEPOINT sp_42`, "SAVEPOINT", ""},
		{"pragma", `PRAGMA journal_mode=WAL`, "PRAGMA", ""},
		{"notify", `NOTIFY dispatch_jobs, 'payload'`, "NOTIFY", ""},
		{"set", `SET LOCAL app.tenant_id = 'abc'`, "SET", ""},

		// Garbage gets nothing, so it can't add label values.
		{"empty", ``, "", ""},
		{"only space", "  \n\t", "", ""},
		{"unknown keyword", `FROBNICATE everything`, "", ""},
		{"number first", `42`, "", ""},
		{"unterminated comment", `/* SELECT FROM t`, "", ""},
		{"unterminated quote", `SELECT * FROM "users`, "SELECT", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := summarize(tc.query)
			if got.Operation != tc.op || got.Collection != tc.coll {
				t.Fatalf("summarize(%q) = (%q, %q), want (%q, %q)", tc.query, got.Operation, got.Collection, tc.op, tc.coll)
			}

			want := tc.op
			if tc.coll != "" {
				want += " " + tc.coll
			}
			if got.Summary != want {
				t.Fatalf("summary = %q, want %q", got.Summary, want)
			}
		})
	}
}

// The metric label is built from the table without its qualifier. The
// scanner splits the parts, so a quoted name holding a dot stays whole.
func TestSummarizeSQLTable(t *testing.T) {
	tests := []struct {
		query, coll, table string
	}{
		{`SELECT * FROM jobs`, "jobs", "jobs"},
		{`SELECT * FROM public.jobs`, "public.jobs", "jobs"},
		{`SELECT "id" FROM "tenant_42"."fabriq_crdt_docs" WHERE "id" = $1`, "tenant_42.fabriq_crdt_docs", "fabriq_crdt_docs"},
		{"SELECT `id` FROM `analytics`.`events`", "analytics.events", "events"},
		{"SELECT [id] FROM [db].[dbo].[orders]", "db.dbo.orders", "orders"},
		{`SELECT 1 FROM "odd.name"`, "odd.name", "odd.name"},
		{`SELECT 1 FROM "s"."odd.name"`, "s.odd.name", "odd.name"},
		{`INSERT INTO tenant_7.events (a) VALUES ($1)`, "tenant_7.events", "events"},
		{`UPDATE ONLY tenant_7.events SET a = 1`, "tenant_7.events", "events"},
		{`CREATE TABLE IF NOT EXISTS tenant_7.events (id int)`, "tenant_7.events", "events"},
		{`CREATE INDEX idx ON tenant_7.events (id)`, "tenant_7.events", "events"},
		{`SELECT * FROM (SELECT 1) AS s`, "", ""},
		{`COMMIT`, "", ""},
	}
	for _, tc := range tests {
		got := summarize(tc.query)
		if got.Collection != tc.coll || got.table != tc.table {
			t.Errorf("summarize(%q) = (%q, %q), want (%q, %q)", tc.query, got.Collection, got.table, tc.coll, tc.table)
		}
	}
}

func TestSummarizeSQLNeverReturnsValues(t *testing.T) {
	q := `SELECT * FROM users WHERE email = 'alice@example.com' AND id = 42`
	got := SummarizeSQL(q)
	for _, s := range []string{got.Operation, got.Collection, got.Summary} {
		if strings.Contains(s, "alice") || strings.Contains(s, "42") {
			t.Fatalf("summary leaked a value: %+v", got)
		}
	}
}

func TestSummarizeSQLDropsOverlongNames(t *testing.T) {
	q := "SELECT * FROM " + strings.Repeat("a", maxIdentifierLen+1)
	if got := summarize(q); got.Collection != "" {
		t.Fatalf("collection = %q, want empty for a name over %d bytes", got.Collection, maxIdentifierLen)
	}
}

func TestSummarizeSQLCachesRepeats(t *testing.T) {
	q := `SELECT id FROM cache_probe_table WHERE id = $1`
	first := SummarizeSQL(q)

	v, ok := summaryCache.m.Load(q)
	if !ok {
		t.Fatal("summary was not cached")
	}
	if cached, _ := v.(sqlShape); cached.SQLSummary != first {
		t.Fatalf("cached %+v, returned %+v", cached.SQLSummary, first)
	}

	long := "SELECT id FROM t WHERE " + strings.Repeat("a = $1 AND ", maxCachedQueryLen/10)
	SummarizeSQL(long)
	if _, ok := summaryCache.m.Load(long); ok {
		t.Fatal("a statement over maxCachedQueryLen was cached")
	}
}

func BenchmarkSummarizeSQL(b *testing.B) {
	q := `SELECT "j"."id", "j"."state", "j"."payload" FROM "dispatch_jobs" AS "j" WHERE ("j"."state" = $1) ORDER BY "j"."run_at" LIMIT 10`

	b.Run("cached", func(b *testing.B) {
		SummarizeSQL(q)
		b.ReportAllocs()
		for b.Loop() {
			SummarizeSQL(q)
		}
	})

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			summarize(q)
		}
	})
}

// The cache counters stop at their cap. If they kept counting misses they
// would wrap negative after 2^31 of them and the caches would grow again.
func TestClaimStopsAtTheLimit(t *testing.T) {
	var n atomic.Int32
	for i := range 3 {
		if !claim(&n, 3) {
			t.Fatalf("claim %d failed under the limit", i)
		}
	}
	for range 5 {
		if claim(&n, 3) {
			t.Fatal("claim succeeded past the limit")
		}
	}
	if got := n.Load(); got != 3 {
		t.Errorf("counter = %d after misses past the limit, want 3", got)
	}

	n.Store(math.MaxInt32)
	if claim(&n, math.MaxInt32) || n.Load() != math.MaxInt32 {
		t.Errorf("counter moved off MaxInt32: %d", n.Load())
	}
}

func TestSummaryCacheStopsCountingWhenFull(t *testing.T) {
	saved := summaryCache.n.Load()
	t.Cleanup(func() { summaryCache.n.Store(saved) })
	summaryCache.n.Store(maxCachedSummaries)

	q := `SELECT id FROM cache_full_probe`
	SummarizeSQL(q)
	SummarizeSQL(q)

	if got := summaryCache.n.Load(); got != maxCachedSummaries {
		t.Errorf("counter = %d, want it to stay at %d", got, maxCachedSummaries)
	}
	if _, ok := summaryCache.m.Load(q); ok {
		t.Error("a statement was cached past the cap")
	}
}
