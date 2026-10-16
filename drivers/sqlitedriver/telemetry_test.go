package sqlitedriver

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

func openTelemetryDB(t *testing.T) (*SqliteDB, *telemetrytest.Metrics) {
	t.Helper()

	db := New()
	if err := db.Open(t.Context(), filepath.Join(t.TempDir(), "telemetry.db")); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m := telemetrytest.NewMetrics()
	if _, err := grove.Open(db, grove.WithMetrics(m)); err != nil {
		t.Fatalf("grove.Open: %v", err)
	}

	// Schema setup runs without a parent span, so it adds metric
	// observations but no spans.
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db, m
}

func names(spans []sdktrace.ReadOnlySpan) string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return strings.Join(out, ",")
}

func countByOp(m *telemetrytest.Metrics) map[string]uint64 {
	ops := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		key := s.Labels["db.operation.name"]
		if c := s.Labels["db.collection.name"]; c != "" {
			key += " " + c
		}
		if e := s.Labels["error.type"]; e != "" {
			key += " !" + e
		}
		ops[key] += s.Count
	}
	return ops
}

func TestTelemetryHappyPath(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openTelemetryDB(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	if _, err := db.Exec(ctx, `INSERT INTO users (email) VALUES (?)`, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	var email string
	if err := db.QueryRow(ctx, `SELECT email FROM users WHERE id = ?`, 1).Scan(&email); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, `SELECT id FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	if got, want := names(spans), "INSERT users,SELECT users,SELECT users"; got != want {
		t.Fatalf("spans = %s, want %s", got, want)
	}
	for _, s := range spans {
		if s.SpanKind() != trace.SpanKindClient || s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s: kind %v, parent %v", s.Name(), s.SpanKind(), s.Parent().SpanID())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "sqlite" {
			t.Errorf("%s: db.system.name = %q", s.Name(), attrs["db.system.name"])
		}
		for k, v := range attrs {
			if strings.Contains(v, "alice") {
				t.Errorf("%s: %s carries a bound value", s.Name(), k)
			}
		}
	}

	ops := countByOp(m)
	if ops["CREATE TABLE users"] != 1 || ops["INSERT users"] != 1 || ops["SELECT users"] != 2 {
		t.Errorf("series = %v", ops)
	}
}

func TestTelemetryConstraintFailure(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openTelemetryDB(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	for range 2 {
		_, err := db.Exec(ctx, `INSERT INTO users (email) VALUES (?)`, "dup@example.com")
		if err != nil {
			break
		}
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	failed := spans[len(spans)-1]
	if failed.Status().Code != codes.Error {
		t.Fatalf("second insert should fail: %v", failed.Status())
	}
	attrs := telemetrytest.AttrMap(failed)
	// 2067 is SQLITE_CONSTRAINT_UNIQUE.
	if attrs["error.type"] != "2067" || attrs["db.response.status_code"] != "2067" {
		t.Errorf("error attributes = %v", attrs)
	}
	if countByOp(m)["INSERT users !2067"] != 1 {
		t.Errorf("series = %v", countByOp(m))
	}
}

func TestTelemetryNoRowsAndTransactions(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openTelemetryDB(t)

	ctx, parent := telemetrytest.Parent(t.Context())

	var email string
	if err := db.QueryRow(ctx, `SELECT email FROM users WHERE id = ?`, 404).Scan(&email); err == nil {
		t.Fatal("want no rows")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO users (email) VALUES (?)`, "tx@example.com"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx2.(*sqliteTx).Prepare(ctx, `DELETE FROM users WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(ctx, 1); err != nil {
		t.Fatal(err)
	}
	_ = stmt.Close()
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	if got, want := names(spans), "SELECT users,BEGIN,INSERT users,COMMIT,BEGIN,DELETE users,ROLLBACK"; got != want {
		t.Fatalf("spans = %s\nwant    %s", got, want)
	}
	if spans[0].Status().Code == codes.Error || telemetrytest.HasAttr(spans[0], "error.type") {
		t.Error("a no-rows lookup was marked failed")
	}
	for _, s := range spans {
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
	}

	ops := countByOp(m)
	if ops["BEGIN"] != 2 || ops["COMMIT"] != 1 || ops["ROLLBACK"] != 1 {
		t.Errorf("transaction series = %v", ops)
	}
}

func TestTelemetryBuilderAndNoParent(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openTelemetryDB(t)

	type user struct {
		grove.BaseModel `grove:"table:users"`
		ID              int64  `grove:"id,pk,autoincrement"`
		Email           string `grove:"email"`
	}

	u := &user{Email: "builder@example.com"}
	if _, err := db.NewInsert(u).Exec(context.Background()); err != nil {
		t.Fatalf("builder insert: %v", err)
	}
	if n := len(telemetrytest.Ended(rec)); n != 0 {
		t.Fatalf("a call with no parent made %d spans", n)
	}
	if countByOp(m)["INSERT users"] != 1 {
		t.Fatalf("builder insert not recorded: %v", countByOp(m))
	}
}
