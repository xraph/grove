package tursodriver

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

func TestTelemetry(t *testing.T) {
	rec := telemetrytest.Spans(t)

	db := New()
	db.db = telemetrytest.SQLDB(t, func(q string) error {
		switch {
		case strings.Contains(q, "dup"):
			return errors.New("SQLITE_CONSTRAINT: UNIQUE constraint failed")
		case strings.Contains(q, "missing"):
			return sql.ErrNoRows
		}
		return nil
	})
	db.tel.SetConnection(observability.ConnectionFromURL("libsql://grove-demo.turso.io?authToken=secret"))

	m := telemetrytest.NewMetrics()
	if _, err := grove.Open(db, grove.WithMetrics(m)); err != nil {
		t.Fatal(err)
	}

	ctx, parent := telemetrytest.Parent(t.Context())

	if _, err := db.Exec(ctx, `INSERT INTO "users" ("email") VALUES (?)`, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := db.QueryRow(ctx, `SELECT "id" FROM "users" WHERE "email" = ?`, "alice@example.com").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT "id" FROM "users" WHERE missing = ?`, 1).Scan(&n); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("want no rows, got %v", err)
	}
	rows, err := db.Query(ctx, `SELECT "id" FROM "events"`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE "users" SET "email" = ?`, "x"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()

	if _, err := db.Exec(ctx, `INSERT INTO "users" ("email") VALUES ('dup')`); err == nil {
		t.Fatal("want the duplicate error back")
	}

	parent.End()

	spans := telemetrytest.Ended(rec)
	got := make([]string, len(spans))
	for i, s := range spans {
		got[i] = s.Name()
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "turso" {
			t.Errorf("%s: db.system.name = %q", s.Name(), attrs["db.system.name"])
		}
		if attrs["server.address"] != "grove-demo.turso.io" {
			t.Errorf("%s: connection attributes %v", s.Name(), attrs)
		}
		for _, v := range attrs {
			if strings.Contains(v, "secret") {
				t.Error("auth token leaked")
			}
		}
		for k, v := range attrs {
			if strings.Contains(v, "alice") {
				t.Errorf("%s: %s carries a bound value", s.Name(), k)
			}
		}
	}
	want := "INSERT users,SELECT users,SELECT users,SELECT events,BEGIN,UPDATE users,COMMIT,INSERT users"
	if strings.Join(got, ",") != want {
		t.Fatalf("spans = %s\nwant    %s", strings.Join(got, ","), want)
	}

	if s := spans[2]; s.Status().Code == codes.Error || telemetrytest.HasAttr(s, "error.type") {
		t.Error("a no-rows lookup was marked failed")
	}
	failed := spans[7]
	if failed.Status().Code != codes.Error {
		t.Fatalf("duplicate insert not marked failed: %v", failed.Status())
	}
	if v, _ := telemetrytest.Attr(failed, "error.type"); v != "*errors.errorString" {
		t.Errorf("error.type = %q, want %q", v, "*errors.errorString")
	}
	if v, ok := telemetrytest.Attr(failed, "db.response.status_code"); v != "" || ok != (false) {
		t.Errorf("db.response.status_code = %q (set %v), want %q", v, ok, "")
	}

	ops := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		key := strings.TrimSpace(s.Labels["db.operation.name"] + " " + s.Labels["db.collection.name"])
		if e := s.Labels["error.type"]; e != "" {
			key += " !" + e
		}
		ops[key] += s.Count
		if _, leaked := s.Labels["server.address"]; leaked {
			t.Error("server.address leaked onto the metric")
		}
	}
	if ops["INSERT users"] != 1 || ops["INSERT users !*errors.errorString"] != 1 || ops["SELECT users"] != 2 ||
		ops["BEGIN"] != 1 || ops["COMMIT"] != 1 || ops["ROLLBACK"] != 0 {
		t.Errorf("series = %v", ops)
	}
}
