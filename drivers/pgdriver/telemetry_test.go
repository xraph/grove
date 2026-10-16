package pgdriver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

// fakePG is just enough of a Postgres server to answer pgx in simple-protocol
// mode, so the telemetry tests run without Docker. It answers by what the
// query contains:
//
//	"fail_unique"  ErrorResponse with SQLSTATE 23505
//	"no_rows"      an empty result
//	SELECT         one row, one bigint column
//	anything else  a command tag
type fakePG struct {
	ln net.Listener
	wg sync.WaitGroup

	mu      sync.Mutex
	queries []string
}

func startFakePG(t *testing.T) *fakePG {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakePG{ln: ln}

	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.serve(conn)
			}()
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		f.wg.Wait()
	})
	return f
}

func (f *fakePG) dsn() string {
	addr, _ := f.ln.Addr().(*net.TCPAddr)
	return fmt.Sprintf("postgres://grove@127.0.0.1:%d/appdb?sslmode=disable&default_query_exec_mode=simple_protocol", addr.Port)
}

func (f *fakePG) serve(conn net.Conn) {
	defer conn.Close()

	be := pgproto3.NewBackend(conn, conn)
	if _, err := be.ReceiveStartupMessage(); err != nil {
		return
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	be.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		return
	}

	txStatus := byte('I')
	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		q, ok := msg.(*pgproto3.Query)
		if !ok {
			return // Terminate, or anything the fake doesn't speak
		}

		f.mu.Lock()
		f.queries = append(f.queries, q.String)
		f.mu.Unlock()

		lower := strings.ToLower(strings.TrimSpace(q.String))
		switch {
		case strings.Contains(lower, "fail_unique"):
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505",
				Message: `duplicate key value violates unique constraint "users_email_key"`})
			if txStatus == 'T' {
				txStatus = 'E'
			}
		case strings.HasPrefix(lower, "begin"):
			txStatus = 'T'
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		case strings.HasPrefix(lower, "commit"):
			txStatus = 'I'
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		case strings.HasPrefix(lower, "rollback"):
			txStatus = 'I'
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
		case strings.Contains(lower, "no_rows"):
			be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{bigintField("n")}})
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		case strings.HasPrefix(lower, "select") || strings.HasPrefix(lower, "-- ping"):
			if strings.HasPrefix(lower, "-- ping") {
				be.Send(&pgproto3.EmptyQueryResponse{})
				break
			}
			be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{bigintField("n")}})
			be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		default:
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
		}
		be.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
		if err := be.Flush(); err != nil {
			return
		}
	}
}

func bigintField(name string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1}
}

func openFake(t *testing.T) (*PgDB, *telemetrytest.Metrics) {
	t.Helper()

	f := startFakePG(t)
	db := New()
	if err := db.Open(t.Context(), f.dsn()); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m := telemetrytest.NewMetrics()
	if _, err := grove.Open(db, grove.WithMetrics(m)); err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	return db, m
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	return names
}

func TestTelemetryQueryUnderParent(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	var n int64
	if err := db.QueryRow(ctx, `SELECT "n" FROM "jobs" WHERE "id" = $1`, 7).Scan(&n); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE "jobs" SET "state" = $1 WHERE "id" = $2`, "done", 7); err != nil {
		t.Fatalf("exec: %v", err)
	}
	rows, err := db.Query(ctx, `SELECT "n" FROM "events"`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	got := spanNames(spans)
	want := []string{"SELECT jobs", "UPDATE jobs", "SELECT events"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("spans = %v, want %v", got, want)
	}

	for _, s := range spans {
		if s.SpanKind() != trace.SpanKindClient {
			t.Errorf("%s: kind %v", s.Name(), s.SpanKind())
		}
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s: not a child of the request span", s.Name())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "postgresql" || attrs["db.namespace"] != "appdb" ||
			attrs["server.address"] != "127.0.0.1" || attrs["server.port"] == "" {
			t.Errorf("%s: attributes %v", s.Name(), attrs)
		}
		for k, v := range attrs {
			if strings.Contains(v, "done") {
				t.Errorf("%s: %s carries a bound value: %q", s.Name(), k, v)
			}
		}
	}

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 3 {
		t.Fatalf("got %d series, want 3: %+v", len(series), series)
	}
	for _, s := range series {
		if s.Labels["db.system.name"] != "postgresql" || s.Labels["db.namespace"] != "appdb" || s.Count != 1 {
			t.Errorf("series %v count %d", s.Labels, s.Count)
		}
		if _, ok := s.Labels["server.address"]; ok {
			t.Error("server.address leaked onto the metric")
		}
	}

	// The same calls again add observations, not series.
	for range 2 {
		if _, err := db.Exec(ctx, `UPDATE "jobs" SET "state" = $1 WHERE "id" = $2`, "done", 8); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(m.Series(observability.OperationDurationMetric)); got != 3 {
		t.Fatalf("repeat calls added series: %d", got)
	}
}

func TestTelemetryFailureCarriesSQLState(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	_, err := db.Exec(ctx, `INSERT INTO "users" ("email") VALUES ('fail_unique')`)
	parent.End()
	if err == nil {
		t.Fatal("want the unique violation back")
	}
	if sqlState(err) != "23505" {
		t.Fatalf("driver error lost its SQLSTATE: %v", err)
	}

	s := telemetrytest.Ended(rec)[0]
	if s.Name() != "INSERT users" || s.Status().Code != codes.Error {
		t.Fatalf("span %q status %v", s.Name(), s.Status())
	}
	attrs := telemetrytest.AttrMap(s)
	if attrs["error.type"] != "23505" || attrs["db.response.status_code"] != "23505" {
		t.Errorf("error attributes = %v", attrs)
	}

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 1 || series[0].Labels["error.type"] != "23505" || series[0].Labels["db.response.status_code"] != "23505" {
		t.Fatalf("failure series = %+v", series)
	}
}

func TestTelemetryNoRowsIsNotAnError(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	var n int64
	err := db.QueryRow(ctx, `SELECT "n" FROM "users" WHERE "email" = 'no_rows'`).Scan(&n)
	parent.End()
	if err == nil {
		t.Fatal("want no rows")
	}

	s := telemetrytest.Ended(rec)[0]
	if s.Status().Code == codes.Error || telemetrytest.HasAttr(s, "error.type") {
		t.Errorf("no rows marked the span failed: %v %v", s.Status(), telemetrytest.AttrMap(s))
	}
	if got := m.Series(observability.OperationDurationMetric)[0].Labels["error.type"]; got != "" {
		t.Errorf("error.type = %q on a no-rows lookup", got)
	}
}

func TestTelemetryTransaction(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	tx, err := db.BeginTxQuery(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err = tx.NewRaw(`UPDATE "jobs" SET "state" = 'running'`).Exec(ctx); err != nil {
		t.Fatalf("tx exec: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// The usual deferred Rollback after a commit: no round trip, no span.
	if err = tx.Rollback(); err == nil {
		t.Log("rollback after commit returned nil")
	}

	tx2, err := db.BeginTxQuery(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	parent.End()

	spans := telemetrytest.Ended(rec)
	got := strings.Join(spanNames(spans), ",")
	if want := "BEGIN,UPDATE jobs,COMMIT,BEGIN,ROLLBACK"; got != want {
		t.Fatalf("spans = %s, want %s", got, want)
	}
	for _, s := range spans {
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
	}

	ops := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		ops[s.Labels["db.operation.name"]] += s.Count
	}
	if ops["BEGIN"] != 2 || ops["COMMIT"] != 1 || ops["ROLLBACK"] != 1 {
		t.Errorf("transaction operations recorded = %v", ops)
	}
}

func TestTelemetryPreparedStatement(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, _ := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ptx, ok := tx.(*pgTx)
	if !ok {
		t.Fatalf("BeginTx returned %T", tx)
	}

	// The fake doesn't speak the extended protocol, so drive the statement
	// wrapper directly with a statement name it will treat as a command.
	stmt := &pgTxStmt{name: "INSERT INTO audit DEFAULT VALUES", query: `INSERT INTO "audit" ("a") VALUES ($1)`, tx: ptx}
	if _, err := stmt.Exec(ctx); err != nil {
		t.Fatalf("stmt exec: %v", err)
	}
	parent.End()

	var found bool
	for _, s := range telemetrytest.Ended(rec) {
		if s.Name() == "INSERT audit" {
			found = true
		}
	}
	if !found {
		t.Fatalf("prepared statement span missing: %v", spanNames(telemetrytest.Ended(rec)))
	}
}

func TestTelemetryWithoutParentRecordsMetricOnly(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, m := openFake(t)

	if _, err := db.Exec(context.Background(), `DELETE FROM "jobs" WHERE "id" = $1`, 1); err != nil {
		t.Fatal(err)
	}
	if n := len(telemetrytest.Ended(rec)); n != 0 {
		t.Fatalf("a call with no parent span made %d spans", n)
	}
	if s := m.Series(observability.OperationDurationMetric); len(s) != 1 || s[0].Labels["db.operation.name"] != "DELETE" {
		t.Fatalf("series = %+v", s)
	}
}

func TestTelemetryContextCanceled(t *testing.T) {
	rec := telemetrytest.Spans(t)
	db, _ := openFake(t)

	ctx, parent := telemetrytest.Parent(t.Context())
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := db.Exec(cctx, `UPDATE "jobs" SET "state" = 'x'`)
	parent.End()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	s := telemetrytest.Ended(rec)[0]
	if v, _ := telemetrytest.Attr(s, "error.type"); v != "context.Canceled" {
		t.Errorf("error.type = %q", v)
	}
}
