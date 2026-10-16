package observability_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

type codeError struct{ code string }

func (e *codeError) Error() string { return "db error " + e.code }

var errNotFound = errors.New("not found")

func newTelemetry() *observability.Telemetry {
	t := observability.NewTelemetry("postgresql",
		observability.WithStatusCode(func(err error) string {
			var ce *codeError
			if errors.As(err, &ce) {
				return ce.code
			}
			return ""
		}),
		observability.WithBenign(func(err error) bool { return errors.Is(err, errNotFound) }),
	)
	t.SetConnection("app", "db.internal", 5432)
	return t
}

func TestStartSQLSpanUnderParent(t *testing.T) {
	rec := telemetrytest.Spans(t)
	tel := newTelemetry()

	ctx, parent := telemetrytest.Parent(t.Context())
	_, op := tel.StartSQL(ctx, `SELECT "id" FROM "jobs" WHERE "id" = $1`)
	op.End(nil)
	parent.End()

	spans := telemetrytest.Ended(rec)
	if len(spans) != 1 {
		t.Fatalf("got %d grove spans, want 1", len(spans))
	}
	s := spans[0]

	if s.Name() != "SELECT jobs" {
		t.Errorf("name = %q, want %q", s.Name(), "SELECT jobs")
	}
	if s.SpanKind() != trace.SpanKindClient {
		t.Errorf("kind = %v, want client", s.SpanKind())
	}
	if s.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Error("span is not a child of the parent")
	}
	if s.Status().Code != codes.Unset {
		t.Errorf("status = %v, want unset", s.Status().Code)
	}

	want := map[string]string{
		"db.system.name":     "postgresql",
		"db.namespace":       "app",
		"db.operation.name":  "SELECT",
		"db.collection.name": "jobs",
		"db.query.summary":   "SELECT jobs",
		"server.address":     "db.internal",
		"server.port":        "5432",
	}
	if got := telemetrytest.AttrMap(s); !reflect.DeepEqual(got, want) {
		t.Errorf("attributes = %v\nwant %v", got, want)
	}
}

func TestNoSpanWithoutParent(t *testing.T) {
	rec := telemetrytest.Spans(t)
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()
	tel.SetMetrics(m)

	ctx, op := tel.StartSQL(t.Context(), `SELECT id FROM jobs`)
	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		t.Error("a span was put on a context that had none")
	}
	op.End(nil)

	if n := len(telemetrytest.Ended(rec)); n != 0 {
		t.Fatalf("got %d spans for a call with no parent, want 0", n)
	}

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 1 || series[0].Count != 1 {
		t.Fatalf("the histogram should still record without a parent span: %+v", series)
	}
}

func TestEndWithStatusCode(t *testing.T) {
	rec := telemetrytest.Spans(t)
	tel := newTelemetry()

	ctx, parent := telemetrytest.Parent(t.Context())
	_, op := tel.StartSQL(ctx, `INSERT INTO users (email) VALUES ($1)`)
	op.End(fmt.Errorf("pgdriver: exec: %w", &codeError{code: "23505"}))
	parent.End()

	s := telemetrytest.Ended(rec)[0]
	if s.Status().Code != codes.Error {
		t.Errorf("status = %v, want error", s.Status().Code)
	}
	if v, _ := telemetrytest.Attr(s, "error.type"); v != "23505" {
		t.Errorf("error.type = %q, want the status code", v)
	}
	if v, _ := telemetrytest.Attr(s, "db.response.status_code"); v != "23505" {
		t.Errorf("db.response.status_code = %q, want 23505", v)
	}
	if len(s.Events()) != 1 || s.Events()[0].Name != "exception" {
		t.Errorf("want one exception event, got %v", s.Events())
	}
}

// A driver's error message can quote the values it was given (a duplicate
// key, a bad integer), so the span carries the error's type and never its
// text.
func TestEndKeepsErrorTextOffTheSpan(t *testing.T) {
	const secret = "alice@example.com"

	tests := []struct {
		name string
		err  error
	}{
		{"with a status code", fmt.Errorf("duplicate entry '%s' for key 'users.email': %w", secret, &codeError{code: "1062"})},
		{"without one", fmt.Errorf("invalid input syntax for type integer: %q", secret)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := telemetrytest.Spans(t)
			tel := newTelemetry()

			ctx, parent := telemetrytest.Parent(t.Context())
			_, op := tel.StartSQL(ctx, `INSERT INTO users (email) VALUES ($1)`)
			op.End(tt.err)
			parent.End()

			s := telemetrytest.Ended(rec)[0]
			if s.Status().Code != codes.Error {
				t.Errorf("status = %v, want error", s.Status().Code)
			}
			if strings.Contains(s.Status().Description, secret) {
				t.Errorf("status description holds the error text: %q", s.Status().Description)
			}
			for k, v := range telemetrytest.AttrMap(s) {
				if strings.Contains(v, secret) {
					t.Errorf("attribute %s holds the error text: %q", k, v)
				}
			}
			for _, e := range s.Events() {
				for _, a := range e.Attributes {
					if strings.Contains(a.Value.String(), secret) {
						t.Errorf("%s event attribute %s holds the error text", e.Name, a.Key)
					}
				}
			}
		})
	}
}

// With no parent span and no metrics sink there's nothing to record, so a
// call shouldn't cost an allocation.
func TestDisabledCallsAllocateNothing(t *testing.T) {
	tel := newTelemetry()
	ctx := t.Context()

	allocs := testing.AllocsPerRun(100, func() {
		_, op := tel.StartSQL(ctx, `SELECT id FROM jobs WHERE id = $1`)
		op.End(nil)
		_, op = tel.Start(ctx, "find", "jobs")
		op.End(nil)
	})
	if allocs != 0 {
		t.Errorf("a call with no parent span and no sink allocated %v times, want 0", allocs)
	}
}

func TestErrorTypeWithoutStatusCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"wrapped plain error", fmt.Errorf("pgdriver: exec: %w", errors.New("boom")), "*errors.errorString"},
		{"deadline", fmt.Errorf("pgdriver: query: %w", context.DeadlineExceeded), "context.DeadlineExceeded"},
		{"canceled", fmt.Errorf("pgdriver: query: %w", context.Canceled), "context.Canceled"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := telemetrytest.Spans(t)
			tel := newTelemetry()

			ctx, parent := telemetrytest.Parent(t.Context())
			_, op := tel.StartSQL(ctx, `SELECT 1`)
			op.End(tc.err)
			parent.End()

			s := telemetrytest.Ended(rec)[0]
			if v, _ := telemetrytest.Attr(s, "error.type"); v != tc.want {
				t.Errorf("error.type = %q, want %q", v, tc.want)
			}
			if telemetrytest.HasAttr(s, "db.response.status_code") {
				t.Error("db.response.status_code set with no code to report")
			}
		})
	}
}

func TestBenignErrorsAreNotFailures(t *testing.T) {
	for _, err := range []error{sql.ErrNoRows, fmt.Errorf("wrapped: %w", sql.ErrNoRows), errNotFound} {
		t.Run(err.Error(), func(t *testing.T) {
			rec := telemetrytest.Spans(t)
			m := telemetrytest.NewMetrics()
			tel := newTelemetry()
			tel.SetMetrics(m)

			ctx, parent := telemetrytest.Parent(t.Context())
			_, op := tel.StartSQL(ctx, `SELECT id FROM users WHERE id = $1`)
			op.End(err)
			parent.End()

			s := telemetrytest.Ended(rec)[0]
			if s.Status().Code != codes.Unset || telemetrytest.HasAttr(s, "error.type") {
				t.Errorf("a benign error marked the span failed: status %v", s.Status())
			}
			if got := m.Series(observability.OperationDurationMetric)[0].Labels["error.type"]; got != "" {
				t.Errorf("metric error.type = %q, want empty", got)
			}
		})
	}
}

func TestMetricSeries(t *testing.T) {
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()
	tel.SetMetrics(m)
	ctx := t.Context()

	for range 3 {
		_, op := tel.StartSQL(ctx, `SELECT id FROM jobs WHERE state = $1`)
		op.End(nil)
	}
	_, op := tel.StartSQL(ctx, `UPDATE jobs SET state = $1 WHERE id = $2`)
	op.End(&codeError{code: "40001"})

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 2 {
		t.Fatalf("got %d series, want 2: %+v", len(series), series)
	}

	for _, s := range series {
		if s.Unit != "s" {
			t.Errorf("unit = %q, want s", s.Unit)
		}
		if !reflect.DeepEqual(s.Buckets, observability.OperationDurationBuckets) {
			t.Errorf("buckets = %v", s.Buckets)
		}
		if len(s.Labels) != 6 {
			t.Errorf("want all six labels on every series, got %v", s.Labels)
		}
	}

	sel, upd := series[0], series[1]
	if sel.Labels["db.operation.name"] != "SELECT" {
		sel, upd = upd, sel
	}

	wantSel := map[string]string{
		"db.system.name": "postgresql", "db.namespace": "app", "db.operation.name": "SELECT",
		"db.collection.name": "jobs", "db.response.status_code": "", "error.type": "",
	}
	if !reflect.DeepEqual(sel.Labels, wantSel) || sel.Count != 3 {
		t.Errorf("select series = %v (count %d)", sel.Labels, sel.Count)
	}
	if upd.Labels["error.type"] != "40001" || upd.Labels["db.response.status_code"] != "40001" || upd.Count != 1 {
		t.Errorf("update series = %v (count %d)", upd.Labels, upd.Count)
	}

	// The same call again adds an observation, not a series.
	_, op = tel.StartSQL(ctx, `SELECT id FROM jobs WHERE state = $1`)
	op.End(nil)
	if got := len(m.Series(observability.OperationDurationMetric)); got != 2 {
		t.Fatalf("a repeat call added a series: %d", got)
	}
}

func TestStartAndStartIn(t *testing.T) {
	rec := telemetrytest.Spans(t)
	tel := observability.NewTelemetry("mongodb")
	tel.SetConnection("main", "", 0)

	ctx, parent := telemetrytest.Parent(t.Context())
	_, a := tel.Start(ctx, "find", "users")
	a.End(nil)
	_, b := tel.StartIn(ctx, "other", "aggregate", "")
	b.SetBatchSize(1) // ignored under 2
	b.End(nil)
	parent.End()

	spans := telemetrytest.Ended(rec)
	if spans[0].Name() != "find users" || spans[1].Name() != "aggregate" {
		t.Fatalf("names = %q, %q", spans[0].Name(), spans[1].Name())
	}
	if v, _ := telemetrytest.Attr(spans[1], "db.namespace"); v != "other" {
		t.Errorf("StartIn namespace = %q, want other", v)
	}
	if telemetrytest.HasAttr(spans[0], "server.address") {
		t.Error("server.address set with no address")
	}
	if telemetrytest.HasAttr(spans[1], "db.operation.batch.size") {
		t.Error("batch size under 2 was recorded")
	}
}

func TestUnknownStatementNamesSpanBySystem(t *testing.T) {
	rec := telemetrytest.Spans(t)
	tel := newTelemetry()

	ctx, parent := telemetrytest.Parent(t.Context())
	_, op := tel.StartSQL(ctx, `FROBNICATE`)
	op.End(nil)
	parent.End()

	s := telemetrytest.Ended(rec)[0]
	if s.Name() != "postgresql" {
		t.Errorf("name = %q, want the system name", s.Name())
	}
	if telemetrytest.HasAttr(s, "db.operation.name") || telemetrytest.HasAttr(s, "db.query.summary") {
		t.Error("an unrecognised statement got an operation or summary")
	}
}

func TestNilSafety(t *testing.T) {
	var tel *observability.Telemetry
	ctx, op := tel.StartSQL(t.Context(), `SELECT 1`)
	if ctx != t.Context() || op != nil {
		t.Fatal("a nil Telemetry should hand back the context and a nil Op")
	}
	op.End(errors.New("x"))
	op.SetBatchSize(3)
	tel.SetMetrics(nil)
	tel.SetConnection("a", "b", 1)

	m := telemetrytest.NewMetrics()
	live := newTelemetry()
	live.SetMetrics(m)
	_, o := live.StartSQL(t.Context(), `SELECT 1`)
	o.End(nil)
	o.End(errors.New("second end is ignored"))
	if s := m.Series(observability.OperationDurationMetric); len(s) != 1 || s[0].Count != 1 {
		t.Fatalf("End twice recorded twice: %+v", s)
	}

	live.SetMetrics(nil)
	_, o = live.StartSQL(t.Context(), `SELECT 1`)
	o.End(nil)
	if s := m.Series(observability.OperationDurationMetric); s[0].Count != 1 {
		t.Fatal("recorded after SetMetrics(nil)")
	}
}

func TestConnectionFromURL(t *testing.T) {
	tests := []struct {
		dsn      string
		ns, host string
		port     int
	}{
		{"clickhouse://user:secret@ch.internal:9000/analytics?dial_timeout=1s", "analytics", "ch.internal", 9000},
		{"clickhouse://ch.internal:9000?database=events", "events", "ch.internal", 9000},
		{"libsql://grove-demo.turso.io?authToken=abc", "", "grove-demo.turso.io", 0},
		{"file:local.db", "", "", 0},
		{"/tmp/local.db", "", "", 0},
		{"::not a url", "", "", 0},
	}
	for _, tc := range tests {
		ns, host, port := observability.ConnectionFromURL(tc.dsn)
		if ns != tc.ns || host != tc.host || port != tc.port {
			t.Errorf("ConnectionFromURL(%q) = (%q, %q, %d), want (%q, %q, %d)", tc.dsn, ns, host, port, tc.ns, tc.host, tc.port)
		}
	}
}

func TestSetCollection(t *testing.T) {
	rec := telemetrytest.Spans(t)
	m := telemetrytest.NewMetrics()
	tel := observability.NewTelemetry("elasticsearch")
	tel.SetMetrics(m)

	ctx, parent := telemetrytest.Parent(t.Context())
	_, op := tel.Start(ctx, "search", "")
	op.SetCollection("orders")
	op.End(nil)
	op.SetCollection("too-late")
	parent.End()

	s := telemetrytest.Ended(rec)[0]
	if s.Name() != "search orders" {
		t.Errorf("name = %q", s.Name())
	}
	if v, _ := telemetrytest.Attr(s, "db.query.summary"); v != "search orders" {
		t.Errorf("summary = %q", v)
	}
	if got := m.Series(observability.OperationDurationMetric)[0].Labels["db.collection.name"]; got != "orders" {
		t.Errorf("metric collection = %q", got)
	}
}
