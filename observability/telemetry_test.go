package observability_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
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

// collections returns the db.collection.name values recorded on m, with the
// number of calls behind each.
func collections(m *telemetrytest.Metrics) map[string]uint64 {
	out := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		out[s.Labels["db.collection.name"]] += s.Count
	}
	return out
}

// Per-tenant schemas would add a label value per tenant. The metric drops the
// qualifier, so more tenants than the limit share one, while the span keeps
// the table as written. The tenant count stays small because every statement
// takes a slot in the process-wide summary cache, which other tests rely on.
func TestCollectionLabelDropsSchema(t *testing.T) {
	rec := telemetrytest.Spans(t)
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()
	tel.SetMetrics(m)

	ctx, parent := telemetrytest.Parent(t.Context())
	const tenants = observability.DefaultCollectionLabelLimit + 44
	for i := range tenants {
		_, op := tel.StartSQL(ctx, fmt.Sprintf(`SELECT "id" FROM "tenant_%d"."fabriq_crdt_docs" WHERE "id" = $1`, i))
		op.End(nil)
		_, op = tel.StartSQL(ctx, fmt.Sprintf(`UPDATE tenant_%d.fabriq_crdt_docs SET v = $1`, i))
		op.End(nil)
	}
	parent.End()

	series := m.Series(observability.OperationDurationMetric)
	if len(series) != 2 {
		t.Fatalf("got %d series for %d tenants, want 2 (SELECT and UPDATE): %+v", len(series), tenants, series)
	}
	for _, s := range series {
		if got := s.Labels["db.collection.name"]; got != "fabriq_crdt_docs" {
			t.Errorf("metric collection = %q, want fabriq_crdt_docs", got)
		}
		if s.Count != tenants {
			t.Errorf("%s count = %d, want %d", s.Labels["db.operation.name"], s.Count, tenants)
		}
	}

	s := telemetrytest.Ended(rec)[2*42]
	if s.Name() != "SELECT tenant_42.fabriq_crdt_docs" {
		t.Errorf("span name = %q, want the table as written", s.Name())
	}
	want := map[string]string{
		"db.system.name":     "postgresql",
		"db.namespace":       "app",
		"db.operation.name":  "SELECT",
		"db.collection.name": "tenant_42.fabriq_crdt_docs",
		"db.query.summary":   "SELECT tenant_42.fabriq_crdt_docs",
		"server.address":     "db.internal",
		"server.port":        "5432",
	}
	if got := telemetrytest.AttrMap(s); !reflect.DeepEqual(got, want) {
		t.Errorf("span attributes = %v\nwant %v", got, want)
	}
}

// A dot is part of a MongoDB collection or Elasticsearch index name, and the
// database is already db.namespace, so names that don't come from SQL are
// used as reported.
func TestCollectionLabelKeepsNonSQLNames(t *testing.T) {
	m := telemetrytest.NewMetrics()
	tel := observability.NewTelemetry("mongodb")
	tel.SetMetrics(m)
	ctx := t.Context()

	_, op := tel.Start(ctx, "find", "fs.files")
	op.End(nil)
	_, op = tel.StartIn(ctx, "other", "find", "system.profile")
	op.End(nil)
	_, op = tel.Start(ctx, "search", "")
	op.SetCollection("logs-2026.10.19")
	op.End(nil)

	got := collections(m)
	want := map[string]uint64{"fs.files": 1, "system.profile": 1, "logs-2026.10.19": 1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("collections = %v, want %v", got, want)
	}
}

// Past the limit a new table records as other, and the tables already seen
// keep their own label.
func TestCollectionLabelLimit(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		m := telemetrytest.NewMetrics()
		tel := newTelemetry()
		tel.SetMetrics(m)
		ctx := t.Context()

		const n = observability.DefaultCollectionLabelLimit + 50
		for i := range n {
			_, op := tel.StartSQL(ctx, fmt.Sprintf(`SELECT * FROM x_%d`, i))
			op.End(nil)
		}
		_, op := tel.StartSQL(ctx, `SELECT * FROM x_0`)
		op.End(nil)

		got := collections(m)
		if len(got) != observability.DefaultCollectionLabelLimit+1 {
			t.Fatalf("got %d label values, want %d plus other", len(got), observability.DefaultCollectionLabelLimit)
		}
		if got[observability.OtherCollection] != 50 {
			t.Errorf("other = %d, want the 50 tables past the limit", got[observability.OtherCollection])
		}
		if got["x_0"] != 2 {
			t.Errorf("x_0 = %d, want 2: a table seen before the limit keeps its label", got["x_0"])
		}
		if _, ok := got[fmt.Sprintf("x_%d", n-1)]; ok {
			t.Error("a table past the limit got its own label")
		}
	})

	t.Run("custom", func(t *testing.T) {
		m := telemetrytest.NewMetrics()
		tel := newTelemetry()
		tel.SetMetrics(observability.MetricsWithCollectionLabels(m, observability.CollectionLabels{Limit: 2}))
		ctx := t.Context()

		for _, q := range []string{
			`SELECT * FROM a`, `SELECT * FROM b`, `SELECT * FROM c`, `SELECT * FROM a`,
			`SELECT 1`, `COMMIT`,
		} {
			_, op := tel.StartSQL(ctx, q)
			op.End(nil)
		}

		want := map[string]uint64{"a": 2, "b": 1, observability.OtherCollection: 1, "": 2}
		if got := collections(m); !reflect.DeepEqual(got, want) {
			t.Errorf("collections = %v, want %v (no collection doesn't count toward the limit)", got, want)
		}
	})

	t.Run("SetMetrics starts a fresh count", func(t *testing.T) {
		tel := newTelemetry()
		first := telemetrytest.NewMetrics()
		tel.SetMetrics(observability.MetricsWithCollectionLabels(first, observability.CollectionLabels{Limit: 1}))
		_, op := tel.StartSQL(t.Context(), `SELECT * FROM a`)
		op.End(nil)

		second := telemetrytest.NewMetrics()
		tel.SetMetrics(observability.MetricsWithCollectionLabels(second, observability.CollectionLabels{Limit: 1}))
		_, op = tel.StartSQL(t.Context(), `SELECT * FROM b`)
		op.End(nil)

		if got := collections(second); !reflect.DeepEqual(got, map[string]uint64{"b": 1}) {
			t.Errorf("collections after SetMetrics = %v, want b alone", got)
		}
	})
}

func TestCollectionLabelFunc(t *testing.T) {
	rec := telemetrytest.Spans(t)
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()

	var seen []string
	tel.SetMetrics(observability.MetricsWithCollectionLabels(m, observability.CollectionLabels{
		Limit: 2,
		Label: func(collection string) string {
			seen = append(seen, collection)
			switch {
			case strings.HasPrefix(collection, "tenant_"):
				return "tenant_table"
			case strings.HasPrefix(collection, "scratch_"):
				return ""
			case strings.HasPrefix(collection, "raw_"):
				return observability.OtherCollection
			}
			return collection
		},
	}))

	ctx, parent := telemetrytest.Parent(t.Context())
	for _, q := range []string{
		`SELECT * FROM tenant_1.orders`,
		`SELECT * FROM tenant_2.orders`,
		`SELECT * FROM scratch_9f2c`,
		`SELECT * FROM raw_77`,
		`SELECT * FROM users`,
		`SELECT * FROM jobs`, // past the limit of 2
		`COMMIT`,             // no collection, so the function isn't called
	} {
		_, op := tel.StartSQL(ctx, q)
		op.End(nil)
	}
	parent.End()

	wantSeen := []string{"tenant_1.orders", "tenant_2.orders", "scratch_9f2c", "raw_77", "users", "jobs"}
	if !reflect.DeepEqual(seen, wantSeen) {
		t.Errorf("label function got %q, want the collections as written %q", seen, wantSeen)
	}

	want := map[string]uint64{"tenant_table": 2, "": 2, observability.OtherCollection: 2, "users": 1}
	if got := collections(m); !reflect.DeepEqual(got, want) {
		t.Errorf("collections = %v, want %v", got, want)
	}

	if s := telemetrytest.Ended(rec)[0]; s.Name() != "SELECT tenant_1.orders" {
		t.Errorf("span name = %q: the label function must not touch spans", s.Name())
	}
}

// grove.WithMetrics(MetricsWithCollectionLabels(m, a)) plus
// grove.WithCollectionLabelLimit wraps the sink twice. The outer limit applies
// and the inner label function, which the outer wrap leaves unset, survives.
func TestMetricsWithCollectionLabelsWrappedTwice(t *testing.T) {
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()

	inner := observability.MetricsWithCollectionLabels(m, observability.CollectionLabels{
		Label: strings.ToUpper,
		Limit: 5,
	})
	tel.SetMetrics(observability.MetricsWithCollectionLabels(inner, observability.CollectionLabels{Limit: 1}))

	ctx := context.Background()
	for _, q := range []string{`SELECT * FROM users`, `SELECT * FROM jobs`} {
		_, op := tel.StartSQL(ctx, q)
		op.End(nil)
	}

	want := map[string]uint64{"USERS": 1, observability.OtherCollection: 1}
	if got := collections(m); !reflect.DeepEqual(got, want) {
		t.Errorf("collections = %v, want %v", got, want)
	}
}

func TestMetricsWithCollectionLabelsNil(t *testing.T) {
	if m := observability.MetricsWithCollectionLabels(nil, observability.CollectionLabels{Limit: 1}); m != nil {
		t.Fatalf("a nil sink came back as %T", m)
	}
}

// Many goroutines minting new tables at once must not let more than the
// limit through, and every call must still be counted.
func TestCollectionLabelLimitConcurrent(t *testing.T) {
	const (
		workers = 16
		limit   = observability.DefaultCollectionLabelLimit
		tables  = 2 * limit
	)
	m := telemetrytest.NewMetrics()
	tel := newTelemetry()
	tel.SetMetrics(m)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			ctx := context.Background()
			for i := range tables {
				// Each worker walks the same tables from a different start,
				// so first sightings of a table race.
				n := (i + w*7) % tables
				_, op := tel.StartSQL(ctx, fmt.Sprintf(`SELECT * FROM t_%d`, n))
				op.End(nil)
			}
		})
	}
	wg.Wait()

	got := collections(m)
	var total uint64
	named := 0
	for v, c := range got {
		total += c
		if v != observability.OtherCollection {
			named++
		}
	}
	if total != workers*tables {
		t.Errorf("recorded %d calls, want %d", total, workers*tables)
	}
	if named > limit {
		t.Errorf("%d label values got through a limit of %d", named, limit)
	}
	if got[observability.OtherCollection] == 0 {
		t.Error("no call recorded as other with more tables than the limit")
	}
}
