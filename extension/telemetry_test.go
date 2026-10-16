package extension

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/forge"
	gometrics "github.com/xraph/go-utils/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/xraph/grove/grovetest"
	"github.com/xraph/grove/observability"
)

// instrumentedDriver stands in for a real driver: it records through grove's
// telemetry the way pgdriver and the rest do, without a database.
type instrumentedDriver struct {
	*grovetest.MockDriver
	tel *observability.Telemetry
}

func newInstrumentedDriver() *instrumentedDriver {
	return &instrumentedDriver{MockDriver: grovetest.NewMockDriver(), tel: observability.NewTelemetry("sqlite")}
}

func (d *instrumentedDriver) SetMetrics(m gometrics.Metrics) { d.tel.SetMetrics(m) }

func (d *instrumentedDriver) run(ctx context.Context, query string, err error) {
	_, op := d.tel.StartSQL(ctx, query)
	op.End(err)
}

func scrape(t *testing.T, app forge.App) string {
	t.Helper()
	p, ok := app.Metrics().(interface{ PrometheusHandler() http.Handler })
	if !ok {
		t.Fatalf("app metrics %T have no Prometheus handler", app.Metrics())
	}
	rr := httptest.NewRecorder()
	p.PrometheusHandler().ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_/metrics", nil))
	body, _ := io.ReadAll(rr.Body)
	return string(body)
}

func dbSeries(body string, suffix string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "db_client_operation_duration_seconds"+suffix+"{") {
			out = append(out, line)
		}
	}
	return out
}

func TestExtensionWiresTelemetryIntoForge(t *testing.T) {
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	app := forge.New(forge.WithAppName("grove-telemetry-test"), forge.WithAppLogger(forge.NewNoopLogger()))
	rec := tracetest.NewSpanRecorder()
	if err := forge.RegisterSpanProcessor(app, rec); err != nil {
		t.Fatalf("RegisterSpanProcessor: %v", err)
	}

	drv := newInstrumentedDriver()
	ext := New(WithDriver(drv))
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, span := forge.StartSpan(t.Context(), "process job")
	drv.run(ctx, `SELECT "id" FROM "jobs" WHERE "state" = $1`, nil)
	drv.run(ctx, `UPDATE "jobs" SET "state" = $1`, errors.New("disk I/O error"))
	span.End()

	// Spans go to forge's tracer provider, under the caller's span.
	var names []string
	for _, s := range rec.Ended() {
		if s.InstrumentationScope().Name != observability.InstrumentationName {
			continue
		}
		names = append(names, s.Name())
		if s.Parent().SpanID() != span.SpanContext().SpanID() {
			t.Errorf("%s is not a child of the job span", s.Name())
		}
		if s.Name() == "UPDATE jobs" && s.Status().Code != codes.Error {
			t.Errorf("failed update not marked: %v", s.Status())
		}
	}
	if strings.Join(names, ",") != "SELECT jobs,UPDATE jobs" {
		t.Fatalf("grove spans = %v", names)
	}

	// The histogram is on the scrape under its semconv name: no forge_
	// prefix, a _seconds suffix, dotted labels turned into underscores.
	body := scrape(t, app)
	if strings.Contains(body, "forge_db_client") {
		t.Fatal("the semconv histogram was prefixed with the namespace")
	}
	counts := dbSeries(body, "_count")
	if len(counts) != 2 {
		t.Fatalf("want 2 label sets on the scrape, got %d:\n%s", len(counts), strings.Join(counts, "\n"))
	}
	var sawSelect, sawFailure bool
	for _, line := range counts {
		if strings.Contains(line, `db_operation_name="SELECT"`) && strings.Contains(line, `db_collection_name="jobs"`) &&
			strings.Contains(line, `db_system_name="sqlite"`) && strings.Contains(line, `error_type=""`) {
			sawSelect = true
		}
		if strings.Contains(line, `db_operation_name="UPDATE"`) && strings.Contains(line, `error_type="*errors.errorString"`) {
			sawFailure = true
		}
	}
	if !sawSelect || !sawFailure {
		t.Fatalf("missing expected series:\n%s", strings.Join(counts, "\n"))
	}
	if len(dbSeries(body, "_bucket")) != 2*(len(observability.OperationDurationBuckets)+1) {
		t.Errorf("bucket lines = %d, want %d per series", len(dbSeries(body, "_bucket")), len(observability.OperationDurationBuckets)+1)
	}

	// The same call again adds an observation, not a series.
	drv.run(ctx, `SELECT "id" FROM "jobs" WHERE "state" = $1`, nil)
	if got := len(dbSeries(scrape(t, app), "_count")); got != 2 {
		t.Fatalf("a repeat call added a series: %d", got)
	}
}
