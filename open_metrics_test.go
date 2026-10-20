package grove_test

import (
	"reflect"
	"strings"
	"testing"

	gometrics "github.com/xraph/go-utils/metrics"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

// instrumentedDriver is shaped like every grove driver: it owns a Telemetry
// and forwards SetMetrics to it, knowing nothing about collection labels.
type instrumentedDriver struct {
	plainDriver
	tel *observability.Telemetry
	got gometrics.Metrics
}

func newInstrumentedDriver() *instrumentedDriver {
	return &instrumentedDriver{tel: observability.NewTelemetry("postgresql")}
}

func (d *instrumentedDriver) SetMetrics(m gometrics.Metrics) {
	d.got = m
	d.tel.SetMetrics(m)
}

func (d *instrumentedDriver) query(t *testing.T, queries ...string) {
	t.Helper()
	for _, q := range queries {
		_, op := d.tel.StartSQL(t.Context(), q)
		op.End(nil)
	}
}

func collectionLabels(m *telemetrytest.Metrics) map[string]uint64 {
	out := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		out[s.Labels["db.collection.name"]] += s.Count
	}
	return out
}

// With no label option the driver gets the sink you passed, untouched, so a
// decorator you wrap around it sees every Histogram call, and the defaults
// still keep the label bounded.
func TestOpen_DefaultCollectionLabels(t *testing.T) {
	m := telemetrytest.NewMetrics()
	drv := newInstrumentedDriver()
	if _, err := grove.Open(drv, grove.WithMetrics(m)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if drv.got != gometrics.Metrics(m) {
		t.Fatalf("driver got %T, want the sink passed to WithMetrics", drv.got)
	}

	drv.query(t,
		`SELECT * FROM tenant_1.orders`,
		`SELECT * FROM "tenant_2"."orders"`,
		`SELECT * FROM tenant_3.orders`,
	)
	if got := collectionLabels(m); !reflect.DeepEqual(got, map[string]uint64{"orders": 3}) {
		t.Errorf("collections = %v, want orders for every tenant", got)
	}
}

func TestOpen_CollectionLabelOptionsReachTheDriver(t *testing.T) {
	m := telemetrytest.NewMetrics()
	drv := newInstrumentedDriver()
	_, err := grove.Open(drv,
		grove.WithMetrics(m),
		grove.WithCollectionLabel(func(collection string) string {
			if strings.HasPrefix(collection, "entity_") {
				return "entity"
			}
			return collection
		}),
		grove.WithCollectionLabelLimit(2),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	drv.query(t,
		`SELECT * FROM entity_8f3a`,
		`SELECT * FROM entity_77c1`,
		`SELECT * FROM public.users`,
		`SELECT * FROM jobs`, // past the limit of 2
	)
	want := map[string]uint64{"entity": 2, "public.users": 1, observability.OtherCollection: 1}
	if got := collectionLabels(m); !reflect.DeepEqual(got, want) {
		t.Errorf("collections = %v, want %v", got, want)
	}
}

func TestOpen_CollectionLabelOptionsWithoutMetrics(t *testing.T) {
	drv := newInstrumentedDriver()
	_, err := grove.Open(drv,
		grove.WithCollectionLabel(func(string) string { return "x" }),
		grove.WithCollectionLabelLimit(5),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if drv.got != nil {
		t.Fatalf("SetMetrics was called with %T and no WithMetrics", drv.got)
	}
}

func TestWithCollectionLabelLimitIgnoresNonPositive(t *testing.T) {
	m := telemetrytest.NewMetrics()
	drv := newInstrumentedDriver()
	if _, err := grove.Open(drv, grove.WithMetrics(m), grove.WithCollectionLabelLimit(0), grove.WithCollectionLabelLimit(-3)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if drv.got != gometrics.Metrics(m) {
		t.Fatalf("a limit of zero or less changed the sink: driver got %T", drv.got)
	}
}
