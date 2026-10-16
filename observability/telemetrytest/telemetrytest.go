// Package telemetrytest records the spans and metrics grove's drivers emit, so
// a driver test can assert on them without a Forge app.
package telemetrytest

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	gometrics "github.com/xraph/go-utils/metrics"
)

// Spans installs an in-memory tracer provider as the OTel global for the rest
// of the test and returns its recorder. The previous global comes back in
// t.Cleanup. Tests that call it must not run in parallel with each other.
func Spans(t testing.TB) *tracetest.SpanRecorder {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background()) //nolint:errcheck // nothing to flush to
	})

	return rec
}

// Parent starts a span to stand in for the request or job a query runs under.
// Grove only starts a span when the context already has one.
func Parent(ctx context.Context) (context.Context, trace.Span) {
	return otel.Tracer("telemetrytest").Start(ctx, "parent")
}

// Ended returns the ended spans the recorder saw from grove's tracer, oldest
// first, skipping the test's own parent spans.
func Ended(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.InstrumentationScope().Name == "telemetrytest" {
			continue
		}
		out = append(out, s)
	}
	return out
}

// Attr returns the value of key on span as a string, and whether it was set.
func Attr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}

// AttrMap returns every attribute on span as strings.
func AttrMap(span sdktrace.ReadOnlySpan) map[string]string {
	m := make(map[string]string, len(span.Attributes()))
	for _, kv := range span.Attributes() {
		m[string(kv.Key)] = kv.Value.String()
	}
	return m
}

// HasAttr reports whether span carries key at all.
func HasAttr(span sdktrace.ReadOnlySpan, key attribute.Key) bool {
	for _, kv := range span.Attributes() {
		if kv.Key == key {
			return true
		}
	}
	return false
}

// Series is one label set of a recorded histogram.
type Series struct {
	Name    string
	Unit    string
	Buckets []float64
	Labels  map[string]string
	Count   uint64
}

// Metrics is a go-utils metrics sink that keeps every histogram series in
// memory. Only Histogram is implemented; anything else panics, because grove
// never calls it.
type Metrics struct {
	gometrics.Metrics

	mu     sync.Mutex
	series map[string]*recorded
}

type recorded struct {
	opts gometrics.MetricOptions
	name string
	hist gometrics.Histogram
}

// NewMetrics returns an empty sink.
func NewMetrics() *Metrics {
	return &Metrics{series: map[string]*recorded{}}
}

// Histogram returns the series for name and the labels in opts, creating it
// the first time.
func (m *Metrics) Histogram(name string, opts ...gometrics.MetricOption) gometrics.Histogram {
	var o gometrics.MetricOptions
	for _, opt := range opts {
		opt(&o)
	}

	key := name + "{" + labelKey(o.Labels) + "}"

	m.mu.Lock()
	defer m.mu.Unlock()

	if r, ok := m.series[key]; ok {
		return r.hist
	}
	r := &recorded{opts: o, name: name, hist: gometrics.NewHistogram(name, opts...)}
	m.series[key] = r
	return r.hist
}

// Series returns every label set recorded under name, sorted by labels.
func (m *Metrics) Series(name string) []Series {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []Series
	for _, r := range m.series {
		if r.name != name {
			continue
		}
		out = append(out, Series{
			Name:    r.name,
			Unit:    r.opts.Unit,
			Buckets: r.opts.Buckets,
			Labels:  r.opts.Labels,
			Count:   r.hist.Count(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return out
}

func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}
