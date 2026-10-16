package observability

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	gometrics "github.com/xraph/go-utils/metrics"
)

// InstrumentationName is the tracer name on every span grove starts.
const InstrumentationName = "github.com/xraph/grove"

// OperationDurationMetric is the OTel database client histogram every driver
// records. A Forge app scrapes it as db_client_operation_duration_seconds.
const OperationDurationMetric = "db.client.operation.duration"

// OperationDurationBuckets are the bounds the OTel database conventions advise
// for OperationDurationMetric, in seconds.
var OperationDurationBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10}

// The metric's label keys. Every series carries all of them, empty when they
// don't apply, so the family keeps one label set.
const (
	labelSystem     = "db.system.name"
	labelNamespace  = "db.namespace"
	labelOperation  = "db.operation.name"
	labelCollection = "db.collection.name"
	labelStatusCode = "db.response.status_code"
	labelErrorType  = "error.type"
)

// maxCachedSeries bounds the per-Telemetry cache of histogram handles. Past
// it, a new label set still records; it just goes through the registry each
// time. The registry's own series cap is what bounds the exported series.
const maxCachedSeries = 4096

// Telemetry traces and times the calls one driver makes, following the
// OpenTelemetry database client conventions. Each driver builds one in New
// and calls Start (or StartSQL) around every call that reaches the database.
//
// Spans go to the global tracer provider, which forge.New installs. A span is
// only started when the context already carries one, so a background poll
// that finds nothing doesn't produce a root trace per tick: give the unit of
// work its own span and the queries under it nest inside. The duration
// histogram is recorded either way, once SetMetrics has been called.
//
// A nil *Telemetry is valid and does nothing.
type Telemetry struct {
	system    string
	namespace string
	address   string
	port      int

	statusCode func(error) string
	benign     func(error) bool

	sink atomic.Pointer[metricSink]
}

// Option configures a Telemetry.
type Option func(*Telemetry)

// WithStatusCode sets how the driver reads db.response.status_code out of an
// error: a Postgres SQLSTATE, a MySQL error number, a MongoDB error code. The
// function returns "" when the error carries none.
func WithStatusCode(fn func(error) string) Option {
	return func(t *Telemetry) { t.statusCode = fn }
}

// WithBenign marks errors that are normal outcomes rather than failures, such
// as a document not found. They end the span without an error status and
// record no error.type. sql.ErrNoRows is always benign.
func WithBenign(fn func(error) bool) Option {
	return func(t *Telemetry) { t.benign = fn }
}

// NewTelemetry returns the telemetry for one driver instance. system is the
// db.system.name value: postgresql, mysql, sqlite, mongodb, clickhouse,
// elasticsearch and so on.
func NewTelemetry(system string, opts ...Option) *Telemetry {
	t := &Telemetry{system: system}
	for _, o := range opts {
		o(t)
	}
	return t
}

// SetConnection records where the driver is connected: the database name
// (db.namespace) and the server's address and port. Call it from Open, before
// the first query. Empty values and a zero port are left off.
func (t *Telemetry) SetConnection(namespace, address string, port int) {
	if t == nil {
		return
	}
	t.namespace = namespace
	t.address = address
	t.port = port
}

// Namespace returns the db.namespace set by SetConnection.
func (t *Telemetry) Namespace() string {
	if t == nil {
		return ""
	}
	return t.namespace
}

// SetMetrics starts recording OperationDurationMetric on m. In a Forge app,
// pass app.Metrics(); grove's Forge extension does this for you. A nil m
// stops recording.
func (t *Telemetry) SetMetrics(m gometrics.Metrics) {
	if t == nil {
		return
	}
	if m == nil {
		t.sink.Store(nil)
		return
	}
	t.sink.Store(&metricSink{m: m})
}

// StartSQL starts an operation for a SQL statement. The operation, table and
// span name come from SummarizeSQL, so they never hold a value from query.
func (t *Telemetry) StartSQL(ctx context.Context, query string) (context.Context, *Op) {
	if t.idle(ctx) {
		return ctx, nil
	}
	s := SummarizeSQL(query)
	return t.start(ctx, t.namespace, s.Operation, s.Collection, s.Summary)
}

// Start starts an operation the driver describes itself, for databases that
// aren't SQL: operation is the command (find, insert, search) and collection
// the collection or index, "" when there isn't one.
func (t *Telemetry) Start(ctx context.Context, operation, collection string) (context.Context, *Op) {
	if t.idle(ctx) {
		return ctx, nil
	}
	return t.start(ctx, t.namespace, operation, collection, summaryOf(operation, collection))
}

// StartIn is Start for a driver whose namespace changes per call, like a
// MongoDB client that talks to several databases.
func (t *Telemetry) StartIn(ctx context.Context, namespace, operation, collection string) (context.Context, *Op) {
	if t.idle(ctx) {
		return ctx, nil
	}
	return t.start(ctx, namespace, operation, collection, summaryOf(operation, collection))
}

// idle reports whether a call on ctx has nothing to record: no parent span to
// nest under and no metrics sink. The Start methods then return a nil *Op,
// which every Op method accepts, so a driver with telemetry switched off pays
// for a pointer load and nothing else.
func (t *Telemetry) idle(ctx context.Context) bool {
	return t == nil || (t.sink.Load() == nil && !trace.SpanContextFromContext(ctx).IsValid())
}

func summaryOf(operation, collection string) string {
	if collection == "" {
		return operation
	}
	return operation + " " + collection
}

func (t *Telemetry) start(ctx context.Context, namespace, operation, collection, summary string) (context.Context, *Op) {
	op := &Op{
		t:          t,
		start:      time.Now(),
		namespace:  namespace,
		operation:  operation,
		collection: collection,
	}

	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, op
	}

	name := summary
	if name == "" {
		name = t.system
	}

	ctx, op.span = otel.Tracer(InstrumentationName).Start(ctx, name, //nolint:spancheck // Op.End ends it
		trace.WithSpanKind(trace.SpanKindClient))
	if op.span.IsRecording() {
		op.span.SetAttributes(t.spanAttributes(namespace, operation, collection, summary)...)
	}

	return ctx, op
}

func (t *Telemetry) spanAttributes(namespace, operation, collection, summary string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 7)
	attrs = append(attrs, semconv.DBSystemNameKey.String(t.system))
	if namespace != "" {
		attrs = append(attrs, semconv.DBNamespace(namespace))
	}
	if operation != "" {
		attrs = append(attrs, semconv.DBOperationName(operation))
	}
	if collection != "" {
		attrs = append(attrs, semconv.DBCollectionName(collection))
	}
	if summary != "" {
		attrs = append(attrs, semconv.DBQuerySummary(summary))
	}
	if t.address != "" {
		attrs = append(attrs, semconv.ServerAddress(t.address))
		if t.port > 0 {
			attrs = append(attrs, semconv.ServerPort(t.port))
		}
	}
	return attrs
}

func (t *Telemetry) isBenign(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	return t.benign != nil && t.benign(err)
}

// Op is one call to the database, from Start to End.
type Op struct {
	t          *Telemetry
	span       trace.Span
	start      time.Time
	namespace  string
	operation  string
	collection string
	ended      bool
}

// Context returns ctx with the operation's span on it, for code that has to
// hand a context to the driver after Start returned. A nil *Op returns ctx.
func (o *Op) Context(ctx context.Context) context.Context {
	if o == nil || o.span == nil {
		return ctx
	}
	return trace.ContextWithSpan(ctx, o.span)
}

// SetCollection names the collection after Start, for a client that reports
// it late, like the Elasticsearch client reporting the index once the request
// path is built. It renames the span and sets the metric label to match.
func (o *Op) SetCollection(collection string) {
	if o == nil || o.ended || collection == "" {
		return
	}
	o.collection = collection
	if o.span == nil || !o.span.IsRecording() {
		return
	}
	summary := summaryOf(o.operation, collection)
	o.span.SetName(summary)
	o.span.SetAttributes(semconv.DBCollectionName(collection), semconv.DBQuerySummary(summary))
}

// SetBatchSize records db.operation.batch.size for a call that carries more
// than one operation, like a bulk write. Sizes under 2 are ignored, as the
// conventions ask.
func (o *Op) SetBatchSize(n int) {
	if o == nil || o.span == nil || n < 2 || !o.span.IsRecording() {
		return
	}
	o.span.SetAttributes(semconv.DBOperationBatchSize(n))
}

// End ends the span and records the duration. A nil err, or one the driver
// marked benign, is a success. Anything else sets error.type,
// db.response.status_code when the driver can read one, an exception event
// carrying the error's Go type, and an error status on the span. The error's
// text never goes on the span, because a driver's message can quote the
// values it was given (a duplicate key, a bad integer). End is safe on a nil
// *Op and does nothing the second time.
func (o *Op) End(err error) {
	if o == nil || o.ended {
		return
	}
	o.ended = true
	elapsed := time.Since(o.start).Seconds()
	t := o.t

	var status, errType string
	if err != nil && !t.isBenign(err) {
		if t.statusCode != nil {
			status = t.statusCode(err)
		}
		errType = errorType(err, status)
	}

	if o.span != nil {
		if errType != "" && o.span.IsRecording() {
			o.span.SetAttributes(semconv.ErrorTypeKey.String(errType))
			if status != "" {
				o.span.SetAttributes(semconv.DBResponseStatusCode(status))
			}
			o.span.AddEvent(semconv.ExceptionEventName, trace.WithAttributes(
				semconv.ExceptionType(errorType(err, ""))))
			o.span.SetStatus(codes.Error, errType)
		}
		o.span.End()
	}

	if s := t.sink.Load(); s != nil {
		s.observe(elapsed, seriesKey{
			system:     t.system,
			namespace:  o.namespace,
			operation:  o.operation,
			collection: o.collection,
			status:     status,
			errType:    errType,
		})
	}
}

// errorType names an error for error.type. The database's own status code
// wins when there is one, as the conventions ask. Otherwise a context error
// gets its package name and anything else the Go type of the innermost error
// in its chain, the way forge's messaging package does it.
func errorType(err error, status string) string {
	if status != "" {
		return status
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	}

	for next := errors.Unwrap(err); next != nil; next = errors.Unwrap(err) {
		err = next
	}
	return fmt.Sprintf("%T", err)
}

// StatusCodeInt formats an integer error code for WithStatusCode functions.
func StatusCodeInt(code int64) string {
	return strconv.FormatInt(code, 10)
}

type seriesKey struct {
	system, namespace, operation, collection, status, errType string
}

type metricSink struct {
	m     gometrics.Metrics
	cache sync.Map // seriesKey -> gometrics.Histogram
	n     atomic.Int32
}

func (s *metricSink) observe(v float64, k seriesKey) {
	if h, ok := s.cache.Load(k); ok {
		if hist, ok := h.(gometrics.Histogram); ok {
			hist.Observe(v)
			return
		}
	}

	hist := s.m.Histogram(OperationDurationMetric,
		gometrics.WithUnit("s"),
		gometrics.WithBuckets(OperationDurationBuckets...),
		gometrics.WithDescription("Duration of database client operations."),
		gometrics.WithLabels(map[string]string{
			labelSystem:     k.system,
			labelNamespace:  k.namespace,
			labelOperation:  k.operation,
			labelCollection: k.collection,
			labelStatusCode: k.status,
			labelErrorType:  k.errType,
		}),
	)
	if claim(&s.n, maxCachedSeries) {
		s.cache.Store(k, hist)
	}
	hist.Observe(v)
}

// claim takes one of limit slots counted on n and reports whether it got one.
// Once the slots are gone n stops moving, so a stream of misses can't wrap it
// negative and let a bounded cache grow again.
func claim(n *atomic.Int32, limit int32) bool {
	for {
		cur := n.Load()
		if cur >= limit {
			return false
		}
		if n.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// ConnectionFromURL reads the database name, host and port out of a URL-form
// DSN (clickhouse://host:9000/db, libsql://name.turso.io) for SetConnection.
// The database name comes from the path, or from a database query parameter
// when the path is empty. Credentials are never read. A DSN that isn't a URL
// with a host, such as a local file, reports nothing.
func ConnectionFromURL(dsn string) (namespace, address string, port int) {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "", "", 0
	}

	namespace = strings.Trim(u.Path, "/")
	if namespace == "" {
		namespace = u.Query().Get("database")
	}

	if p := u.Port(); p != "" {
		port, _ = strconv.Atoi(p) //nolint:errcheck // a bad port is reported as none
	}
	return namespace, u.Hostname(), port
}
