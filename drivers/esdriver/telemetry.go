package esdriver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	gometrics "github.com/xraph/go-utils/metrics"

	"github.com/xraph/grove/observability"
)

// StatusError is the error a call ends with when Elasticsearch answers with
// a 4xx or 5xx status and the client itself returned no error. It only shows
// up in telemetry: callers still get the response and read its status.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string {
	return "esdriver: status " + strconv.Itoa(e.Code)
}

// newTelemetry builds the db.client telemetry for one ElasticDB. A failed call
// reports its HTTP status as db.response.status_code and error.type, as the
// OTel Elasticsearch conventions ask.
func newTelemetry() *observability.Telemetry {
	return observability.NewTelemetry("elasticsearch", observability.WithStatusCode(httpStatus))
}

func httpStatus(err error) string {
	var se *StatusError
	if errors.As(err, &se) {
		return strconv.Itoa(se.Code)
	}
	return ""
}

// SetMetrics records the db.client.operation.duration histogram on m.
// grove.Open calls it when you pass grove.WithMetrics.
func (db *ElasticDB) SetMetrics(m gometrics.Metrics) {
	db.tel.SetMetrics(m)
}

// instrumentation plugs grove's telemetry into the Elasticsearch client, which
// calls it around every API request: Start with the endpoint name ("search",
// "index", "bulk"), RecordPathPart with the index, AfterResponse with the
// status, Close when the call returns. Request bodies are never captured.
type instrumentation struct {
	tel *observability.Telemetry
}

var _ elastictransport.Instrumentation = instrumentation{}

type callKey struct{}

type call struct {
	op       *observability.Op
	endpoint string
	err      error
	status   int
}

func callFrom(ctx context.Context) *call {
	c, _ := ctx.Value(callKey{}).(*call) //nolint:errcheck // absent means not ours
	return c
}

func (i instrumentation) Start(ctx context.Context, name string) context.Context {
	ctx, op := i.tel.Start(ctx, name, "")
	return context.WithValue(ctx, callKey{}, &call{op: op, endpoint: name})
}

func (i instrumentation) Close(ctx context.Context) {
	c := callFrom(ctx)
	if c == nil {
		return
	}
	err := c.err
	if err == nil && c.status >= 400 && (c.status != http.StatusNotFound || !notFoundIsAnswer(c.endpoint)) {
		err = &StatusError{Code: c.status}
	}
	c.op.End(err)
}

func (i instrumentation) RecordError(ctx context.Context, err error) {
	if c := callFrom(ctx); c != nil && c.err == nil {
		c.err = err
	}
}

// RecordPathPart names the collection after the index, but only for a single
// concrete index. A list or a wildcard pattern is several collections, and
// would mint a label value per pattern.
func (i instrumentation) RecordPathPart(ctx context.Context, pathPart, value string) {
	if pathPart != "index" || value == "" || strings.ContainsAny(value, ",*") {
		return
	}
	if c := callFrom(ctx); c != nil {
		c.op.SetCollection(value)
	}
}

func (i instrumentation) RecordRequestBody(context.Context, string, io.Reader) io.ReadCloser {
	return nil
}

func (i instrumentation) BeforeRequest(*http.Request, string) {}

func (i instrumentation) AfterRequest(*http.Request, string, string) {}

func (i instrumentation) AfterResponse(ctx context.Context, res *http.Response) {
	if c := callFrom(ctx); c != nil && res != nil {
		c.status = res.StatusCode
	}
}

// notFoundIsAnswer reports whether a 404 from endpoint is an answer rather
// than a failure: an exists check that says no, or a get of a document that
// isn't there.
func notFoundIsAnswer(endpoint string) bool {
	return strings.Contains(endpoint, "exists") || endpoint == "get" || endpoint == "get_source"
}

// serverOf reads the first address for server.address and server.port.
func serverOf(addrs []string) (address string, port int) {
	if len(addrs) == 0 {
		return "", 0
	}
	u, err := url.Parse(addrs[0])
	if err != nil || u.Host == "" {
		return "", 0
	}
	port, _ = strconv.Atoi(u.Port()) //nolint:errcheck // a missing port is reported as none
	return u.Hostname(), port
}
