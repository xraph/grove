package esdriver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/observability/telemetrytest"
)

// fakeES answers the handful of calls the test makes: info at the root, a
// search on "orders", a 404 for anything under "missing".
func fakeES(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/":
			_, _ = w.Write([]byte(`{"version":{"number":"8.17.0","build_flavor":"default"},"tagline":"You Know, for Search"}`))
		case strings.HasPrefix(r.URL.Path, "/missing"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"},"status":404}`))
		case strings.HasSuffix(r.URL.Path, "/_search"):
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":0},"hits":[]}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"found":false}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestTelemetry(t *testing.T) {
	rec := telemetrytest.Spans(t)
	srv := fakeES(t)

	db := New()
	if err := db.Open(t.Context(), srv.URL); err != nil {
		t.Fatalf("open: %v", err)
	}
	m := telemetrytest.NewMetrics()
	if _, err := grove.Open(db, grove.WithMetrics(m)); err != nil {
		t.Fatal(err)
	}

	ctx, parent := telemetrytest.Parent(t.Context())
	c := db.client

	res, err := c.Search(c.Search.WithContext(ctx), c.Search.WithIndex("orders"),
		c.Search.WithBody(strings.NewReader(`{"query":{"term":{"email":"alice@example.com"}}}`)))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()

	res, err = c.Search(c.Search.WithContext(ctx), c.Search.WithIndex("missing"))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()

	res, err = c.Get("orders", "404", c.Get.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()

	res, err = c.Search(c.Search.WithContext(ctx), c.Search.WithIndex("logs-*"))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	parent.End()

	spans := telemetrytest.Ended(rec)
	got := make([]string, 0, len(spans))
	for _, s := range spans {
		got = append(got, s.Name())
		if s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Errorf("%s is not under the caller's span", s.Name())
		}
		attrs := telemetrytest.AttrMap(s)
		if attrs["db.system.name"] != "elasticsearch" || attrs["server.address"] != "127.0.0.1" || attrs["server.port"] == "" {
			t.Errorf("%s: attributes %v", s.Name(), attrs)
		}
		for k, v := range attrs {
			if strings.Contains(v, "alice") {
				t.Errorf("%s: %s carries the request body", s.Name(), k)
			}
		}
	}
	if strings.Join(got, ",") != "search orders,search missing,get orders,search" {
		t.Fatalf("spans = %v", got)
	}

	if s := spans[1]; s.Status().Code != codes.Error {
		t.Errorf("search on a missing index should fail: %v", s.Status())
	} else if v, _ := telemetrytest.Attr(s, "error.type"); v != "404" {
		t.Errorf("error.type = %q, want 404", v)
	}
	if s := spans[2]; s.Status().Code == codes.Error {
		t.Error("a get of a missing document was marked failed")
	}
	if telemetrytest.HasAttr(spans[3], "db.collection.name") {
		t.Error("a wildcard index pattern became a collection")
	}

	ops := map[string]uint64{}
	for _, s := range m.Series(observability.OperationDurationMetric) {
		key := strings.TrimSpace(s.Labels["db.operation.name"] + " " + s.Labels["db.collection.name"] + " " + s.Labels["error.type"])
		ops[key] += s.Count
	}
	// Open's connectivity check ("info") runs before grove.Open hands the
	// driver its metrics, so it isn't counted.
	want := map[string]uint64{"search orders": 1, "search missing 404": 1, "get orders": 1, "search": 1}
	if len(ops) != len(want) {
		t.Errorf("series = %v, want %v", ops, want)
	}
	for k, v := range want {
		if ops[k] != v {
			t.Errorf("series %q = %d, want %d (all: %v)", k, ops[k], v, ops)
		}
	}
}
