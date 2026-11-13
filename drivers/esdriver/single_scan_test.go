package esdriver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/observability/telemetrytest"
)

type singleScanDocument struct {
	Title string `json:"title"`
}

func TestSingleModelScanCapsHitsWithoutChangingBuilder(t *testing.T) {
	bodies := make(chan map[string]any, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		bodies <- body
		if body["size"] == float64(0) {
			_, _ = w.Write([]byte(`{"hits":{"hits":[]}}`))
			return
		}
		_, _ = w.Write([]byte(`{"hits":{"hits":[{"_id":"1","_source":{"title":"first"}}]}}`))
	}))
	defer server.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), server.URL))
	defer db.Close()
	recorder := telemetrytest.Spans(t)
	ctx, parent := telemetrytest.Parent(t.Context())
	defer parent.End()
	for _, size := range []int{-1, 1000, 1, 0} {
		var doc singleScanDocument
		q := db.NewSearch(&doc).Index("docs").From(7).Sort("title", "asc").Size(size)
		err := q.Scan(ctx)
		if size == 0 {
			require.ErrorContains(t, err, "no documents found")
		} else {
			require.NoError(t, err)
			require.Equal(t, "first", doc.Title)
		}
		body := <-bodies
		want := float64(1)
		if size == 0 {
			want = 0
		}
		require.Equal(t, want, body["size"])
		require.Equal(t, float64(7), body["from"])
		require.NotEmpty(t, body["sort"])
		require.Equal(t, size, q.GetSize(), "Scan mutated the reusable builder")
		_, err = q.ScanHits(ctx)
		require.NoError(t, err)
		raw := <-bodies
		if size < 0 {
			require.NotContains(t, raw, "size")
		} else {
			require.Equal(t, float64(size), raw["size"])
		}
	}
	var docs []singleScanDocument
	require.NoError(t, db.NewSearch(&docs).Index("docs").Size(1000).Scan(ctx))
	require.Equal(t, float64(1000), (<-bodies)["size"], "slice scans must preserve their requested size")
	var doc *singleScanDocument
	require.NoError(t, db.NewSearch(&doc).Index("docs").Size(1000).Scan(ctx))
	require.Equal(t, "first", doc.Title)
	require.Equal(t, float64(1), (<-bodies)["size"])
	spans := telemetrytest.Ended(recorder)
	require.Len(t, spans, 10)
	for _, span := range spans {
		require.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
	}
}

func TestInvalidScanDestinationDoesNotQuery(t *testing.T) {
	db := New()
	for _, target := range []any{(*singleScanDocument)(nil), singleScanDocument{}, new(int)} {
		q := &SearchQuery{db: db, index: "docs", model: target, size: -1}
		require.Error(t, q.Scan(t.Context()), "invalid destination must fail before accessing the client")
	}
}
