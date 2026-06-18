package esdriver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// opensearchLikeServer mimics OpenSearch: valid ES-shaped responses but WITHOUT
// the X-Elastic-Product header. The go-elasticsearch v8 product check rejects
// such servers unless WithOpenSearch injects the header.
func opensearchLikeServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte(`{"name":"node","cluster_name":"opensearch","version":{"number":"7.10.2","distribution":"opensearch"}}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
}

// TestOpen_OpenSearchProductCheck verifies the WithOpenSearch escape hatch:
// without it the v8 client rejects a header-less (OpenSearch) server; with it
// the injected X-Elastic-Product header satisfies the product check.
func TestOpen_OpenSearchProductCheck(t *testing.T) {
	srv := opensearchLikeServer()
	defer srv.Close()

	if err := New().Open(context.Background(), srv.URL); err == nil {
		t.Fatal("expected product-check failure against an OpenSearch-like server without WithOpenSearch()")
	}

	if err := New().Open(context.Background(), srv.URL, WithOpenSearch()); err != nil {
		t.Fatalf("WithOpenSearch() should accept an OpenSearch-like server, got: %v", err)
	}
}
