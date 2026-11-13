package esdriver

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/observability/telemetrytest"
)

func TestPingReusesHTTPConnection(t *testing.T) {
	var mu sync.Mutex
	addresses := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addresses[r.RemoteAddr] = true
		mu.Unlock()
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = w.Write([]byte(`{"name":"node","version":{"number":"8.17.0"}}`))
	}))
	defer srv.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	for range 3 {
		require.NoError(t, db.Ping(t.Context()))
	}
	mu.Lock()
	count := len(addresses)
	mu.Unlock()
	require.Equal(t, 1, count, "health checks must drain response bodies and reuse their connection")
}

func TestCloseReleasesOwnedHTTPTransport(t *testing.T) {
	var active atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = w.Write([]byte(`{"hits":{"hits":[]}}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			active.Add(1)
		}
		if state == http.StateClosed {
			active.Add(-1)
		}
	}
	srv.Start()
	defer srv.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	_, err := db.NewSearch().Index("docs").ScanHits(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, active.Load())
	require.NoError(t, db.Close())
	require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
}

func TestOpenPreservesExistingClient(t *testing.T) {
	db := New()
	srv := fakeES(t)
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	existing := db.Client()
	require.ErrorContains(t, db.Open(t.Context(), srv.URL), "already opened")
	require.Same(t, existing, db.Client())
}

func TestScrollLifecycle(t *testing.T) {
	var requests atomic.Int32
	var mu sync.Mutex
	var cleared, keepAlive string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.URL.Path == "/":
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/_search"):
			_, _ = w.Write([]byte(`{"_scroll_id":"first","hits":{"hits":[{"_id":"1"},{"_id":"2"}]}}`))
		case r.Method == http.MethodDelete:
			mu.Lock()
			cleared = strings.TrimPrefix(r.URL.Path, "/_search/scroll/")
			mu.Unlock()
			_, _ = w.Write([]byte(`{"succeeded":true}`))
		default:
			requests.Add(1)
			mu.Lock()
			keepAlive = r.URL.Query().Get("scroll")
			mu.Unlock()
			_, _ = w.Write([]byte(`{"_scroll_id":"last","hits":{"hits":[]}}`))
		}
	}))
	defer srv.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	rec := telemetrytest.Spans(t)
	ctx, parent := telemetrytest.Parent(t.Context())
	cursor, err := db.NewSearch().Index("docs").Size(2).Scroll(ctx, "2m")
	require.NoError(t, err)
	defer cursor.Close()
	var ids []string
	for cursor.Next() {
		ids = append(ids, cursor.Hit().ID)
	}
	require.NoError(t, cursor.Err())
	require.Equal(t, []string{"1", "2"}, ids, "initial page must include its first hit")
	require.NoError(t, cursor.Close())
	parent.End()
	require.EqualValues(t, 1, requests.Load())
	mu.Lock()
	lastID, duration := cleared, keepAlive
	mu.Unlock()
	require.Equal(t, "last", lastID, "clear the most recently returned scroll ID, including empty pages")
	require.Contains(t, []string{"2m", "120000ms"}, duration)
	spans := telemetrytest.Ended(rec)
	require.Len(t, spans, 3, "search, scroll, and clear-scroll must retain tracing context")
	for _, span := range spans {
		require.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
	}
}

func TestCanceledScrollDoesNotFetch(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/_search/scroll" && r.Method != http.MethodDelete {
			requests.Add(1)
		}
		_, _ = w.Write([]byte(`{"_scroll_id":"first","hits":{"hits":[{"_id":"1"}]}}`))
	}))
	defer srv.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cursor, err := db.NewSearch().Index("docs").Scroll(ctx, "1m")
	require.NoError(t, err)
	defer cursor.Close()
	cancel()
	require.False(t, cursor.Next())
	require.ErrorIs(t, cursor.Err(), context.Canceled)
	require.Zero(t, requests.Load())
	require.NoError(t, cursor.Close(), "cleanup must still work after cancellation")
}

func TestNativeClientWithoutContextDoesNotPanic(t *testing.T) {
	db := New()
	require.NoError(t, db.Open(t.Context(), fakeES(t).URL))
	defer db.Close()
	require.NotPanics(t, func() {
		res, err := db.Client().Info()
		require.NoError(t, err)
		defer res.Body.Close()
	})
}

func TestScrollCancellationInterruptsPendingFetch(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.URL.Path == "/_search/scroll" && r.Method != http.MethodDelete {
			close(started)
			select {
			case <-r.Context().Done():
			case <-unblock:
			}
			return
		}
		_, _ = w.Write([]byte(`{"_scroll_id":"first","hits":{"hits":[{"_id":"1"}]}}`))
	}))
	defer srv.Close()
	defer close(unblock)
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cursor, err := db.NewSearch().Index("docs").Size(1).Scroll(ctx, "1m")
	require.NoError(t, err)
	defer cursor.Close()
	require.True(t, cursor.Next())
	done := make(chan bool, 1)
	go func() { done <- cursor.Next() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fetch did not start")
	}
	cancel()
	select {
	case next := <-done:
		require.False(t, next)
	case <-time.After(time.Second):
		t.Fatal("canceled fetch kept its connection busy")
	}
	require.ErrorIs(t, cursor.Err(), context.Canceled)
}

func TestScrollCloseReportsFailureAndCanRetry(t *testing.T) {
	var clears atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if r.Method == http.MethodDelete && clears.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"cleanup_failed","reason":"try again"},"status":400}`))
			return
		}
		_, _ = w.Write([]byte(`{"_scroll_id":"first","hits":{"hits":[]}}`))
	}))
	defer srv.Close()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL))
	defer db.Close()
	cursor, err := db.NewSearch().Index("docs").Scroll(t.Context(), "1m")
	require.NoError(t, err)
	require.Error(t, cursor.Close())
	require.False(t, cursor.Next())
	require.NoError(t, cursor.Close())
	require.NoError(t, cursor.Close())
	require.EqualValues(t, 2, clears.Load())
}

func TestOwnedTransportWithCACertAndOpenSearch(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL, WithCACert(cert), WithOpenSearch()))
	require.NotNil(t, db.transport)
	require.NoError(t, db.Ping(t.Context()))
	require.NoError(t, db.Close())
}

func TestCustomTransportRemainsCallerOwned(t *testing.T) {
	base, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	transport := base.Clone()
	defer transport.CloseIdleConnections()
	db := New()
	require.NoError(t, db.Open(t.Context(), fakeES(t).URL, WithTransport(transport)))
	require.Nil(t, db.transport)
	require.NoError(t, db.Close())
}

func TestFailedOpenReleasesOwnedConnectionsAndCanRetry(t *testing.T) {
	var active atomic.Int32
	var available atomic.Bool
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if !available.Load() {
			w.WriteHeader(http.StatusUnauthorized)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			active.Add(1)
		}
		if state == http.StateClosed {
			active.Add(-1)
		}
	}
	srv.Start()
	defer srv.Close()
	db := New()
	require.Error(t, db.Open(t.Context(), srv.URL))
	require.Nil(t, db.Client())
	require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
	available.Store(true)
	require.NoError(t, db.Open(t.Context(), srv.URL))
	require.NoError(t, db.Close())
}
