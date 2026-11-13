package esdriver

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultPoolBoundsConcurrentConnections(t *testing.T) {
	testPoolBounds(t, 10)
}

func TestExplicitPoolBoundsConcurrentConnections(t *testing.T) {
	testPoolBounds(t, 2, WithPoolSize(2), WithPoolSize(0), WithPoolSize(-1))
}

func TestPoolSizeClonesCustomTransport(t *testing.T) {
	base, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	custom := base.Clone()
	custom.MaxConnsPerHost = 20
	custom.MaxIdleConnsPerHost = 15
	defer custom.CloseIdleConnections()
	opts := defaultEsOptions()
	opts.apply([]EsOption{WithTransport(custom), WithPoolSize(1)})
	transport, owned, err := newTransport(opts)
	require.NoError(t, err)
	require.Same(t, owned, transport)
	require.NotSame(t, custom, owned)
	require.Equal(t, 1, owned.MaxConnsPerHost)
	require.Equal(t, 1, owned.MaxIdleConnsPerHost)
	require.Equal(t, 20, custom.MaxConnsPerHost)
	require.Equal(t, 15, custom.MaxIdleConnsPerHost)
	testPoolBounds(t, 1, WithTransport(custom), WithPoolSize(1))
}

func TestPoolSizePreservesConfiguredDefaultTransport(t *testing.T) {
	base, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	custom := base.Clone()
	custom.MaxConnsPerHost = 3
	http.DefaultTransport = custom
	t.Cleanup(func() { http.DefaultTransport = base; custom.CloseIdleConnections() })
	transport, owned, err := newTransport(defaultEsOptions())
	require.NoError(t, err)
	require.Same(t, owned, transport)
	require.NotSame(t, custom, owned)
	require.Equal(t, 3, owned.MaxConnsPerHost)
	opts := defaultEsOptions()
	opts.apply([]EsOption{WithPoolSize(1)})
	_, override, err := newTransport(opts)
	require.NoError(t, err)
	require.Equal(t, 1, override.MaxConnsPerHost)
	require.Equal(t, 3, custom.MaxConnsPerHost)
}

func TestPoolSizeComposesWithCACertAndOpenSearch(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	custom, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL, WithTransport(custom), WithPoolSize(1), WithCACert(cert), WithOpenSearch()))
	defer db.Close()
	require.NotSame(t, custom, db.transport)
	require.Equal(t, 1, db.transport.MaxConnsPerHost)
	require.NoError(t, db.Ping(t.Context()))
}

type poolTestTransport struct{}

func (poolTestTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("invalid pool configuration must fail before making requests")
}

func TestPoolSizeRejectsUnsupportedCustomTransport(t *testing.T) {
	db := New()
	require.ErrorContains(t, db.Open(t.Context(), "http://localhost:9200", WithTransport(poolTestTransport{}), WithPoolSize(1)), "cannot set pool size")
	require.Nil(t, db.Client())
}

func testPoolBounds(t *testing.T, size int, opts ...EsOption) {
	t.Helper()
	var block atomic.Bool
	var requests atomic.Int32
	entered := make(chan struct{}, size+1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if block.Load() {
			requests.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte(`{"name":"node","version":{"number":"8.17.0"}}`))
	}))
	defer srv.Close()
	defer unblock()
	db := New()
	require.NoError(t, db.Open(t.Context(), srv.URL, opts...))
	defer db.Close()
	block.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results := make(chan error, size)
	for range size {
		go func() { results <- db.Ping(ctx) }()
	}
	for range size {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("requests did not fill the connection pool")
		}
	}
	waitCtx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	require.ErrorIs(t, db.Ping(waitCtx), context.DeadlineExceeded)
	require.EqualValues(t, size, requests.Load(), "excess requests must wait instead of opening another connection")
	block.Store(false)
	unblock()
	for range size {
		require.NoError(t, <-results)
	}
	require.NoError(t, db.Ping(ctx), "canceling a queued request must not strand a pool slot")
}
