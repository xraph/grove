package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- HTTPClient Tests ---

func TestHTTPClient_Pull_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/pull", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req PullRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, []string{"documents"}, req.Tables)
		assert.Equal(t, "client-1", req.NodeID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PullResponse{
			Changes: []ChangeRecord{
				{Table: "documents", PK: "1", Field: "title", NodeID: "server"},
			},
			LatestHLC: HLC{Timestamp: 200, Counter: 1, NodeID: "server"},
		})
	}))
	defer server.Close()

	client := HTTPTransport(server.URL)
	resp, err := client.Pull(context.Background(), &PullRequest{
		Tables: []string{"documents"},
		Since:  HLC{Timestamp: 100},
		NodeID: "client-1",
	})

	require.NoError(t, err)
	assert.Len(t, resp.Changes, 1)
	assert.Equal(t, "documents", resp.Changes[0].Table)
	assert.Equal(t, int64(200), resp.LatestHLC.Timestamp)
}

func TestHTTPClient_Pull_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer server.Close()

	client := HTTPTransport(server.URL)
	_, err := client.Pull(context.Background(), &PullRequest{
		Tables: []string{"docs"},
		NodeID: "client",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPClient_Pull_NetworkError(t *testing.T) {
	client := HTTPTransport("http://localhost:1") // Invalid port.

	_, err := client.Pull(context.Background(), &PullRequest{
		Tables: []string{"docs"},
		NodeID: "client",
	})

	assert.Error(t, err)
}

func TestHTTPClient_Push_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/push", r.URL.Path)

		var req PushRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Len(t, req.Changes, 2)
		assert.Equal(t, "client-1", req.NodeID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(PushResponse{
			Merged:    2,
			LatestHLC: HLC{Timestamp: 300, NodeID: "server"},
		})
	}))
	defer server.Close()

	client := HTTPTransport(server.URL)
	resp, err := client.Push(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{Table: "docs", PK: "1", NodeID: "client-1"},
			{Table: "docs", PK: "2", NodeID: "client-1"},
		},
		NodeID: "client-1",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, resp.Merged)
	assert.Equal(t, int64(300), resp.LatestHLC.Timestamp)
}

func TestHTTPClient_Push_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := HTTPTransport(server.URL)
	_, err := client.Push(context.Background(), &PushRequest{
		Changes: []ChangeRecord{{Table: "docs", PK: "1"}},
		NodeID:  "client",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestHTTPTransport_WithClient_CustomClient(t *testing.T) {
	customClient := &http.Client{Timeout: 5 * time.Second}
	transport := HTTPTransportWithClient("https://example.com/sync", customClient)

	assert.Equal(t, "https://example.com/sync", transport.baseURL)
	assert.Equal(t, customClient, transport.httpClient)
}

// --- StreamingTransport Tests ---

func TestStreamingTransport_EmbedHTTPClient(t *testing.T) {
	transport := NewStreamingTransport("https://example.com/sync")

	// Should embed HTTPClient for pull/push.
	assert.Equal(t, "https://example.com/sync", transport.HTTPClient.baseURL)
	// Should set stream URL.
	assert.Equal(t, "https://example.com/sync/stream", transport.streamURL)
	// Should have default reconnect delay.
	assert.Equal(t, 5*time.Second, transport.reconnectDelay)
}

func TestStreamingTransport_WithOptions(t *testing.T) {
	transport := NewStreamingTransport("https://example.com/sync",
		WithStreamTables("docs", "users"),
		WithStreamReconnect(10*time.Second),
	)

	assert.Equal(t, []string{"docs", "users"}, transport.tables)
	assert.Equal(t, 10*time.Second, transport.reconnectDelay)
}

func TestStreamingTransport_BuildStreamURL(t *testing.T) {
	transport := NewStreamingTransport("https://example.com/sync",
		WithStreamTables("docs", "users"),
	)

	// With zero HLC.
	url := transport.buildStreamURL(HLC{})
	assert.Contains(t, url, "tables=docs,users")

	// With non-zero HLC.
	url = transport.buildStreamURL(HLC{Timestamp: 100, Counter: 5, NodeID: "node-1"})
	assert.Contains(t, url, "tables=docs,users")
	assert.Contains(t, url, "since_ts=100")
	assert.Contains(t, url, "since_count=5")
	assert.Contains(t, url, "since_node=node-1")
}

func TestStreamingTransport_BuildStreamURL_NoTables(t *testing.T) {
	transport := NewStreamingTransport("https://example.com/sync")

	url := transport.buildStreamURL(HLC{Timestamp: 50, Counter: 1, NodeID: "n"})
	assert.Contains(t, url, "since_ts=50")
	// Should not contain "tables=".
	assert.NotContains(t, url, "tables=")
}

func TestStreamingTransport_StreamChanges_ReceivesSSE(t *testing.T) {
	// Create a mock SSE server that sends a single change event.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "text/event-stream", r.Header.Get("Accept"))

		flusher, ok := w.(http.Flusher)
		require.True(t, ok, "ResponseWriter must support Flusher")

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)

		// Send a single change event.
		change := ChangeRecord{
			Table:    "docs",
			PK:       "1",
			Field:    "title",
			CRDTType: TypeLWW,
			HLC:      HLC{Timestamp: 100, Counter: 1, NodeID: "server"},
			NodeID:   "server",
			Value:    json.RawMessage(`"hello"`),
		}
		data, _ := json.Marshal(change)
		fmt.Fprintf(w, "event: change\ndata: %s\n\n", data)
		flusher.Flush()

		// Send a batch changes event.
		batch := []ChangeRecord{
			{Table: "docs", PK: "2", Field: "body", CRDTType: TypeLWW, HLC: HLC{Timestamp: 200, NodeID: "server"}, NodeID: "server", Value: json.RawMessage(`"world"`)},
		}
		batchData, _ := json.Marshal(batch)
		fmt.Fprintf(w, "event: changes\ndata: %s\n\n", batchData)
		flusher.Flush()

		// Close the connection by returning.
	}))
	defer server.Close()

	transport := NewStreamingTransport(server.URL,
		WithStreamTables("docs"),
		WithStreamReconnect(100*time.Millisecond),
	)
	// Override the stream URL to point at the test server.
	transport.streamURL = server.URL + "/stream"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var received []ChangeRecord
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = transport.StreamChanges(ctx, HLC{}, func(change ChangeRecord) {
			received = append(received, change)
			if len(received) >= 2 {
				cancel() // Stop after receiving both events.
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for stream to complete")
	}

	assert.GreaterOrEqual(t, len(received), 2)
	assert.Equal(t, "docs", received[0].Table)
	assert.Equal(t, "1", received[0].PK)
	assert.Equal(t, "2", received[1].PK)
}

func TestStreamingTransport_StreamChanges_ContextCancel(t *testing.T) {
	// SSE server that keeps the connection open.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		flusher.Flush()

		// Block until the client disconnects.
		<-r.Context().Done()
	}))
	defer server.Close()

	transport := NewStreamingTransport(server.URL,
		WithStreamReconnect(100*time.Millisecond),
	)
	transport.streamURL = server.URL + "/stream"

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- transport.StreamChanges(ctx, HLC{}, func(change ChangeRecord) {})
	}()

	// Cancel after a short delay.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("StreamChanges should return after context cancel")
	}
}

func TestStreamingTransport_StreamChanges_Reconnects(t *testing.T) {
	var connectionCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionCount.Add(1)

		flusher, ok := w.(http.Flusher)
		if !ok {
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		// Send a keep-alive then close (simulating disconnect).
		fmt.Fprintf(w, ": keep-alive\n\n")
		flusher.Flush()
		// Return immediately to close the connection.
	}))
	defer server.Close()

	transport := NewStreamingTransport(server.URL,
		WithStreamReconnect(50*time.Millisecond),
	)
	transport.streamURL = server.URL + "/stream"

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = transport.StreamChanges(ctx, HLC{}, func(change ChangeRecord) {})

	// Should have reconnected at least twice.
	assert.GreaterOrEqual(t, connectionCount.Load(), int32(2))
}

func TestStreamingTransport_StreamChanges_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	transport := NewStreamingTransport(server.URL,
		WithStreamReconnect(50*time.Millisecond),
	)
	transport.streamURL = server.URL + "/stream"

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := transport.StreamChanges(ctx, HLC{}, func(change ChangeRecord) {})
	// Should return context deadline exceeded after retries.
	assert.Error(t, err)
}

func TestStreamingTransport_ProcessSSEStream_KeepAlive(t *testing.T) {
	transport := NewStreamingTransport("http://example.com/sync")

	// SSE stream with only keep-alive comments.
	stream := ": keep-alive\n\n: ping\n\n"
	reader := strings.NewReader(stream)

	var received []ChangeRecord
	err := transport.processSSEStream(context.Background(), reader, func(change ChangeRecord) {
		received = append(received, change)
	})

	assert.NoError(t, err)
	assert.Empty(t, received) // No change events.
}

func TestStreamingTransport_ProcessSSEStream_InvalidJSON(t *testing.T) {
	transport := NewStreamingTransport("http://example.com/sync")

	// SSE stream with invalid JSON in data.
	stream := "event: change\ndata: {invalid json}\n\n"
	reader := strings.NewReader(stream)

	var received []ChangeRecord
	err := transport.processSSEStream(context.Background(), reader, func(change ChangeRecord) {
		received = append(received, change)
	})

	assert.NoError(t, err)    // Should not error — just log and skip.
	assert.Empty(t, received) // Invalid JSON should be skipped.
}

func TestStreamingTransport_SatisfiesTransportInterface(t *testing.T) {
	var _ Transport = NewStreamingTransport("http://example.com/sync")
}
