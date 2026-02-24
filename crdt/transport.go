package crdt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Transport is the interface for sync communication between nodes.
// Implement this interface to support custom transport layers
// (WebSocket, NATS, Kafka, gRPC, etc.).
type Transport interface {
	// Pull requests changes from a remote node since the given HLC.
	Pull(ctx context.Context, req *PullRequest) (*PullResponse, error)

	// Push sends local changes to a remote node.
	Push(ctx context.Context, req *PushRequest) (*PushResponse, error)
}

// PullRequest asks a remote node for changes since a given point.
type PullRequest struct {
	// Tables to pull changes for.
	Tables []string `json:"tables"`

	// Since is the HLC after which to return changes.
	Since HLC `json:"since"`

	// NodeID of the requesting node.
	NodeID string `json:"node_id"`
}

// PullResponse contains changes from the remote node.
type PullResponse struct {
	// Changes are the field-level change records.
	Changes []ChangeRecord `json:"changes"`

	// LatestHLC is the highest HLC in the response.
	LatestHLC HLC `json:"latest_hlc"`
}

// PushRequest sends local changes to a remote node.
type PushRequest struct {
	// Changes to push.
	Changes []ChangeRecord `json:"changes"`

	// NodeID of the pushing node.
	NodeID string `json:"node_id"`
}

// PushResponse acknowledges a push.
type PushResponse struct {
	// Merged is the number of changes that were merged.
	Merged int `json:"merged"`

	// LatestHLC is the remote node's latest HLC after merging.
	LatestHLC HLC `json:"latest_hlc"`
}

// HTTPClient is a Transport implementation that communicates via HTTP.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

// HTTPTransport creates a new HTTP transport client pointing at the given
// base URL. The sync server should be mounted at that URL (see NewHTTPHandler).
//
//	transport := crdt.HTTPTransport("https://cloud.example.com/sync")
func HTTPTransport(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL:    baseURL,
		httpClient: http.DefaultClient,
	}
}

// HTTPTransportWithClient creates an HTTP transport with a custom http.Client.
func HTTPTransportWithClient(baseURL string, client *http.Client) *HTTPClient {
	return &HTTPClient{
		baseURL:    baseURL,
		httpClient: client,
	}
}

// Pull requests changes from the remote node.
func (c *HTTPClient) Pull(ctx context.Context, req *PullRequest) (*PullResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("crdt: marshal pull request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/pull", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("crdt: create pull request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("crdt: pull: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body) //nolint:errcheck // best-effort error body read
		return nil, fmt.Errorf("crdt: pull returned %d: %s", resp.StatusCode, string(respBody))
	}

	var pullResp PullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pullResp); err != nil {
		return nil, fmt.Errorf("crdt: decode pull response: %w", err)
	}
	return &pullResp, nil
}

// Push sends local changes to the remote node.
func (c *HTTPClient) Push(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("crdt: marshal push request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/push", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("crdt: create push request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("crdt: push: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body) //nolint:errcheck // best-effort error body read
		return nil, fmt.Errorf("crdt: push returned %d: %s", resp.StatusCode, string(respBody))
	}

	var pushResp PushResponse
	if err := json.NewDecoder(resp.Body).Decode(&pushResp); err != nil {
		return nil, fmt.Errorf("crdt: decode push response: %w", err)
	}
	return &pushResp, nil
}

// --- Streaming Transport ---

// StreamingTransport wraps an HTTPClient and adds SSE streaming for real-time
// change propagation. It satisfies the Transport interface for pull/push
// operations and additionally supports StreamChanges for SSE-based real-time sync.
//
// Use NewStreamingTransport to create one:
//
//	t := crdt.NewStreamingTransport("https://cloud.example.com/sync",
//	    crdt.WithStreamTables("documents"),
//	    crdt.WithStreamReconnect(5 * time.Second),
//	)
//
//	// Use as a Transport for pull/push:
//	syncer := crdt.NewSyncer(plugin, crdt.WithTransport(t))
//
//	// Or stream changes in real-time:
//	go t.StreamChanges(ctx, since, func(change crdt.ChangeRecord) {
//	    // process each change as it arrives
//	})
type StreamingTransport struct {
	*HTTPClient // Embeds for Pull/Push.

	streamURL      string
	tables         []string
	reconnectDelay time.Duration
	logger         *slog.Logger
}

// NewStreamingTransport creates a streaming transport that supports both
// pull/push (via embedded HTTPClient) and SSE streaming for real-time changes.
//
// The baseURL should point to the sync server root (e.g., "https://cloud.example.com/sync").
// The SSE endpoint is assumed to be at baseURL + "/stream".
func NewStreamingTransport(baseURL string, opts ...StreamingOption) *StreamingTransport {
	t := &StreamingTransport{
		HTTPClient:     HTTPTransport(baseURL),
		streamURL:      baseURL + "/stream",
		reconnectDelay: 5 * time.Second,
		logger:         slog.Default(),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// StreamChanges connects to the remote SSE endpoint and processes changes
// in real-time. It blocks until the context is cancelled. On disconnection,
// it automatically reconnects after the configured delay.
//
// The handler function is called for each ChangeRecord received from the
// SSE stream. The since parameter specifies the starting HLC; subsequent
// reconnections use the latest HLC received.
func (t *StreamingTransport) StreamChanges(ctx context.Context, since HLC, handler func(ChangeRecord)) error {
	lastHLC := since

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := t.connectAndProcess(ctx, lastHLC, func(change ChangeRecord) {
			if change.HLC.After(lastHLC) {
				lastHLC = change.HLC
			}
			handler(change)
		})

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			t.logger.Error("crdt: SSE stream disconnected",
				slog.String("error", err.Error()),
			)
		}

		// Wait before reconnecting.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.reconnectDelay):
			t.logger.Info("crdt: SSE reconnecting",
				slog.String("stream_url", t.streamURL),
			)
		}
	}
}

// connectAndProcess opens a single SSE connection and processes events
// until the connection is lost or the context is cancelled.
func (t *StreamingTransport) connectAndProcess(ctx context.Context, since HLC, handler func(ChangeRecord)) error {
	// Build stream URL with query params.
	url := t.buildStreamURL(since)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("crdt: create stream request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("crdt: stream connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body) //nolint:errcheck // best-effort error body read
		return fmt.Errorf("crdt: stream returned %d: %s", resp.StatusCode, string(respBody))
	}

	return t.processSSEStream(ctx, resp.Body, handler)
}

// buildStreamURL constructs the SSE endpoint URL with query parameters.
func (t *StreamingTransport) buildStreamURL(since HLC) string {
	url := t.streamURL + "?"

	if len(t.tables) > 0 {
		url += "tables=" + strings.Join(t.tables, ",") + "&"
	}

	if !since.IsZero() {
		url += fmt.Sprintf("since_ts=%d&since_count=%d&since_node=%s",
			since.Timestamp, since.Counter, since.NodeID)
	}

	return strings.TrimRight(url, "&?")
}

// processSSEStream reads an SSE stream and dispatches change events.
// SSE format:
//
//	event: change
//	data: {"table":"docs","pk":"1",...}
//
//	: keep-alive comment
func (t *StreamingTransport) processSSEStream(ctx context.Context, body io.Reader, handler func(ChangeRecord)) error {
	scanner := bufio.NewScanner(body)
	var eventType string
	var dataLines []string

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()

		// Empty line = end of event.
		if line == "" {
			if eventType == "change" && len(dataLines) > 0 {
				data := strings.Join(dataLines, "\n")
				t.handleChangeEvent(data, handler)
			} else if eventType == "changes" && len(dataLines) > 0 {
				data := strings.Join(dataLines, "\n")
				t.handleChangesEvent(data, handler)
			}
			eventType = ""
			dataLines = nil
			continue
		}

		// SSE comment line (keep-alive).
		if strings.HasPrefix(line, ":") {
			continue
		}

		// Parse SSE fields.
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("crdt: SSE read error: %w", err)
	}

	return nil // Stream ended cleanly.
}

// handleChangeEvent processes a single "change" SSE event.
func (t *StreamingTransport) handleChangeEvent(data string, handler func(ChangeRecord)) {
	var change ChangeRecord
	if err := json.Unmarshal([]byte(data), &change); err != nil {
		t.logger.Error("crdt: SSE parse change event",
			slog.String("error", err.Error()),
		)
		return
	}
	handler(change)
}

// handleChangesEvent processes a batch "changes" SSE event (JSON array).
func (t *StreamingTransport) handleChangesEvent(data string, handler func(ChangeRecord)) {
	var changes []ChangeRecord
	if err := json.Unmarshal([]byte(data), &changes); err != nil {
		t.logger.Error("crdt: SSE parse changes event",
			slog.String("error", err.Error()),
		)
		return
	}
	for _, change := range changes {
		handler(change)
	}
}
