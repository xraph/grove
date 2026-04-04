package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/xraph/go-utils/log"
)

// WebSocketMessage is the JSON framing format for multiplexed WebSocket messages.
// When using the binary protobuf format, use the proto WebSocketFrame instead.
type WebSocketMessage struct {
	Type      WSMessageType   `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	RequestID string          `json:"request_id,omitempty"`
}

// WSMessageType identifies the type of WebSocket message.
type WSMessageType string

const (
	WSPullRequest    WSMessageType = "pull_request"
	WSPullResponse   WSMessageType = "pull_response"
	WSPushRequest    WSMessageType = "push_request"
	WSPushResponse   WSMessageType = "push_response"
	WSChange         WSMessageType = "change"
	WSChanges        WSMessageType = "changes"
	WSPresenceUpdate WSMessageType = "presence_update"
	WSPresenceEvent  WSMessageType = "presence_event"
	WSPresenceGet    WSMessageType = "presence_get"
	WSPresenceSnap   WSMessageType = "presence_snapshot"
	WSSubscribe      WSMessageType = "subscribe"
	WSUnsubscribe    WSMessageType = "unsubscribe"
	WSError          WSMessageType = "error"
	WSPing           WSMessageType = "ping"
	WSPong           WSMessageType = "pong"
)

// WebSocketConn is the interface that any WebSocket implementation must satisfy.
// This allows plugging in gorilla/websocket, nhooyr.io/websocket, or any other library.
type WebSocketConn interface {
	// ReadMessage reads the next message from the connection.
	ReadMessage() ([]byte, error)
	// WriteMessage sends a message over the connection.
	WriteMessage(data []byte) error
	// Close closes the connection.
	Close() error
}

// WebSocketTransport implements the Transport interface over WebSocket connections.
// It provides bidirectional multiplexed communication for pull, push, presence,
// and real-time change streaming over a single connection.
type WebSocketTransport struct {
	conn           WebSocketConn
	logger         log.Logger
	reconnectDelay time.Duration
	pingInterval   time.Duration
	tables         []string

	mu              sync.Mutex
	pending         map[string]chan json.RawMessage // requestID → response channel
	changeHandlers  []func(ChangeRecord)
	presenceHandler func(PresenceEvent)
	requestCounter  atomic.Int64
	closed          atomic.Bool
}

// WebSocketOption configures a WebSocketTransport.
type WebSocketOption func(*WebSocketTransport)

// WithWSLogger sets the logger.
func WithWSLogger(l log.Logger) WebSocketOption {
	return func(t *WebSocketTransport) { t.logger = l }
}

// WithWSReconnectDelay sets the reconnect delay.
func WithWSReconnectDelay(d time.Duration) WebSocketOption {
	return func(t *WebSocketTransport) { t.reconnectDelay = d }
}

// WithWSPingInterval sets the ping/keepalive interval.
func WithWSPingInterval(d time.Duration) WebSocketOption {
	return func(t *WebSocketTransport) { t.pingInterval = d }
}

// WithWSTables restricts which tables to subscribe to.
func WithWSTables(tables ...string) WebSocketOption {
	return func(t *WebSocketTransport) { t.tables = tables }
}

// NewWebSocketTransport creates a new WebSocket-based transport.
func NewWebSocketTransport(conn WebSocketConn, opts ...WebSocketOption) *WebSocketTransport {
	t := &WebSocketTransport{
		conn:           conn,
		logger:         log.NewNoopLogger(),
		reconnectDelay: 5 * time.Second,
		pingInterval:   30 * time.Second,
		pending:        make(map[string]chan json.RawMessage),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Start begins the read loop for processing incoming messages.
// Call this in a goroutine after creating the transport.
func (t *WebSocketTransport) Start(ctx context.Context) error {
	// Send initial subscription if tables are configured.
	if len(t.tables) > 0 {
		if err := t.sendSubscribe(t.tables); err != nil {
			return fmt.Errorf("crdt: ws subscribe: %w", err)
		}
	}

	// Start ping loop.
	go t.pingLoop(ctx)

	// Read loop.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := t.conn.ReadMessage()
		if err != nil {
			if t.closed.Load() {
				return nil
			}
			return fmt.Errorf("crdt: ws read: %w", err)
		}

		var msg WebSocketMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.logger.Error("crdt: ws parse error", log.String("error", err.Error()))
			continue
		}

		t.handleMessage(msg)
	}
}

// Close closes the WebSocket connection.
func (t *WebSocketTransport) Close() error {
	t.closed.Store(true)
	return t.conn.Close()
}

// Pull requests changes from the remote node via WebSocket.
func (t *WebSocketTransport) Pull(ctx context.Context, req *PullRequest) (*PullResponse, error) {
	respData, err := t.sendRequest(ctx, WSPullRequest, req)
	if err != nil {
		return nil, fmt.Errorf("crdt: ws pull: %w", err)
	}
	var resp PullResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("crdt: ws decode pull response: %w", err)
	}
	return &resp, nil
}

// Push sends local changes to the remote node via WebSocket.
func (t *WebSocketTransport) Push(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	respData, err := t.sendRequest(ctx, WSPushRequest, req)
	if err != nil {
		return nil, fmt.Errorf("crdt: ws push: %w", err)
	}
	var resp PushResponse
	if err := json.Unmarshal(respData, &resp); err != nil {
		return nil, fmt.Errorf("crdt: ws decode push response: %w", err)
	}
	return &resp, nil
}

// OnChange registers a handler for incoming change events.
func (t *WebSocketTransport) OnChange(handler func(ChangeRecord)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.changeHandlers = append(t.changeHandlers, handler)
}

// OnPresence registers a handler for incoming presence events.
func (t *WebSocketTransport) OnPresence(handler func(PresenceEvent)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.presenceHandler = handler
}

// sendRequest sends a request and waits for a correlated response.
func (t *WebSocketTransport) sendRequest(ctx context.Context, msgType WSMessageType, payload any) (json.RawMessage, error) {
	requestID := fmt.Sprintf("req-%d", t.requestCounter.Add(1))

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	msg := WebSocketMessage{
		Type:      msgType,
		Payload:   payloadBytes,
		RequestID: requestID,
	}

	respCh := make(chan json.RawMessage, 1)
	t.mu.Lock()
	t.pending[requestID] = respCh
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		delete(t.pending, requestID)
		t.mu.Unlock()
	}()

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if err := t.conn.WriteMessage(data); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// sendMessage sends a fire-and-forget message (no response expected).
func (t *WebSocketTransport) sendMessage(msgType WSMessageType, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := WebSocketMessage{
		Type:    msgType,
		Payload: payloadBytes,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return t.conn.WriteMessage(data)
}

func (t *WebSocketTransport) sendSubscribe(tables []string) error {
	return t.sendMessage(WSSubscribe, map[string]any{"tables": tables})
}

// handleMessage dispatches incoming messages to the appropriate handler.
func (t *WebSocketTransport) handleMessage(msg WebSocketMessage) {
	switch msg.Type {
	case WSPullResponse, WSPushResponse:
		// Correlated response — route to pending request.
		if msg.RequestID != "" {
			t.mu.Lock()
			ch, ok := t.pending[msg.RequestID]
			t.mu.Unlock()
			if ok {
				ch <- msg.Payload
			}
		}

	case WSChange:
		var change ChangeRecord
		if err := json.Unmarshal(msg.Payload, &change); err != nil {
			t.logger.Error("crdt: ws parse change", log.String("error", err.Error()))
			return
		}
		t.mu.Lock()
		handlers := make([]func(ChangeRecord), len(t.changeHandlers))
		copy(handlers, t.changeHandlers)
		t.mu.Unlock()
		for _, h := range handlers {
			h(change)
		}

	case WSChanges:
		var changes []ChangeRecord
		if err := json.Unmarshal(msg.Payload, &changes); err != nil {
			t.logger.Error("crdt: ws parse changes", log.String("error", err.Error()))
			return
		}
		t.mu.Lock()
		handlers := make([]func(ChangeRecord), len(t.changeHandlers))
		copy(handlers, t.changeHandlers)
		t.mu.Unlock()
		for _, change := range changes {
			for _, h := range handlers {
				h(change)
			}
		}

	case WSPresenceEvent:
		var event PresenceEvent
		if err := json.Unmarshal(msg.Payload, &event); err != nil {
			t.logger.Error("crdt: ws parse presence", log.String("error", err.Error()))
			return
		}
		t.mu.Lock()
		handler := t.presenceHandler
		t.mu.Unlock()
		if handler != nil {
			handler(event)
		}

	case WSPong:
		// Pong received — connection is alive.

	case WSError:
		t.logger.Error("crdt: ws remote error", log.String("payload", string(msg.Payload)))

	default:
		t.logger.Warn("crdt: ws unknown message type", log.String("type", string(msg.Type)))
	}
}

func (t *WebSocketTransport) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(t.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if t.closed.Load() {
				return
			}
			if err := t.sendMessage(WSPing, nil); err != nil {
				t.logger.Error("crdt: ws ping failed", log.String("error", err.Error()))
			}
		}
	}
}

// --- Server-side WebSocket Handler ---

// WebSocketHandler processes WebSocket messages on the server side.
// It wraps a SyncController and handles pull/push/subscribe requests
// received over WebSocket.
type WebSocketHandler struct {
	ctrl   *SyncController
	conn   WebSocketConn
	logger log.Logger

	mu            sync.Mutex
	subscribing   bool
	tables        []string
	closed        atomic.Bool
	resubscribeCh chan struct{} // signals stream restart on new subscription
}

// NewWebSocketHandler creates a new server-side WebSocket handler.
func NewWebSocketHandler(ctrl *SyncController, conn WebSocketConn, logger log.Logger) *WebSocketHandler {
	return &WebSocketHandler{
		ctrl:          ctrl,
		conn:          conn,
		logger:        logger,
		resubscribeCh: make(chan struct{}, 1),
	}
}

// Serve reads messages from the WebSocket connection and processes them.
// Call this in a goroutine per client connection.
func (h *WebSocketHandler) Serve(ctx context.Context) error {
	defer h.conn.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := h.conn.ReadMessage()
		if err != nil {
			if h.closed.Load() {
				return nil
			}
			return err
		}

		var msg WebSocketMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			h.sendError(msg.RequestID, "invalid message format")
			continue
		}

		h.handleMessage(ctx, msg)
	}
}

func (h *WebSocketHandler) handleMessage(ctx context.Context, msg WebSocketMessage) {
	switch msg.Type {
	case WSPullRequest:
		var req PullRequest
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			h.sendError(msg.RequestID, "invalid pull request")
			return
		}
		resp, err := h.ctrl.HandlePull(ctx, &req)
		if err != nil {
			h.sendError(msg.RequestID, err.Error())
			return
		}
		h.sendResponse(msg.RequestID, WSPullResponse, resp)

	case WSPushRequest:
		var req PushRequest
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			h.sendError(msg.RequestID, "invalid push request")
			return
		}
		resp, err := h.ctrl.HandlePush(ctx, &req)
		if err != nil {
			h.sendError(msg.RequestID, err.Error())
			return
		}
		h.sendResponse(msg.RequestID, WSPushResponse, resp)

	case WSSubscribe:
		var sub struct {
			Tables []string `json:"tables"`
		}
		if err := json.Unmarshal(msg.Payload, &sub); err != nil {
			h.sendError(msg.RequestID, "invalid subscribe request")
			return
		}
		h.mu.Lock()
		h.tables = sub.Tables
		if !h.subscribing {
			h.subscribing = true
			go h.streamLoop(ctx)
		} else {
			// Signal the existing stream loop to restart with new tables.
			select {
			case h.resubscribeCh <- struct{}{}:
			default:
			}
		}
		h.mu.Unlock()

	case WSPresenceUpdate:
		if h.ctrl.Presence() == nil {
			h.sendError(msg.RequestID, "presence not enabled")
			return
		}
		var update PresenceUpdate
		if err := json.Unmarshal(msg.Payload, &update); err != nil {
			h.sendError(msg.RequestID, "invalid presence update")
			return
		}
		event, err := h.ctrl.HandlePresenceUpdate(ctx, &update)
		if err != nil {
			h.sendError(msg.RequestID, err.Error())
			return
		}
		// Broadcast presence event back.
		h.sendResponse("", WSPresenceEvent, event)

	case WSPing:
		h.sendResponse("", WSPong, nil)

	default:
		h.sendError(msg.RequestID, "unknown message type: "+string(msg.Type))
	}
}

func (h *WebSocketHandler) streamLoop(ctx context.Context) {
	presenceCh := h.ctrl.PresenceChannel()
	lastHLC := HLC{}

	for {
		// Read current table list.
		h.mu.Lock()
		tables := make([]string, len(h.tables))
		copy(tables, h.tables)
		h.mu.Unlock()

		if len(tables) == 0 {
			// Wait for subscription.
			select {
			case <-ctx.Done():
				return
			case <-h.resubscribeCh:
				continue
			}
		}

		ch, err := h.ctrl.StreamChangesSince(ctx, tables, lastHLC)
		if err != nil {
			h.logger.Error("crdt: ws stream error", log.String("error", err.Error()))
			return
		}

		// Stream until context cancelled or resubscribe signal.
		restarted := false
		for !restarted {
			select {
			case <-ctx.Done():
				return
			case <-h.resubscribeCh:
				// Tables changed — restart the stream.
				restarted = true
			case changes, ok := <-ch:
				if !ok {
					return
				}
				for _, change := range changes {
					h.sendResponse("", WSChange, change)
					if change.HLC.After(lastHLC) {
						lastHLC = change.HLC
					}
				}
			case event, ok := <-presenceCh:
				if !ok {
					continue
				}
				h.sendResponse("", WSPresenceEvent, event)
			}
		}
	}
}

func (h *WebSocketHandler) sendResponse(requestID string, msgType WSMessageType, payload any) {
	payloadBytes, _ := json.Marshal(payload) //nolint:errcheck // serialization of known types won't fail
	msg := WebSocketMessage{
		Type:      msgType,
		Payload:   payloadBytes,
		RequestID: requestID,
	}
	data, _ := json.Marshal(msg) //nolint:errcheck // serialization of known struct won't fail
	if err := h.conn.WriteMessage(data); err != nil {
		h.logger.Error("crdt: ws write error", log.String("error", err.Error()))
	}
}

func (h *WebSocketHandler) sendError(requestID, errMsg string) {
	h.sendResponse(requestID, WSError, map[string]string{"error": errMsg})
}
