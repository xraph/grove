package crdt

import (
	"context"
	"encoding/json"
	"errors"
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
	// Close must unblock concurrent ReadMessage and WriteMessage calls.
	Close() error
}

// WebSocketTransport implements the Transport interface over WebSocket connections.
// It provides bidirectional multiplexed communication for pull, push, presence,
// and real-time change streaming over a single connection.
type WebSocketTransport struct {
	conn           *wsClientConn
	logger         log.Logger
	reconnectDelay time.Duration
	pingInterval   time.Duration
	tables         []string

	mu              sync.Mutex
	pending         map[string]chan wsReply // requestID → response channel
	changeHandlers  []func(ChangeRecord)
	presenceHandler func(PresenceEvent)
	requestCounter  atomic.Int64
	closed          atomic.Bool
	done            chan struct{}
}

// ErrWebSocketClosed indicates a closed connection, including a disconnect
// while waiting for a response. Requests are never replayed automatically.
var ErrWebSocketClosed = errors.New("crdt: websocket connection closed")

type wsReply struct {
	payload json.RawMessage
	err     error
}

type wsClientConn struct {
	conn      WebSocketConn
	writer    chan struct{}
	done      chan struct{}
	reading   atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newWSClientConn(conn WebSocketConn) *wsClientConn {
	return &wsClientConn{conn: conn, writer: make(chan struct{}, 1), done: make(chan struct{})}
}

func (c *wsClientConn) close() error {
	c.closeOnce.Do(func() { close(c.done); c.closeErr = c.conn.Close() })
	return c.closeErr
}

func (c *wsClientConn) write(ctx context.Context, data []byte) error {
	select {
	case c.writer <- struct{}{}:
		defer func() { <-c.writer }()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrWebSocketClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return ErrWebSocketClosed
	default:
	}
	// The generic connection has no write deadline API. Closing is the only
	// way to interrupt a blocked write once its context has been cancelled.
	stop := context.AfterFunc(ctx, func() { _ = c.close() }) // cancellation cleanup; write reports the failure
	err := c.conn.WriteMessage(data)
	stop()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// WebSocketOption configures a WebSocketTransport.
type WebSocketOption func(*WebSocketTransport)

// WithWSLogger sets the logger.
func WithWSLogger(l log.Logger) WebSocketOption {
	return func(t *WebSocketTransport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithWSReconnectDelay sets the reconnect delay.
func WithWSReconnectDelay(d time.Duration) WebSocketOption {
	return func(t *WebSocketTransport) { t.reconnectDelay = d }
}

// WithWSPingInterval sets the ping/keepalive interval.
func WithWSPingInterval(d time.Duration) WebSocketOption {
	return func(t *WebSocketTransport) {
		if d > 0 {
			t.pingInterval = d
		}
	}
}

// WithWSTables restricts which tables to subscribe to.
func WithWSTables(tables ...string) WebSocketOption {
	return func(t *WebSocketTransport) { t.tables = tables }
}

// NewWebSocketTransport creates a new WebSocket-based transport.
func NewWebSocketTransport(conn WebSocketConn, opts ...WebSocketOption) *WebSocketTransport {
	t := &WebSocketTransport{
		conn:           newWSClientConn(conn),
		logger:         log.NewNoopLogger(),
		reconnectDelay: 5 * time.Second,
		pingInterval:   30 * time.Second,
		pending:        make(map[string]chan wsReply),
		done:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Start begins the read loop for processing incoming messages.
// Call this in a goroutine after creating the transport.
// Cancelling ctx closes this connection and wakes pending requests.
func (t *WebSocketTransport) Start(ctx context.Context) error {
	conn := t.connection()
	if t.closed.Load() {
		return ErrWebSocketClosed
	}
	if !conn.reading.CompareAndSwap(false, true) {
		return fmt.Errorf("crdt: websocket read loop already started")
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stopClose := context.AfterFunc(streamCtx, func() { t.closeConnection(conn) })
	var pingDone chan struct{}
	defer func() {
		cancel()
		t.closeConnection(conn)
		stopClose()
		if pingDone != nil {
			<-pingDone
		}
	}()

	if len(t.tables) > 0 {
		if err := t.sendOn(streamCtx, conn, WSSubscribe, map[string]any{"tables": t.tables}); err != nil {
			return fmt.Errorf("crdt: ws subscribe: %w", err)
		}
	}
	pingDone = make(chan struct{})
	go func() { defer close(pingDone); t.pingLoop(streamCtx, conn) }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := conn.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
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

// WebSocketDialer is a function that establishes a new WebSocket connection.
// Used by StartWithReconnect to re-establish connections after disconnection.
type WebSocketDialer func(ctx context.Context) (WebSocketConn, error)

// StartWithReconnect begins the read loop and automatically reconnects on
// disconnection using the provided dialer. It blocks until the context is
// cancelled. Between reconnection attempts, it waits for reconnectDelay.
func (t *WebSocketTransport) StartWithReconnect(ctx context.Context, dial WebSocketDialer) error {
	// Explicit Close also cancels an in-flight dial and reconnect delay.
	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-t.done:
			cancel()
		case <-dialCtx.Done():
		}
	}()
	for {
		err := t.Start(dialCtx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if t.closed.Load() {
			return nil
		}
		t.logger.Error("crdt: ws disconnected, reconnecting", log.String("error", err.Error()))
		for {
			timer := time.NewTimer(t.reconnectDelay)
			select {
			case <-dialCtx.Done():
				timer.Stop()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			case <-timer.C:
			}
			newConn, dialErr := dial(dialCtx)
			if dialErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if t.closed.Load() {
					return nil
				}
				t.logger.Error("crdt: ws reconnect dial failed", log.String("error", dialErr.Error()))
				continue
			}
			conn := newWSClientConn(newConn)
			t.mu.Lock()
			if t.closed.Load() || ctx.Err() != nil {
				t.mu.Unlock()
				t.closeConnection(conn)
				return ctx.Err()
			}
			t.conn = conn
			t.mu.Unlock()
			break
		}
	}
}

// Close closes the current connection, wakes pending requests, and prevents reconnection.
func (t *WebSocketTransport) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		close(t.done)
	}
	return t.connection().close()
}

func (t *WebSocketTransport) connection() *wsClientConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn
}

func (t *WebSocketTransport) closeConnection(conn *wsClientConn) {
	if err := conn.close(); err != nil {
		t.logger.Debug("crdt: ws close", log.String("error", err.Error()))
	}
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

	respCh := make(chan wsReply, 1)
	t.mu.Lock()
	conn := t.conn
	if t.closed.Load() {
		t.mu.Unlock()
		return nil, ErrWebSocketClosed
	}
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
	if err := conn.write(ctx, data); err != nil {
		return nil, err
	}

	select {
	case resp := <-respCh:
		return resp.payload, resp.err
	case <-conn.done:
		// A server may send its final response immediately before disconnecting.
		// Preserve a response already delivered by the reader.
		select {
		case resp := <-respCh:
			return resp.payload, resp.err
		default:
			return nil, ErrWebSocketClosed
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *WebSocketTransport) sendOn(ctx context.Context, conn *wsClientConn, msgType WSMessageType, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data, err := json.Marshal(WebSocketMessage{Type: msgType, Payload: payloadBytes})
	if err != nil {
		return err
	}
	return conn.write(ctx, data)
}

// handleMessage dispatches incoming messages to the appropriate handler.
func (t *WebSocketTransport) handleMessage(msg WebSocketMessage) {
	switch msg.Type {
	case WSPullResponse, WSPushResponse, WSError:
		reply := wsReply{payload: msg.Payload}
		if msg.Type == WSError {
			var detail struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(msg.Payload, &detail); err != nil || detail.Error == "" {
				detail.Error = "invalid remote error response"
			}
			reply.err = fmt.Errorf("crdt: ws remote error: %s", detail.Error)
		}
		t.mu.Lock()
		ch := t.pending[msg.RequestID]
		delete(t.pending, msg.RequestID)
		if ch != nil {
			ch <- reply
		} // buffered, delivered at most once; never closed
		t.mu.Unlock()
		if ch == nil && reply.err != nil {
			t.logger.Error("crdt: ws remote error", log.String("error", reply.err.Error()))
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

	default:
		t.logger.Warn("crdt: ws unknown message type", log.String("type", string(msg.Type)))
	}
}

func (t *WebSocketTransport) pingLoop(ctx context.Context, conn *wsClientConn) {
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
			if err := t.sendOn(ctx, conn, WSPing, nil); err != nil {
				t.logger.Error("crdt: ws ping failed", log.String("error", err.Error()))
			}
		}
	}
}

// --- Forge Transport Adapters ---

// ForgeWSConn adapts a Forge Connection (from forge.WebSocketHandler) to the
// WebSocketConn interface used by CRDT transport. This allows the CRDT WebSocket
// handler to work natively with Forge's built-in WebSocket upgrade machinery.
//
// Usage inside a Forge WebSocket handler:
//
//	sync.WebSocket("/ws", func(ctx forge.Context, conn forge.Connection) error {
//	    adapter := crdt.NewForgeWSConn(conn)
//	    handler := crdt.NewWebSocketHandler(ctrl, adapter, logger)
//	    return handler.Serve(conn.Context())
//	})
type ForgeWSConn struct {
	id   string
	conn forgeConnection
}

// forgeConnection is the minimal interface from forge.Connection that we need.
// This avoids importing forge directly in the crdt package (which has no
// forge dependency). The extension package provides the concrete type.
type forgeConnection interface {
	ID() string
	Read() ([]byte, error)
	Write(data []byte) error
	WriteJSON(v any) error
	ReadJSON(v any) error
	Close() error
}

// NewForgeWSConn creates a WebSocketConn adapter from a forge.Connection.
func NewForgeWSConn(conn forgeConnection) *ForgeWSConn {
	return &ForgeWSConn{
		id:   conn.ID(),
		conn: conn,
	}
}

// ReadMessage implements WebSocketConn using forge.Connection.Read().
func (f *ForgeWSConn) ReadMessage() ([]byte, error) {
	return f.conn.Read()
}

// WriteMessage implements WebSocketConn using forge.Connection.Write().
func (f *ForgeWSConn) WriteMessage(data []byte) error {
	return f.conn.Write(data)
}

// Close implements WebSocketConn using forge.Connection.Close().
func (f *ForgeWSConn) Close() error {
	return f.conn.Close()
}

// ID returns the forge connection ID.
func (f *ForgeWSConn) ID() string {
	return f.id
}

// --- Server-side WebSocket Handler ---

// WebSocketHandler processes WebSocket messages on the server side.
// It wraps a SyncController and handles pull/push/subscribe requests
// received over WebSocket.
type WebSocketHandler struct {
	ctrl    *SyncController
	conn    WebSocketConn
	logger  log.Logger
	writeMu sync.Mutex // WebSocket implementations commonly require a single writer.

	mu            sync.Mutex
	subscribing   bool
	tables        []string
	since         *HLC   // cursor from the latest subscribe, consumed by streamLoop
	generation    uint64 // bumped by subscribe/unsubscribe; frames from older streams are dropped
	closed        atomic.Bool
	resubscribeCh chan struct{} // signals stream restart on new subscription
}

// NewWebSocketHandler creates a new server-side WebSocket handler.
func NewWebSocketHandler(ctrl *SyncController, conn WebSocketConn, logger log.Logger) *WebSocketHandler {
	if logger == nil {
		logger = log.NewNoopLogger()
	}
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClose := context.AfterFunc(ctx, func() {
		if err := h.conn.Close(); err != nil {
			h.logger.Debug("crdt: ws close", log.String("error", err.Error()))
		}
	})
	defer stopClose()
	defer h.conn.Close()
	// Presence has its own lifetime: a canvas or lobby needs no DB tables.
	if events := h.ctrl.SubscribePresence(ctx); events != nil {
		go h.presenceLoop(ctx, events)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := h.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
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

func (h *WebSocketHandler) presenceLoop(ctx context.Context, events <-chan PresenceEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			h.sendResponse("", WSPresenceEvent, event)
		}
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
		// since is optional: without it the first subscribe streams from
		// HLC zero and a resubscribe continues from the last change sent.
		var sub struct {
			Tables []string `json:"tables"`
			Since  *HLC     `json:"since,omitempty"`
		}
		if err := json.Unmarshal(msg.Payload, &sub); err != nil {
			h.sendError(msg.RequestID, "invalid subscribe request")
			return
		}
		h.mu.Lock()
		h.tables = sub.Tables
		h.since = sub.Since
		h.generation++
		if !h.subscribing {
			h.subscribing = true
			go h.streamLoop(ctx)
		} else {
			h.signalResubscribe()
		}
		h.mu.Unlock()

	case WSUnsubscribe:
		h.mu.Lock()
		h.tables = nil
		h.since = nil
		h.generation++
		h.signalResubscribe()
		h.mu.Unlock()

	case WSPresenceGet:
		var req struct {
			Topic string `json:"topic"`
		}
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				h.sendError(msg.RequestID, "invalid presence get request")
				return
			}
		}
		snapshot, err := h.ctrl.HandleGetPresence(ctx, req.Topic)
		if err != nil {
			h.sendError(msg.RequestID, err.Error())
			return
		}
		h.sendResponse(msg.RequestID, WSPresenceSnap, snapshot)

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

// signalResubscribe wakes streamLoop to pick up a new subscription.
// The caller holds h.mu.
func (h *WebSocketHandler) signalResubscribe() {
	select {
	case h.resubscribeCh <- struct{}{}:
	default:
	}
}

func (h *WebSocketHandler) streamLoop(ctx context.Context) {
	lastHLC := HLC{}

	for {
		// Read the current subscription.
		h.mu.Lock()
		tables := make([]string, len(h.tables))
		copy(tables, h.tables)
		if h.since != nil {
			lastHLC = *h.since
			h.since = nil
		}
		gen := h.generation
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

		// Each subscription gets its own poller, stopped when the
		// subscription is replaced.
		streamCtx, cancel := context.WithCancel(ctx)
		ch, err := h.ctrl.StreamChangesSince(streamCtx, tables, lastHLC)
		if err != nil {
			cancel()
			h.logger.Error("crdt: ws stream error", log.String("error", err.Error()))
			return
		}

		// Stream until context cancelled or resubscribe signal.
		restarted := false
		for !restarted {
			select {
			case <-ctx.Done():
				cancel()
				return
			case <-h.resubscribeCh:
				restarted = true
			case changes, ok := <-ch:
				if !ok {
					cancel()
					return
				}
				h.mu.Lock()
				if h.generation == gen {
					h.sendResponse("", WSChanges, changes)
					for _, change := range changes {
						if change.HLC.After(lastHLC) {
							lastHLC = change.HLC
						}
					}
				}
				h.mu.Unlock()
			}
		}
		cancel()
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
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if err := h.conn.WriteMessage(data); err != nil {
		h.logger.Error("crdt: ws write error", log.String("error", err.Error()))
	}
}

func (h *WebSocketHandler) sendError(requestID, errMsg string) {
	h.sendResponse(requestID, WSError, map[string]string{"error": errMsg})
}
