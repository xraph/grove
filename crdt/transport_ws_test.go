package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock WebSocketConn ---

type mockWSConn struct {
	mu      sync.Mutex
	readCh  chan []byte
	writeCh chan []byte
	closed  bool
}

func newMockWSConn() *mockWSConn {
	return &mockWSConn{
		readCh:  make(chan []byte, 64),
		writeCh: make(chan []byte, 64),
	}
}

func (c *mockWSConn) ReadMessage() ([]byte, error) {
	data, ok := <-c.readCh
	if !ok {
		return nil, errors.New("connection closed")
	}
	return data, nil
}

func (c *mockWSConn) WriteMessage(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("connection closed")
	}
	c.writeCh <- data
	return nil
}

func (c *mockWSConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.readCh)
	}
	return nil
}

// --- Mock Forge Connection ---

type mockForgeConn struct {
	id      string
	readCh  chan []byte
	writeCh chan []byte
	mu      sync.Mutex
	closed  bool
}

func newMockForgeConn(id string) *mockForgeConn {
	return &mockForgeConn{
		id:      id,
		readCh:  make(chan []byte, 64),
		writeCh: make(chan []byte, 64),
	}
}

func (c *mockForgeConn) ID() string { return c.id }

func (c *mockForgeConn) Read() ([]byte, error) {
	data, ok := <-c.readCh
	if !ok {
		return nil, errors.New("connection closed")
	}
	return data, nil
}

func (c *mockForgeConn) Write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("connection closed")
	}
	c.writeCh <- data
	return nil
}

func (c *mockForgeConn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(data)
}

func (c *mockForgeConn) ReadJSON(v any) error {
	data, err := c.Read()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (c *mockForgeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.readCh)
	}
	return nil
}

// --- ForgeWSConn Tests ---

func TestNewForgeWSConn(t *testing.T) {
	fc := newMockForgeConn("conn-42")
	adapter := NewForgeWSConn(fc)
	require.NotNil(t, adapter)
	assert.Equal(t, "conn-42", adapter.ID())
}

func TestForgeWSConn_ReadMessage(t *testing.T) {
	fc := newMockForgeConn("conn-1")
	adapter := NewForgeWSConn(fc)

	fc.readCh <- []byte("hello")
	data, err := adapter.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
}

func TestForgeWSConn_WriteMessage(t *testing.T) {
	fc := newMockForgeConn("conn-1")
	adapter := NewForgeWSConn(fc)

	err := adapter.WriteMessage([]byte("world"))
	require.NoError(t, err)

	written := <-fc.writeCh
	assert.Equal(t, []byte("world"), written)
}

func TestForgeWSConn_Close(t *testing.T) {
	fc := newMockForgeConn("conn-1")
	adapter := NewForgeWSConn(fc)

	err := adapter.Close()
	assert.NoError(t, err)
	assert.True(t, fc.closed)
}

// --- WebSocketTransport Tests ---

func TestWebSocketTransport_SendRequest_ReceiveResponse(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn, WithWSPingInterval(time.Hour)) // disable ping

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the read loop in background.
	go func() {
		_ = transport.Start(ctx)
	}()

	// Simulate sending a pull request in background.
	var pullResp *PullResponse
	var pullErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		pullResp, pullErr = transport.Pull(ctx, &PullRequest{
			Tables: []string{"docs"},
			NodeID: "client",
		})
	}()

	// Read the message the transport sent.
	var sentMsg WebSocketMessage
	select {
	case raw := <-conn.writeCh:
		require.NoError(t, json.Unmarshal(raw, &sentMsg))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sent message")
	}

	assert.Equal(t, WSPullRequest, sentMsg.Type)
	assert.NotEmpty(t, sentMsg.RequestID)

	// Send a correlated response back.
	respPayload, _ := json.Marshal(PullResponse{
		Changes: []ChangeRecord{{Table: "docs", PK: "1", Field: "title"}},
	})
	respMsg := WebSocketMessage{
		Type:      WSPullResponse,
		Payload:   respPayload,
		RequestID: sentMsg.RequestID,
	}
	respData, _ := json.Marshal(respMsg)
	conn.readCh <- respData

	select {
	case <-done:
		require.NoError(t, pullErr)
		require.NotNil(t, pullResp)
		assert.Len(t, pullResp.Changes, 1)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pull response")
	}

	_ = transport.Close()
}

func TestWebSocketTransport_HandleMessage_DispatchesChangeEvents(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn, WithWSPingInterval(time.Hour))

	var received []ChangeRecord
	var mu sync.Mutex
	transport.OnChange(func(cr ChangeRecord) {
		mu.Lock()
		received = append(received, cr)
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = transport.Start(ctx)
	}()

	// Send a change event via the connection.
	changePayload, _ := json.Marshal(ChangeRecord{Table: "docs", PK: "1", Field: "title"})
	msg := WebSocketMessage{Type: WSChange, Payload: changePayload}
	data, _ := json.Marshal(msg)
	conn.readCh <- data

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Len(t, received, 1)
	assert.Equal(t, "docs", received[0].Table)
	mu.Unlock()

	_ = transport.Close()
}

func TestWebSocketTransport_HandleMessage_DispatchesPresenceEvents(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn, WithWSPingInterval(time.Hour))

	var received *PresenceEvent
	var mu sync.Mutex
	transport.OnPresence(func(pe PresenceEvent) {
		mu.Lock()
		received = &pe
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = transport.Start(ctx)
	}()

	presencePayload, _ := json.Marshal(PresenceEvent{Type: "join", NodeID: "n-1", Topic: "room"})
	msg := WebSocketMessage{Type: WSPresenceEvent, Payload: presencePayload}
	data, _ := json.Marshal(msg)
	conn.readCh <- data

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.NotNil(t, received)
	assert.Equal(t, "join", received.Type)
	assert.Equal(t, "n-1", received.NodeID)
	mu.Unlock()

	_ = transport.Close()
}

func TestWebSocketTransport_OnChange_MultipleHandlers(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn, WithWSPingInterval(time.Hour))

	var count1, count2 int
	var mu sync.Mutex
	transport.OnChange(func(_ ChangeRecord) {
		mu.Lock()
		count1++
		mu.Unlock()
	})
	transport.OnChange(func(_ ChangeRecord) {
		mu.Lock()
		count2++
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = transport.Start(ctx)
	}()

	changePayload, _ := json.Marshal(ChangeRecord{Table: "docs", PK: "1"})
	msg := WebSocketMessage{Type: WSChange, Payload: changePayload}
	data, _ := json.Marshal(msg)
	conn.readCh <- data

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, count1)
	assert.Equal(t, 1, count2)
	mu.Unlock()

	_ = transport.Close()
}

func TestWebSocketTransport_Close_SetsFlag(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn)

	assert.False(t, transport.closed.Load())
	_ = transport.Close()
	assert.True(t, transport.closed.Load())
}

// --- WebSocketHandler Tests ---

func TestWebSocketHandler_HandleMessage_PullRequest(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)
	conn := newMockWSConn()
	handler := NewWebSocketHandler(ctrl, conn, nil)

	pullPayload, _ := json.Marshal(PullRequest{Tables: []string{"docs"}, NodeID: "client"})
	msg := WebSocketMessage{
		Type:      WSPullRequest,
		Payload:   pullPayload,
		RequestID: "req-1",
	}

	handler.handleMessage(context.Background(), msg)

	// Read the response from conn.
	select {
	case raw := <-conn.writeCh:
		var resp WebSocketMessage
		require.NoError(t, json.Unmarshal(raw, &resp))
		assert.Equal(t, WSPullResponse, resp.Type)
		assert.Equal(t, "req-1", resp.RequestID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestWebSocketHandler_HandleMessage_PushRequest(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)
	conn := newMockWSConn()
	handler := NewWebSocketHandler(ctrl, conn, nil)

	pushPayload, _ := json.Marshal(PushRequest{
		Changes: []ChangeRecord{
			{Table: "docs", PK: "1", Field: "title", CRDTType: TypeLWW,
				HLC: HLC{Timestamp: 100, NodeID: "remote"}, NodeID: "remote",
				Value: json.RawMessage(`"v"`)},
		},
		NodeID: "remote",
	})
	msg := WebSocketMessage{
		Type:      WSPushRequest,
		Payload:   pushPayload,
		RequestID: "req-2",
	}

	handler.handleMessage(context.Background(), msg)

	select {
	case raw := <-conn.writeCh:
		var resp WebSocketMessage
		require.NoError(t, json.Unmarshal(raw, &resp))
		assert.Equal(t, WSPushResponse, resp.Type)
		assert.Equal(t, "req-2", resp.RequestID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestWebSocketHandler_HandleMessage_Ping(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)
	conn := newMockWSConn()
	handler := NewWebSocketHandler(ctrl, conn, nil)

	msg := WebSocketMessage{Type: WSPing}
	handler.handleMessage(context.Background(), msg)

	select {
	case raw := <-conn.writeCh:
		var resp WebSocketMessage
		require.NoError(t, json.Unmarshal(raw, &resp))
		assert.Equal(t, WSPong, resp.Type)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pong")
	}
}

func TestWebSocketHandler_HandleMessage_UnknownType(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)
	conn := newMockWSConn()
	handler := NewWebSocketHandler(ctrl, conn, nil)

	msg := WebSocketMessage{Type: "unknown_type", RequestID: "req-x"}
	handler.handleMessage(context.Background(), msg)

	select {
	case raw := <-conn.writeCh:
		var resp WebSocketMessage
		require.NoError(t, json.Unmarshal(raw, &resp))
		assert.Equal(t, WSError, resp.Type)
		assert.Equal(t, "req-x", resp.RequestID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error response")
	}
}

func TestWebSocketHandler_HandleMessage_Subscribe(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin, WithStreamPollInterval(50*time.Millisecond))
	conn := newMockWSConn()
	handler := NewWebSocketHandler(ctrl, conn, nil)

	subPayload, _ := json.Marshal(map[string]any{"tables": []string{"docs"}})
	msg := WebSocketMessage{
		Type:    WSSubscribe,
		Payload: subPayload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	handler.handleMessage(ctx, msg)

	// Verify subscribe was recorded.
	handler.mu.Lock()
	assert.True(t, handler.subscribing)
	assert.Equal(t, []string{"docs"}, handler.tables)
	handler.mu.Unlock()
}

// --- WebSocket Options Tests ---

func TestWebSocketTransport_Options(t *testing.T) {
	conn := newMockWSConn()
	transport := NewWebSocketTransport(conn,
		WithWSReconnectDelay(10*time.Second),
		WithWSPingInterval(15*time.Second),
		WithWSTables("t1", "t2"),
	)

	assert.Equal(t, 10*time.Second, transport.reconnectDelay)
	assert.Equal(t, 15*time.Second, transport.pingInterval)
	assert.Equal(t, []string{"t1", "t2"}, transport.tables)
}
