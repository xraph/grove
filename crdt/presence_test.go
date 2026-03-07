package crdt

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- PresenceManager Unit Tests ---

func TestPresenceManager_Update_Join(t *testing.T) {
	var events []PresenceEvent
	var mu sync.Mutex

	pm := NewPresenceManager(30*time.Second, func(e PresenceEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, nil)
	defer pm.Close()

	event := pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "documents:doc-1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})

	assert.Equal(t, PresenceJoin, event.Type)
	assert.Equal(t, "node-1", event.NodeID)
	assert.Equal(t, "documents:doc-1", event.Topic)
	assert.JSONEq(t, `{"name":"Alice"}`, string(event.Data))

	mu.Lock()
	assert.Len(t, events, 1)
	assert.Equal(t, PresenceJoin, events[0].Type)
	mu.Unlock()
}

func TestPresenceManager_Update_ExistingBecomesUpdate(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"cursor":{"x":0,"y":0}}`),
	})

	event := pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"cursor":{"x":10,"y":20}}`),
	})

	assert.Equal(t, PresenceUpdateEvt, event.Type)
	assert.JSONEq(t, `{"cursor":{"x":10,"y":20}}`, string(event.Data))
}

func TestPresenceManager_Get(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})
	pm.Update(PresenceUpdate{
		NodeID: "node-2",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Bob"}`),
	})
	pm.Update(PresenceUpdate{
		NodeID: "node-3",
		Topic:  "docs:2",
		Data:   json.RawMessage(`{"name":"Charlie"}`),
	})

	states := pm.Get("docs:1")
	assert.Len(t, states, 2)

	states2 := pm.Get("docs:2")
	assert.Len(t, states2, 1)
	assert.Equal(t, "node-3", states2[0].NodeID)
}

func TestPresenceManager_Get_EmptyTopic(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, nil)
	defer pm.Close()

	states := pm.Get("nonexistent")
	assert.Nil(t, states)
}

func TestPresenceManager_Remove(t *testing.T) {
	var events []PresenceEvent
	var mu sync.Mutex

	pm := NewPresenceManager(30*time.Second, func(e PresenceEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})

	event := pm.Remove("docs:1", "node-1")
	require.NotNil(t, event)
	assert.Equal(t, PresenceLeave, event.Type)
	assert.Equal(t, "node-1", event.NodeID)

	states := pm.Get("docs:1")
	assert.Nil(t, states)

	mu.Lock()
	assert.Len(t, events, 2) // join + leave
	assert.Equal(t, PresenceLeave, events[1].Type)
	mu.Unlock()
}

func TestPresenceManager_Remove_Nonexistent(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, nil)
	defer pm.Close()

	event := pm.Remove("docs:1", "node-999")
	assert.Nil(t, event)
}

func TestPresenceManager_RemoveNode(t *testing.T) {
	var events []PresenceEvent
	var mu sync.Mutex

	pm := NewPresenceManager(30*time.Second, func(e PresenceEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{NodeID: "node-1", Topic: "docs:1", Data: json.RawMessage(`{}`)})
	pm.Update(PresenceUpdate{NodeID: "node-1", Topic: "docs:2", Data: json.RawMessage(`{}`)})
	pm.Update(PresenceUpdate{NodeID: "node-2", Topic: "docs:1", Data: json.RawMessage(`{}`)})

	leaveEvents := pm.RemoveNode("node-1")
	assert.Len(t, leaveEvents, 2)
	for _, e := range leaveEvents {
		assert.Equal(t, PresenceLeave, e.Type)
		assert.Equal(t, "node-1", e.NodeID)
	}

	// node-2 should still be in docs:1.
	states := pm.Get("docs:1")
	assert.Len(t, states, 1)
	assert.Equal(t, "node-2", states[0].NodeID)

	// docs:2 should be empty (cleaned up).
	states2 := pm.Get("docs:2")
	assert.Nil(t, states2)
}

func TestPresenceManager_GetTopicsForNode(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{NodeID: "node-1", Topic: "docs:1", Data: json.RawMessage(`{}`)})
	pm.Update(PresenceUpdate{NodeID: "node-1", Topic: "docs:2", Data: json.RawMessage(`{}`)})

	topics := pm.GetTopicsForNode("node-1")
	assert.Len(t, topics, 2)
	assert.Contains(t, topics, "docs:1")
	assert.Contains(t, topics, "docs:2")

	topics2 := pm.GetTopicsForNode("node-999")
	assert.Nil(t, topics2)
}

func TestPresenceManager_TTL_Expiry(t *testing.T) {
	var events []PresenceEvent
	var mu sync.Mutex

	// Very short TTL for testing.
	pm := NewPresenceManager(100*time.Millisecond, func(e PresenceEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})

	// Wait for TTL expiry + cleanup interval (cleanup runs every ttl/2 = 50ms).
	time.Sleep(300 * time.Millisecond)

	states := pm.Get("docs:1")
	assert.Empty(t, states)

	mu.Lock()
	// Should have join + leave (from TTL expiry).
	hasLeave := false
	for _, e := range events {
		if e.Type == PresenceLeave && e.NodeID == "node-1" {
			hasLeave = true
		}
	}
	assert.True(t, hasLeave, "expected a leave event from TTL expiry")
	mu.Unlock()
}

func TestPresenceManager_Heartbeat_Extends_TTL(t *testing.T) {
	pm := NewPresenceManager(200*time.Millisecond, nil, nil)
	defer pm.Close()

	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})

	// Send heartbeat before TTL expires.
	time.Sleep(100 * time.Millisecond)
	pm.Update(PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})

	// Wait past original TTL but not past the refreshed one.
	time.Sleep(150 * time.Millisecond)

	states := pm.Get("docs:1")
	assert.Len(t, states, 1, "heartbeat should have extended TTL")
}

func TestMarshalPresenceEvent(t *testing.T) {
	event := PresenceEvent{
		Type:   PresenceJoin,
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"cursor":{"x":10,"y":20}}`),
	}

	data, err := MarshalPresenceEvent(event)
	require.NoError(t, err)

	var decoded PresenceEvent
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, PresenceJoin, decoded.Type)
	assert.Equal(t, "node-1", decoded.NodeID)
	assert.JSONEq(t, `{"cursor":{"x":10,"y":20}}`, string(decoded.Data))
}

// --- SyncController Presence Integration Tests ---

func TestSyncController_PresenceDisabledByDefault(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin)

	assert.Nil(t, ctrl.Presence())
	assert.Nil(t, ctrl.PresenceChannel())
}

func TestSyncController_PresenceEnabled(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithPresenceEnabled(true),
		WithPresenceTTL(30*time.Second),
	)
	defer ctrl.Close()

	assert.NotNil(t, ctrl.Presence())
	assert.NotNil(t, ctrl.PresenceChannel())
}

func TestSyncController_HandlePresenceUpdate(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithPresenceEnabled(true),
	)
	defer ctrl.Close()

	event, err := ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, PresenceJoin, event.Type)

	// Drain the presence channel.
	select {
	case e := <-ctrl.PresenceChannel():
		assert.Equal(t, PresenceJoin, e.Type)
	case <-time.After(time.Second):
		t.Fatal("expected presence event on channel")
	}
}

func TestSyncController_HandlePresenceUpdate_Leave(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithPresenceEnabled(true),
	)
	defer ctrl.Close()

	// Join first.
	_, _ = ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})
	// Drain join event.
	<-ctrl.PresenceChannel()

	// Leave by sending null data.
	event, err := ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   nil,
	})
	require.NoError(t, err)
	assert.Equal(t, PresenceLeave, event.Type)
}

func TestSyncController_HandleGetPresence(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithPresenceEnabled(true),
	)
	defer ctrl.Close()

	_, _ = ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{"name":"Alice"}`),
	})
	<-ctrl.PresenceChannel() // drain

	snapshot, err := ctrl.HandleGetPresence(context.Background(), "docs:1")
	require.NoError(t, err)
	assert.Equal(t, "docs:1", snapshot.Topic)
	assert.Len(t, snapshot.States, 1)
	assert.Equal(t, "node-1", snapshot.States[0].NodeID)
}

func TestSyncController_HandlePresenceUpdate_Validation(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithPresenceEnabled(true),
	)
	defer ctrl.Close()

	// Missing node_id.
	_, err := ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		Topic: "docs:1",
		Data:  json.RawMessage(`{}`),
	})
	assert.Error(t, err)

	// Missing topic.
	_, err = ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Data:   json.RawMessage(`{}`),
	})
	assert.Error(t, err)
}

func TestSyncController_HandlePresenceUpdate_DisabledReturnsError(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin) // presence NOT enabled

	_, err := ctrl.HandlePresenceUpdate(context.Background(), &PresenceUpdate{
		NodeID: "node-1",
		Topic:  "docs:1",
		Data:   json.RawMessage(`{}`),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

// --- HTTP Handler Presence Tests ---

func TestHTTPHandler_PresenceUpdate(t *testing.T) {
	plugin := newTestPlugin()
	handler := NewHTTPHandler(plugin,
		WithPresenceEnabled(true),
	)

	body := `{"node_id":"node-1","topic":"docs:1","data":{"name":"Alice"}}`
	req, err := http.NewRequestWithContext(context.Background(), "POST", "/presence", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var event PresenceEvent
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &event))
	assert.Equal(t, PresenceJoin, event.Type)
	assert.Equal(t, "node-1", event.NodeID)
}

func TestHTTPHandler_GetPresence(t *testing.T) {
	plugin := newTestPlugin()
	handler := NewHTTPHandler(plugin,
		WithPresenceEnabled(true),
	)

	// First, update presence.
	body := `{"node_id":"node-1","topic":"docs:1","data":{"name":"Alice"}}`
	req, err := http.NewRequestWithContext(context.Background(), "POST", "/presence", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Now get presence.
	req2, err := http.NewRequestWithContext(context.Background(), "GET", "/presence?topic=docs:1", nil)
	require.NoError(t, err)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)

	var snapshot PresenceSnapshot
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &snapshot))
	assert.Equal(t, "docs:1", snapshot.Topic)
	assert.Len(t, snapshot.States, 1)
}

func TestHTTPHandler_GetPresence_MissingTopic(t *testing.T) {
	plugin := newTestPlugin()
	handler := NewHTTPHandler(plugin,
		WithPresenceEnabled(true),
	)

	req, err := http.NewRequestWithContext(context.Background(), "GET", "/presence", nil)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
