package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingRoomHook records every lifecycle callback it receives.
type recordingRoomHook struct {
	mu     sync.Mutex
	events []string
	err    error
}

func (h *recordingRoomHook) add(ev string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)

	return h.err
}

func (h *recordingRoomHook) OnRoomCreated(_ context.Context, r *Room) error {
	return h.add("created:" + r.ID)
}

func (h *recordingRoomHook) OnRoomDestroyed(_ context.Context, r *Room) error {
	return h.add("destroyed:" + r.ID)
}

func (h *recordingRoomHook) OnParticipantJoin(_ context.Context, r *Room, node string) error {
	return h.add("join:" + r.ID + ":" + node)
}

func (h *recordingRoomHook) OnParticipantLeave(_ context.Context, r *Room, node string) error {
	return h.add("leave:" + r.ID + ":" + node)
}

func (h *recordingRoomHook) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.events...)
}

func newTestRoomManager(t *testing.T) *RoomManager {
	t.Helper()
	pm := NewPresenceManager(time.Minute, nil, nil)
	t.Cleanup(pm.Close)

	return NewRoomManager(pm, nil)
}

func TestRoomManager_HooksFireInLifecycleOrder(t *testing.T) {
	for _, hookErr := range []error{nil, errors.New("hook failed")} {
		rm := newTestRoomManager(t)
		h := &recordingRoomHook{err: hookErr}
		rm.AddHook(h)
		ctx := t.Context()

		_, err := rm.CreateRoom(ctx, "r1", "canvas", WithRoomCreator("alice"), WithRoomMetadata(map[string]string{"title": "T"}))
		require.NoError(t, err)
		require.NoError(t, rm.JoinRoom(ctx, "r1", "alice", nil))
		require.NoError(t, rm.JoinRoom(ctx, "r1", "bob", nil))
		rm.LeaveRoom(ctx, "r1", "alice")
		assert.NotNil(t, rm.GetRoom("r1"), "room survives while bob is in it")
		rm.LeaveRoom(ctx, "r1", "bob")
		assert.Nil(t, rm.GetRoom("r1"), "the last leave destroys the room")
		// Leaving a room that no longer exists is a no-op.
		rm.LeaveRoom(ctx, "r1", "bob")

		// A failing hook is logged and never blocks the lifecycle.
		assert.Equal(t, []string{
			"created:r1",
			"join:r1:alice",
			"join:r1:bob",
			"leave:r1:alice",
			"leave:r1:bob",
			"destroyed:r1",
		}, h.list(), "hook error %v", hookErr)
		rm.Close()
	}
}

func TestRoomManager_CreateRoomAppliesOptions(t *testing.T) {
	rm := newTestRoomManager(t)

	room, err := rm.CreateRoom(t.Context(), "r", "doc", WithRoomCreator("alice"), WithMaxParticipants(3), WithRoomMetadata(map[string]int{"v": 1}))
	require.NoError(t, err)

	assert.Equal(t, "alice", room.CreatedBy)
	assert.Equal(t, 3, room.MaxParticipants)
	assert.JSONEq(t, `{"v":1}`, string(room.Metadata))
	assert.False(t, room.CreatedAt.IsZero())
}

func TestRoomManager_SetRoomMetadata(t *testing.T) {
	rm := newTestRoomManager(t)
	_, err := rm.CreateRoom(t.Context(), "r", "doc")
	require.NoError(t, err)

	require.NoError(t, rm.SetRoomMetadata("r", map[string]string{"title": "Plan"}))
	assert.JSONEq(t, `{"title":"Plan"}`, string(rm.GetRoom("r").Metadata))

	require.ErrorContains(t, rm.SetRoomMetadata("missing", 1), "room missing not found")
	require.ErrorContains(t, rm.SetRoomMetadata("r", make(chan int)), "marshal metadata")
}

// --- RoomHTTPHandler ---

func roomRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body == "" {
		req = httptest.NewRequestWithContext(t.Context(), method, path, nil)
	} else {
		req = httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()

	var v T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v), rec.Body.String())

	return v
}

func TestRoomHTTPHandler_CreateGetAndList(t *testing.T) {
	rm := newTestRoomManager(t)
	h := RoomHTTPHandler(rm)

	rec := roomRequest(t, h, http.MethodPost, "/rooms",
		`{"id":"r1","type":"doc","metadata":{"title":"A"},"max_participants":2,"created_by":"alice"}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	created := decodeBody[Room](t, rec)
	assert.Equal(t, "r1", created.ID)
	assert.Equal(t, "doc", created.Type)
	assert.Equal(t, 2, created.MaxParticipants)
	assert.Equal(t, "alice", created.CreatedBy)
	assert.JSONEq(t, `{"title":"A"}`, string(created.Metadata))

	_, err := rm.CreateRoom(t.Context(), "r2", "canvas")
	require.NoError(t, err)

	rec = roomRequest(t, h, http.MethodGet, "/rooms", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody[[]RoomInfo](t, rec), 2)

	rec = roomRequest(t, h, http.MethodGet, "/rooms?type=canvas", "")
	require.Equal(t, http.StatusOK, rec.Code)
	byType := decodeBody[[]RoomInfo](t, rec)
	require.Len(t, byType, 1)
	assert.Equal(t, "r2", byType[0].ID)

	rec = roomRequest(t, h, http.MethodGet, "/rooms/r1", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "r1", decodeBody[RoomInfo](t, rec).ID)

	rec = roomRequest(t, h, http.MethodGet, "/rooms/nope", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "room not found")
}

func TestRoomHTTPHandler_CreateValidation(t *testing.T) {
	h := RoomHTTPHandler(newTestRoomManager(t))

	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms", `{bad`).Code)

	rec := roomRequest(t, h, http.MethodPost, "/rooms", `{"type":"doc"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "id is required")
}

func TestRoomHTTPHandler_JoinLeaveAndCapacity(t *testing.T) {
	rm := newTestRoomManager(t)
	h := RoomHTTPHandler(rm)
	_, err := rm.CreateRoom(t.Context(), "r", "doc", WithMaxParticipants(1))
	require.NoError(t, err)

	rec := roomRequest(t, h, http.MethodPost, "/rooms/r/join", `{"node_id":"alice","data":{"name":"Alice"}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	info := decodeBody[RoomInfo](t, rec)
	assert.Equal(t, 1, info.ParticipantCount)
	require.Len(t, info.Participants, 1)
	assert.Equal(t, "alice", info.Participants[0].NodeID)

	rec = roomRequest(t, h, http.MethodPost, "/rooms/r/join", `{"node_id":"bob"}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "is full (1/1)")

	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/join", `{}`).Code)
	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/join", `nope`).Code)

	rec = roomRequest(t, h, http.MethodGet, "/rooms/r/participants", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decodeBody[[]PresenceState](t, rec), 1)

	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/leave", `{}`).Code)
	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/leave", `nope`).Code)
	assert.Equal(t, http.StatusNoContent, roomRequest(t, h, http.MethodPost, "/rooms/r/leave", `{"node_id":"alice"}`).Code)
	assert.Nil(t, rm.GetRoom("r"))

	// An unknown room has an empty participant list, not null.
	rec = roomRequest(t, h, http.MethodGet, "/rooms/r/participants", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `[]`, rec.Body.String())
}

func TestRoomHTTPHandler_CursorTypingAndMetadata(t *testing.T) {
	rm := newTestRoomManager(t)
	h := RoomHTTPHandler(rm)
	require.NoError(t, rm.JoinRoom(t.Context(), "r", "alice", json.RawMessage(`{"name":"Alice"}`)))

	rec := roomRequest(t, h, http.MethodPost, "/rooms/r/cursor", `{"node_id":"alice","cursor":{"line":3,"column":7}}`)
	require.Equal(t, http.StatusNoContent, rec.Code)
	rec = roomRequest(t, h, http.MethodPost, "/rooms/r/typing", `{"node_id":"alice","is_typing":true}`)
	require.Equal(t, http.StatusNoContent, rec.Code)

	states := rm.presence.Get("r")
	require.Len(t, states, 1)
	var pd ParticipantData
	require.NoError(t, json.Unmarshal(states[0].Data, &pd))
	assert.Equal(t, "Alice", pd.Name, "earlier presence data is kept")
	require.NotNil(t, pd.Cursor)
	assert.Equal(t, 3, pd.Cursor.Line)
	assert.Equal(t, 7, pd.Cursor.Column)
	assert.True(t, pd.IsTyping)

	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/cursor", `x`).Code)
	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPost, "/rooms/r/typing", `x`).Code)

	rec = roomRequest(t, h, http.MethodPut, "/rooms/r/metadata", `{"title":"New"}`)
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.JSONEq(t, `{"title":"New"}`, string(rm.GetRoom("r").Metadata))

	assert.Equal(t, http.StatusNotFound, roomRequest(t, h, http.MethodPut, "/rooms/missing/metadata", `{}`).Code)
	assert.Equal(t, http.StatusBadRequest, roomRequest(t, h, http.MethodPut, "/rooms/r/metadata", `{`).Code)
}
