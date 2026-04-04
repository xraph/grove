package crdt

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	log "github.com/xraph/go-utils/log"
)

func TestRoomManager_CreateRoom(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())

	room, err := rm.CreateRoom(context.Background(), "room-1", "document")
	if err != nil {
		t.Fatal(err)
	}
	if room.ID != "room-1" {
		t.Errorf("expected room-1, got %s", room.ID)
	}
	if room.Type != "document" {
		t.Errorf("expected type=document, got %s", room.Type)
	}
}

func TestRoomManager_CreateRoom_Idempotent(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())

	r1, _ := rm.CreateRoom(context.Background(), "room-1", "document")
	r2, _ := rm.CreateRoom(context.Background(), "room-1", "channel")

	// Should return the same room.
	if r1.ID != r2.ID {
		t.Error("expected same room")
	}
	// Type should not change.
	if r2.Type != "document" {
		t.Errorf("expected type to remain document, got %s", r2.Type)
	}
}

func TestRoomManager_JoinAndLeave(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	data, _ := json.Marshal(ParticipantData{Name: "Alice", Color: "#ff0000"})
	err := rm.JoinRoom(ctx, "room-1", "node-1", data)
	if err != nil {
		t.Fatal(err)
	}

	if rm.ParticipantCount("room-1") != 1 {
		t.Errorf("expected 1 participant, got %d", rm.ParticipantCount("room-1"))
	}

	rm.LeaveRoom(ctx, "room-1", "node-1")

	if rm.ParticipantCount("room-1") != 0 {
		t.Errorf("expected 0 participants after leave, got %d", rm.ParticipantCount("room-1"))
	}

	// Room should be destroyed after last participant leaves.
	if rm.GetRoom("room-1") != nil {
		t.Error("expected room to be destroyed after last participant left")
	}
}

func TestRoomManager_MaxParticipants(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	_, _ = rm.CreateRoom(ctx, "small-room", "channel", WithMaxParticipants(2))

	_ = rm.JoinRoom(ctx, "small-room", "node-1", nil)
	_ = rm.JoinRoom(ctx, "small-room", "node-2", nil)
	err := rm.JoinRoom(ctx, "small-room", "node-3", nil)

	if err == nil {
		t.Error("expected error when room is full")
	}
}

func TestRoomManager_ListRooms(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	_, _ = rm.CreateRoom(ctx, "doc-1", "document")
	_, _ = rm.CreateRoom(ctx, "doc-2", "document")
	_, _ = rm.CreateRoom(ctx, "chat-1", "channel")

	all := rm.ListRooms()
	if len(all) != 3 {
		t.Errorf("expected 3 rooms, got %d", len(all))
	}

	docs := rm.ListRoomsByType("document")
	if len(docs) != 2 {
		t.Errorf("expected 2 document rooms, got %d", len(docs))
	}

	channels := rm.ListRoomsByType("channel")
	if len(channels) != 1 {
		t.Errorf("expected 1 channel room, got %d", len(channels))
	}
}

func TestRoomManager_GetRoomInfo(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	_, _ = rm.CreateRoom(ctx, "room-1", "document", WithRoomMetadata(map[string]string{"title": "My Doc"}))

	data1, _ := json.Marshal(ParticipantData{Name: "Alice"})
	data2, _ := json.Marshal(ParticipantData{Name: "Bob"})
	_ = rm.JoinRoom(ctx, "room-1", "node-1", data1)
	_ = rm.JoinRoom(ctx, "room-1", "node-2", data2)

	info := rm.GetRoomInfo("room-1")
	if info == nil {
		t.Fatal("expected room info")
	}
	if info.ParticipantCount != 2 {
		t.Errorf("expected 2 participants, got %d", info.ParticipantCount)
	}
	if len(info.Participants) != 2 {
		t.Errorf("expected 2 participant states, got %d", len(info.Participants))
	}
}

func TestRoomManager_UpdateCursor(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	data, _ := json.Marshal(ParticipantData{Name: "Alice"})
	_ = rm.JoinRoom(ctx, "room-1", "node-1", data)

	rm.UpdateCursor("room-1", "node-1", CursorPosition{Line: 10, Column: 5})

	states := rm.presence.Get("room-1")
	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}

	var pd ParticipantData
	_ = json.Unmarshal(states[0].Data, &pd)
	if pd.Cursor == nil {
		t.Fatal("expected cursor to be set")
	}
	if pd.Cursor.Line != 10 || pd.Cursor.Column != 5 {
		t.Errorf("expected cursor at line 10, col 5, got %+v", pd.Cursor)
	}
	if pd.Name != "Alice" {
		t.Errorf("expected name to be preserved, got %s", pd.Name)
	}
}

func TestRoomManager_UpdateTypingStatus(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	data, _ := json.Marshal(ParticipantData{Name: "Bob"})
	_ = rm.JoinRoom(ctx, "room-1", "node-1", data)

	rm.UpdateTypingStatus("room-1", "node-1", true)

	states := rm.presence.Get("room-1")
	var pd ParticipantData
	_ = json.Unmarshal(states[0].Data, &pd)
	if !pd.IsTyping {
		t.Error("expected is_typing=true")
	}
}

func TestRoomManager_DocumentRoom(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	room, err := rm.CreateDocumentRoom(ctx, "documents", "doc-123")
	if err != nil {
		t.Fatal(err)
	}
	if room.ID != "documents:doc-123" {
		t.Errorf("expected documents:doc-123, got %s", room.ID)
	}
	if room.Type != "document" {
		t.Errorf("expected type=document, got %s", room.Type)
	}

	data, _ := json.Marshal(ParticipantData{Name: "Alice"})
	err = rm.JoinDocumentRoom(ctx, "documents", "doc-123", "node-1", data)
	if err != nil {
		t.Fatal(err)
	}

	participants := rm.GetDocumentParticipants("documents", "doc-123")
	if len(participants) != 1 {
		t.Errorf("expected 1 participant, got %d", len(participants))
	}

	rm.LeaveDocumentRoom(ctx, "documents", "doc-123", "node-1")
	if rm.ParticipantCount("documents:doc-123") != 0 {
		t.Error("expected 0 participants after leave")
	}
}

func TestRoomManager_LeaveAllRooms(t *testing.T) {
	pm := NewPresenceManager(30*time.Second, nil, log.NewNoopLogger())
	defer pm.Close()

	rm := NewRoomManager(pm, log.NewNoopLogger())
	ctx := context.Background()

	_ = rm.JoinRoom(ctx, "room-1", "node-1", nil)
	_ = rm.JoinRoom(ctx, "room-2", "node-1", nil)
	_ = rm.JoinRoom(ctx, "room-3", "node-1", nil)

	rm.LeaveAllRooms(ctx, "node-1")

	if rm.ParticipantCount("room-1") != 0 {
		t.Error("expected 0 in room-1")
	}
	if rm.ParticipantCount("room-2") != 0 {
		t.Error("expected 0 in room-2")
	}
	if rm.ParticipantCount("room-3") != 0 {
		t.Error("expected 0 in room-3")
	}
}

func TestDocumentRoomID(t *testing.T) {
	if DocumentRoomID("documents", "doc-1") != "documents:doc-1" {
		t.Error("unexpected document room ID format")
	}
}
