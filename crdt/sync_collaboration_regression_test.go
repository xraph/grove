package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type regressionRetryTransport struct {
	remote   *SyncController
	attempts int
}

func (r *regressionRetryTransport) Pull(ctx context.Context, req *PullRequest) (*PullResponse, error) {
	return r.remote.HandlePull(ctx, req)
}
func (r *regressionRetryTransport) Push(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	r.attempts++
	if r.attempts == 1 {
		return nil, errors.New("temporary network failure")
	}
	return r.remote.HandlePush(ctx, req)
}

func TestRegressionRetryDoesNotLoseLocalPush(t *testing.T) {
	ctx := context.Background()
	local := newLateServer(t, newMemShadow())
	remote := newLateServer(t, newMemShadow())
	remote.now.Add(int64(time.Second))
	local.push(t, ChangeRecord{Table: "notes", PK: "local", Field: "title", CRDTType: TypeLWW, NodeID: "server", HLC: at(lateBase, "server"), Value: json.RawMessage(`"local edit"`)})
	remote.push(t, ChangeRecord{Table: "notes", PK: "remote", Field: "title", CRDTType: TypeLWW, NodeID: "other", HLC: at(lateBase+int64(time.Second), "other"), Value: json.RawMessage(`"remote edit"`)})
	tr := &regressionRetryTransport{remote: remote.ctrl}
	s := NewSyncer(local.ctrl.plugin, WithSyncTables("notes"))
	_, err := s.syncWithPeer(ctx, tr, "peer")
	require.ErrorContains(t, err, "temporary network failure")
	_, err = s.syncWithPeer(ctx, tr, "peer")
	require.NoError(t, err)
	require.Equal(t, 2, tr.attempts, "the failed local edit must be retried")
}

type regressionFailWrite struct {
	*memShadow
	fail bool
}

func (w *regressionFailWrite) ExecContext(ctx context.Context, q string, args ...any) (ExecResult, error) {
	if w.fail {
		return nil, errors.New("temporary disk failure")
	}
	return w.memShadow.ExecContext(ctx, q, args...)
}
func TestRegressionMergeFailureDoesNotAdvancePullCursor(t *testing.T) {
	ctx := context.Background()
	remote := newLateServer(t, newMemShadow())
	remote.push(t, ChangeRecord{Table: "notes", PK: "remote", Field: "title", CRDTType: TypeLWW, NodeID: "other", HLC: at(lateBase, "other"), Value: json.RawMessage(`"must survive"`)})
	db := &regressionFailWrite{memShadow: newMemShadow(), fail: true}
	p := New(WithNodeID("local"))
	p.SetExecutor(db)
	s := NewSyncer(p, WithSyncTables("notes"))
	tr := controllerTransport{remote.ctrl}
	_, _ = s.syncWithPeer(ctx, tr, "peer")
	db.fail = false
	_, err := s.syncWithPeer(ctx, tr, "peer")
	require.NoError(t, err)
	rows, err := p.metadata.ReadChangesSince(ctx, "notes", HLC{})
	require.NoError(t, err)
	require.Len(t, rows, 1, "a transient merge failure must be retried")
}

func TestRegressionRoomCapacityCannotBeBypassedByCursor(t *testing.T) {
	ctx := context.Background()
	ctrl := NewSyncController(newTestPlugin(), WithRoomManager(true))
	defer ctrl.Close()
	rooms := ctrl.Rooms()
	_, err := rooms.CreateRoom(ctx, "room", "document", WithMaxParticipants(1))
	require.NoError(t, err)
	require.NoError(t, rooms.JoinRoom(ctx, "room", "alice", json.RawMessage(`{}`)))
	require.Error(t, rooms.JoinRoom(ctx, "room", "bob", json.RawMessage(`{}`)))
	_, err = ctrl.HandlePresenceUpdate(ctx, &PresenceUpdate{NodeID: "bob", Topic: "room", Data: json.RawMessage(`{"cursor":{"x":1,"y":2}}`)})
	require.Error(t, err)
	require.Len(t, ctrl.Presence().Get("room"), 1, "cursor publication must not admit a rejected room participant")
}

func TestRegressionSyncReportsTotalFailure(t *testing.T) {
	s := NewSyncer(newTestPlugin(), WithTransport(&recordingTransport{pullErr: errors.New("offline")}), WithRetry(0, 0, 0))
	_, err := s.Sync(context.Background())
	require.Error(t, err, "all peers failed; callers must not see success")
}

type regressionRoomHook struct{ rm *RoomManager }

func (h regressionRoomHook) OnRoomCreated(_ context.Context, r *Room) error {
	h.rm.GetRoom(r.ID)
	return nil
}
func (h regressionRoomHook) OnRoomDestroyed(context.Context, *Room) error            { return nil }
func (h regressionRoomHook) OnParticipantJoin(context.Context, *Room, string) error  { return nil }
func (h regressionRoomHook) OnParticipantLeave(context.Context, *Room, string) error { return nil }
func TestRegressionRoomCreationHookCanReadRoom(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	defer pm.Close()
	rm := NewRoomManager(pm, nil)
	rm.AddHook(regressionRoomHook{rm})
	done := make(chan struct{})
	go func() { _, _ = rm.CreateRoom(context.Background(), "r", "document"); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("room creation deadlocks when its hook calls GetRoom")
	}
}
func TestRegressionRoomMetadataReadRace(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	defer pm.Close()
	rm := NewRoomManager(pm, nil)
	_, err := rm.CreateRoom(context.Background(), "r", "document")
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = rm.SetRoomMetadata("r", map[string]any{"x": 1})
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			rm.GetRoomInfo("r")
		}
	}()
	wg.Wait()
}

func TestRegressionUnseenSetRemoveIsOrderIndependent(t *testing.T) {
	add := ChangeRecord{Table: "notes", PK: "1", Field: "tags", CRDTType: TypeSet, NodeID: "a", HLC: at(1000, "a"), SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)}}
	remove := ChangeRecord{Table: "notes", PK: "1", Field: "tags", CRDTType: TypeSet, NodeID: "b", HLC: at(1001, "b"), SetOp: &SetOperation{Op: SetOpRemove, Elements: json.RawMessage(`["x"]`), Tags: []Tag{}}}
	a, err := ApplyChange(nil, nil, &add)
	require.NoError(t, err)
	a, err = ApplyChange(nil, a, &remove)
	require.NoError(t, err)
	b, err := ApplyChange(nil, nil, &remove)
	require.NoError(t, err)
	b, err = ApplyChange(nil, b, &add)
	require.NoError(t, err)
	require.Equal(t, SetFromFieldState(a).Elements(), SetFromFieldState(b).Elements())
}

func TestRegressionSetTagWireDistinguishesEmptyFromLegacy(t *testing.T) {
	for _, tags := range [][]Tag{nil, {}} {
		op := SetOperation{Op: SetOpRemove, Elements: json.RawMessage(`["x"]`), Tags: tags}
		raw, err := json.Marshal(op)
		require.NoError(t, err)
		var decoded SetOperation
		require.NoError(t, json.Unmarshal(raw, &decoded))
		require.Equal(t, tags == nil, decoded.Tags == nil, string(raw))
		add := ChangeRecord{CRDTType: TypeSet, NodeID: "a", HLC: at(1000, "a"), SetOp: &SetOperation{Op: SetOpAdd, Elements: op.Elements}}
		state, err := ApplyChange(nil, nil, &add)
		require.NoError(t, err)
		remove := ChangeRecord{CRDTType: TypeSet, NodeID: "b", HLC: at(1001, "b"), SetOp: &decoded}
		state, err = ApplyChange(nil, state, &remove)
		require.NoError(t, err)
		if tags == nil {
			require.Empty(t, SetFromFieldState(state).Elements())
		} else {
			require.Len(t, SetFromFieldState(state).Elements(), 1)
		}
	}
}

type regressionPagedTransport struct {
	pushed []ChangeRecord
	failPK string
	failed bool
}

func (tr *regressionPagedTransport) Pull(context.Context, *PullRequest) (*PullResponse, error) {
	return &PullResponse{}, nil
}
func (tr *regressionPagedTransport) Push(_ context.Context, req *PushRequest) (*PushResponse, error) {
	for _, c := range req.Changes {
		if c.PK == tr.failPK && !tr.failed {
			tr.failed = true
			return nil, errors.New("retry page")
		}
	}
	tr.pushed = append(tr.pushed, req.Changes...)
	return &PushResponse{Merged: len(req.Changes)}, nil
}

func TestRegressionOutboundPagesUseIndependentTableCursors(t *testing.T) {
	ctx := context.Background()
	p := New(WithNodeID("local"))
	p.SetExecutor(newMemShadow())
	// Every row has an authored HLC behind its shadow cursor. Advancing from
	// the change's clock would repeatedly read the same page forever.
	for i := range DefaultChangesLimit + 3 {
		require.NoError(t, p.metadata.WriteFieldStateAt(ctx, "notes", fmt.Sprint(i), "title",
			&FieldState{Type: TypeLWW, NodeID: "local", HLC: at(10, "local"), Value: json.RawMessage(`"value"`)}, at(int64(100+i), "local")))
	}
	require.NoError(t, p.metadata.WriteFieldStateAt(ctx, "tasks", "task", "title",
		&FieldState{Type: TypeLWW, NodeID: "local", HLC: at(5, "local"), Value: json.RawMessage(`"task"`)}, at(20, "local")))
	tr := &regressionPagedTransport{failPK: fmt.Sprint(DefaultChangesLimit)}
	s := NewSyncer(p, WithSyncTables("notes", "tasks"))
	r, err := s.syncWithPeer(ctx, tr, "peer")
	require.ErrorContains(t, err, "retry page")
	require.Equal(t, DefaultChangesLimit, r.Pushed)
	r, err = s.syncWithPeer(ctx, tr, "peer")
	require.NoError(t, err)
	require.Equal(t, 4, r.Pushed)
	require.Len(t, tr.pushed, DefaultChangesLimit+4)
	r, err = s.syncWithPeer(ctx, tr, "peer")
	require.NoError(t, err)
	require.Zero(t, r.Pushed)
}

func TestRegressionSyncReportsFailureAndSuccessfulPeers(t *testing.T) {
	offline := errors.New("offline")
	healthy := &recordingTransport{}
	s := NewSyncer(newTestPlugin(), WithPeers(&recordingTransport{pullErr: offline}, healthy), WithRetry(0, 0, 0))
	report, err := s.Sync(t.Context())
	require.ErrorIs(t, err, offline)
	require.NotNil(t, report)
	require.Equal(t, 1, healthy.pullCount())
}

func TestRegressionRoomSnapshotsCannotMutateManager(t *testing.T) {
	pm := NewPresenceManager(time.Minute, nil, nil)
	defer pm.Close()
	rm := NewRoomManager(pm, nil)
	room, err := rm.CreateRoom(t.Context(), "r", "document", WithMaxParticipants(1), WithRoomMetadata(map[string]any{"x": 1}))
	require.NoError(t, err)
	listed := rm.ListRooms()
	snapshots := []*Room{room, rm.GetRoom("r"), &rm.GetRoomInfo("r").Room, &listed[0].Room}
	for _, snapshot := range snapshots {
		snapshot.MaxParticipants = 100
		snapshot.Metadata[0] = '!'
	}
	require.Equal(t, 1, rm.GetRoom("r").MaxParticipants)
	require.JSONEq(t, `{"x":1}`, string(rm.GetRoom("r").Metadata))
	require.NoError(t, rm.JoinRoom(t.Context(), "r", "alice", json.RawMessage(`{}`)))
	rm.UpdateCursor("r", "bob", CursorPosition{})
	rm.UpdateTypingStatus("r", "bob", true)
	require.Len(t, pm.Get("r"), 1)
	// An admitted participant still has permission to update a full room.
	rm.UpdateTypingStatus("r", "alice", true)
	require.Contains(t, string(pm.Get("r")[0].Data), `"is_typing":true`)
}

func TestRegressionCapacitySharedByConcurrentAdmissionPaths(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithRoomManager(true))
	defer ctrl.Close()
	_, err := ctrl.Rooms().CreateRoom(t.Context(), "room", "document", WithMaxParticipants(3))
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := fmt.Sprint(i)
			switch i % 3 {
			case 0:
				_, _ = ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: node, Topic: "room", Data: json.RawMessage(`{}`)})
			case 1:
				_ = ctrl.Rooms().JoinRoom(t.Context(), "room", node, json.RawMessage(`{}`))
			case 2:
				ctrl.Rooms().UpdateCursor("room", node, CursorPosition{})
			}
		}()
	}
	wg.Wait()
	require.Len(t, ctrl.Presence().Get("room"), 3)
}
