package crdt

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
)

func TestRegressionLocalMutationRemainsVisibleAfterFuturePush(t *testing.T) {
	server := newLateServer(t, newMemShadow())
	server.push(t, ChangeRecord{Table: "notes", PK: "remote", Field: "title", CRDTType: TypeLWW, NodeID: "peer", HLC: at(lateBase+int64(10*time.Second), "peer"), Value: json.RawMessage(`"remote"`)})
	first, err := server.ctrl.HandlePull(t.Context(), &PullRequest{Tables: []string{"notes"}})
	require.NoError(t, err)
	p := server.ctrl.plugin
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "local", map[string]string{"title": "lww"}), map[string]any{"title": "local"}, nil))
	next, err := server.ctrl.HandlePull(t.Context(), &PullRequest{Tables: []string{"notes"}, Since: first.LatestHLC})
	require.NoError(t, err)
	require.Len(t, next.Changes, 1, "the local edit was committed after the pull checkpoint")
}

func TestRegressionORMSetReplacementRemovesObservedElements(t *testing.T) {
	p, _ := newMemPlugin(t)
	qc := crdtQC(hook.OpUpdate, "1", map[string]string{"tags": "set"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"tags": []string{"a", "b"}}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"tags": []string{"b"}}, nil))
	state, err := p.metadata.ReadState(t.Context(), "notes", "1")
	require.NoError(t, err)
	elements, err := json.Marshal(SetFromFieldState(state.Fields["tags"]).Elements())
	require.NoError(t, err)
	require.JSONEq(t, `["b"]`, string(elements))
}

func TestRegressionPartialMutationPreservesOmittedLWWFields(t *testing.T) {
	server := newLateServer(t, newMemShadow())
	p := server.ctrl.plugin
	qc := crdtQC(hook.OpUpdate, "1", map[string]string{"title": "lww", "body": "lww"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"title": "first", "body": "keep"}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"title": "second"}, nil))
	state, err := p.metadata.ReadState(t.Context(), "notes", "1")
	require.NoError(t, err)
	require.JSONEq(t, `"keep"`, string(state.Fields["body"].Value))
}

func TestRegressionORMListAndDocumentMutationsProduceMetadata(t *testing.T) {
	for _, typ := range []string{"list", "document"} {
		t.Run(typ, func(t *testing.T) {
			p, _ := newMemPlugin(t)
			value := any([]string{"item"})
			if typ == "document" {
				value = map[string]any{"title": "value"}
			}
			require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "1", map[string]string{"content": typ}), map[string]any{"content": value}, nil))
			state, err := p.metadata.ReadState(t.Context(), "notes", "1")
			require.NoError(t, err)
			require.NotNil(t, state)
			require.NotNil(t, state.Fields["content"], "a supported CRDT tag must produce sync state")
		})
	}
}

func TestRegressionPresenceEnforcesConfiguredPayloadLimit(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true), WithValidation(&ValidationConfig{MaxParticipantDataSize: 5}))
	defer ctrl.Close()
	_, err := ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: "peer", Topic: "room", Data: json.RawMessage(`{"cursor":{"x":1,"y":2}}`)})
	require.Error(t, err, "the configured presence limit must apply before storing and broadcasting data")
	require.Empty(t, ctrl.Presence().Get("room"))
}

type regressionModel struct {
	ID    int64  `grove:"id,pk"`
	Title string `grove:"display_name,crdt:lww" json:"other_name"`
	Body  string `grove:"body,crdt:lww"`
}

func TestRegressionModelMetadataPreservesColumnNamesAndIntegerKeys(t *testing.T) {
	p, _ := newMemPlugin(t)
	const id int64 = 9007199254740993
	qc := &hook.QueryContext{Table: "notes", Operation: hook.OpInsert}
	require.NoError(t, p.AfterMutation(t.Context(), qc, &regressionModel{ID: id, Title: "title", Body: "keep"}, nil))
	qc.Operation = hook.OpUpdate
	qc.Columns = []string{"display_name"}
	require.NoError(t, p.AfterMutation(t.Context(), qc, &regressionModel{ID: id, Title: "changed"}, nil))
	state, err := p.Inspect(t.Context(), "notes", "9007199254740993")
	require.NoError(t, err)
	require.JSONEq(t, `"changed"`, string(state.Fields["display_name"].Value))
	require.JSONEq(t, `"keep"`, string(state.Fields["body"].Value))
	require.NotContains(t, state.Fields, "other_name")
}

func TestRegressionBulkModelMutationsRecordEveryKey(t *testing.T) {
	p, _ := newMemPlugin(t)
	qc := &hook.QueryContext{Table: "notes", Operation: hook.OpInsert}
	require.NoError(t, p.AfterMutation(t.Context(), qc, []regressionModel{{ID: 1, Title: "one"}, {ID: 2, Title: "two"}}, nil))
	for _, pk := range []string{"1", "2"} {
		state, err := p.Inspect(t.Context(), "notes", pk)
		require.NoError(t, err)
		require.NotNil(t, state.Fields["display_name"])
	}
}

func TestRegressionLocalWriteAfterRestartWinsAndRemainsPullable(t *testing.T) {
	db := newMemShadow()
	server := newLateServer(t, db)
	server.push(t, ChangeRecord{Table: "notes", PK: "1", Field: "title", CRDTType: TypeLWW, NodeID: "peer", HLC: at(lateBase+int64(10*time.Second), "peer"), Value: json.RawMessage(`"remote"`)})
	first, err := server.ctrl.HandlePull(t.Context(), &PullRequest{Tables: []string{"notes"}})
	require.NoError(t, err)
	restarted := newLateServer(t, db)
	require.NoError(t, restarted.ctrl.plugin.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, "1", map[string]string{"title": "lww"}), map[string]any{"title": "local"}, nil))
	next, err := restarted.ctrl.HandlePull(t.Context(), &PullRequest{Tables: []string{"notes"}, Since: first.LatestHLC})
	require.NoError(t, err)
	require.Len(t, next.Changes, 1)
	state, err := restarted.ctrl.plugin.Inspect(t.Context(), "notes", "1")
	require.NoError(t, err)
	require.JSONEq(t, `"local"`, string(state.Fields["title"].Value))
}

func TestRegressionLocalFieldsHaveOrderedDistinctCursors(t *testing.T) {
	p, _ := newMemPlugin(t)
	require.NoError(t, p.AfterMutation(t.Context(), crdtQC(hook.OpInsert, "1", map[string]string{"a": "lww", "b": "lww"}), map[string]any{"a": "first", "b": "second"}, nil))
	page, err := p.metadata.readChangesPage(t.Context(), "notes", HLC{}, 1)
	require.NoError(t, err)
	require.Len(t, page.changes, 1)
	next, err := p.metadata.readChangesPage(t.Context(), "notes", page.last, 1)
	require.NoError(t, err)
	require.Len(t, next.changes, 1)
	require.NotEqual(t, page.changes[0].Field, next.changes[0].Field)
}

func TestRegressionListAndDocumentModelReplacement(t *testing.T) {
	p, _ := newMemPlugin(t)
	qc := crdtQC(hook.OpUpdate, "1", map[string]string{"items": "list", "meta": "document"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"items": []int{1, 2, 3}, "meta": map[string]any{"nested": map[string]any{"title": "first"}, "gone": true}}, nil))
	before, err := p.Inspect(t.Context(), "notes", "1")
	require.NoError(t, err)
	id := ListFromFieldState(before.Fields["items"]).NodeIDs()[0]
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"items": []int{1, 4}, "meta": map[string]any{"nested": map[string]any{"title": "second"}}}, nil))
	after, err := p.Inspect(t.Context(), "notes", "1")
	require.NoError(t, err)
	list := ListFromFieldState(after.Fields["items"])
	require.Equal(t, id, list.NodeIDs()[0])
	raw, err := json.Marshal(list.Elements())
	require.NoError(t, err)
	require.JSONEq(t, `[1,4]`, string(raw))
	require.JSONEq(t, `{"nested":{"title":"second"}}`, string(after.Fields["meta"].Value))
}

func TestRegressionInvalidCollectionMutationDoesNotEraseState(t *testing.T) {
	for _, typ := range []string{"set", "list"} {
		t.Run(typ, func(t *testing.T) {
			p, db := newMemPlugin(t)
			qc := crdtQC(hook.OpUpdate, "1", map[string]string{"items": typ})
			require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"items": []int{1}}, nil))
			writes := db.writeCount()
			require.Error(t, p.AfterMutation(t.Context(), qc, map[string]any{"items": 42}, nil))
			require.Equal(t, writes, db.writeCount())
		})
	}
}

type nestedMutationPlugin struct {
	BaseCRDTPlugin
	plugin *Plugin
}

func (p *nestedMutationPlugin) Name() string { return "nested-mutation" }
func (p *nestedMutationPlugin) BeforeMerge(ctx context.Context, ev *MergeEvent) (*FieldState, error) {
	err := p.plugin.AfterMutation(ctx, crdtQC(hook.OpInsert, "nested", map[string]string{"title": "lww"}), map[string]any{"title": "from hook"}, nil)
	return ev.Remote, err
}
func TestRegressionMergeHookCanWriteAnotherModel(t *testing.T) {
	server := newLateServer(t, newMemShadow())
	server.ctrl = NewSyncController(server.ctrl.plugin, WithControllerPlugin(&nestedMutationPlugin{plugin: server.ctrl.plugin}))
	done := make(chan error, 1)
	go func() {
		_, err := server.ctrl.HandlePush(t.Context(), &PushRequest{NodeID: "peer", Changes: []ChangeRecord{{Table: "notes", PK: "remote", Field: "title", CRDTType: TypeLWW, NodeID: "peer", HLC: at(lateBase, "peer"), Value: json.RawMessage(`"remote"`)}}})
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("nested mutation deadlocked")
	}
	state, err := server.ctrl.plugin.Inspect(t.Context(), "notes", "nested")
	require.NoError(t, err)
	require.JSONEq(t, `"from hook"`, string(state.Fields["title"].Value))
}

func TestRegressionRoomAndCursorPayloadLimits(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true), WithRoomManager(true), WithValidation(&ValidationConfig{MaxParticipantDataSize: 30, MaxRoomMetadataSize: 5}))
	defer ctrl.Close()
	require.Error(t, ctrl.Rooms().JoinRoom(t.Context(), "room", "peer", json.RawMessage(`{"name":"this payload exceeds thirty bytes"}`)))
	require.Empty(t, ctrl.Presence().Get("room"))
	require.NoError(t, ctrl.Rooms().JoinRoom(t.Context(), "room", "peer", json.RawMessage(`{"name":"peer"}`)))
	ctrl.Rooms().UpdateCursor("room", "peer", CursorPosition{Field: "a very long field name"})
	require.JSONEq(t, `{"name":"peer"}`, string(ctrl.Presence().Get("room")[0].Data))
	require.Error(t, ctrl.Rooms().SetRoomMetadata("room", map[string]string{"title": "oversized"}))
}

func TestRegressionDocumentReplacementRejectsStalePaths(t *testing.T) {
	p, _ := newMemPlugin(t)
	qc := crdtQC(hook.OpUpdate, "1", map[string]string{"meta": "document"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"meta": map[string]any{"gone": true, "keep": "old"}}, nil))
	before, err := p.Inspect(t.Context(), "notes", "1")
	require.NoError(t, err)
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"meta": map[string]any{"keep": "new"}}, nil))
	after, err := p.Inspect(t.Context(), "notes", "1")
	require.NoError(t, err)
	engine := NewMergeEngine()
	for _, pair := range [][2]*FieldState{{before.Fields["meta"], after.Fields["meta"]}, {after.Fields["meta"], before.Fields["meta"]}} {
		merged, mergeErr := engine.MergeField(pair[0], pair[1])
		require.NoError(t, mergeErr)
		require.JSONEq(t, `{"keep":"new"}`, string(merged.Value))
		stale := &ChangeRecord{CRDTType: TypeDocument, HLC: before.Fields["meta"].HLC, NodeID: "peer", Value: json.RawMessage(`{"path":"gone","value":true}`)}
		merged, mergeErr = ApplyChange(engine, merged, stale)
		require.NoError(t, mergeErr)
		require.JSONEq(t, `{"keep":"new"}`, string(merged.Value))
		stale.HLC = cursorSuccessor(after.Fields["meta"].HLC)
		merged, mergeErr = ApplyChange(engine, merged, stale)
		require.NoError(t, mergeErr)
		require.JSONEq(t, `{"keep":"new","gone":true}`, string(merged.Value))
	}
}

type expandedPresencePlugin struct{ BaseCRDTPlugin }

func (*expandedPresencePlugin) Name() string { return "expanded-presence" }
func (*expandedPresencePlugin) BeforePresenceUpdate(_ context.Context, u *PresenceUpdate) (*PresenceUpdate, error) {
	copyUpdate := *u
	copyUpdate.Data = json.RawMessage(`{"name":"expanded beyond the configured limit"}`)
	return &copyUpdate, nil
}
func TestRegressionPresenceValidatesInterceptorOutput(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true), WithValidation(&ValidationConfig{MaxParticipantDataSize: 5}), WithControllerPlugin(&expandedPresencePlugin{}))
	defer ctrl.Close()
	_, err := ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: "peer", Topic: "room", Data: json.RawMessage(`{}`)})
	require.Error(t, err)
	require.Empty(t, ctrl.Presence().Get("room"))
}

func TestRegressionModelPointerKeysAndTypedCounters(t *testing.T) {
	type count uint16
	type model struct {
		ID    *string `grove:"id,pk"`
		Count count   `grove:"count,crdt:counter"`
	}
	p, _ := newMemPlugin(t)
	id := "record"
	qc := &hook.QueryContext{Table: "notes", Operation: hook.OpUpdate}
	require.NoError(t, p.AfterMutation(t.Context(), qc, &model{ID: &id, Count: 7}, nil))
	state, err := p.Inspect(t.Context(), "notes", id)
	require.NoError(t, err)
	require.Equal(t, int64(7), CounterFromFieldState(state.Fields["count"]).Value())
	require.Error(t, p.AfterMutation(t.Context(), qc, &model{Count: 7}, nil))
	for _, invalid := range []any{float64(1.5), uint64(1 << 63), "no"} {
		require.Error(t, p.AfterMutation(t.Context(), crdtQC(hook.OpUpdate, id, map[string]string{"count": "counter"}), map[string]any{"count": invalid}, nil))
	}
	state, err = p.Inspect(t.Context(), "notes", id)
	require.NoError(t, err)
	require.Equal(t, int64(7), CounterFromFieldState(state.Fields["count"]).Value())
}

func TestRegressionPresenceLimitAllowsLeaving(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true), WithValidation(&ValidationConfig{MaxParticipantDataSize: 1}))
	defer ctrl.Close()
	_, err := ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: "peer", Topic: "room", Data: json.RawMessage(`0`)})
	require.NoError(t, err)
	_, err = ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: "peer", Topic: "room", Data: json.RawMessage(`null`)})
	require.NoError(t, err)
	require.Empty(t, ctrl.Presence().Get("room"))
}
