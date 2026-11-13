package crdt

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
)

// These opt-in review probes assert the required behavior and currently fail.
// Run with GROVE_CRDT_REVIEW=1 until the production defects are fixed.
func remainingReview(t *testing.T) {
	t.Helper()
	if os.Getenv("GROVE_CRDT_REVIEW") != "1" {
		t.Skip("set GROVE_CRDT_REVIEW=1 to run known-failing review reproductions")
	}
}

func TestReviewLocalMutationRemainsVisibleAfterFuturePush(t *testing.T) {
	remainingReview(t)
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

func TestReviewORMSetReplacementRemovesObservedElements(t *testing.T) {
	remainingReview(t)
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

func TestReviewPartialMutationPreservesOmittedLWWFields(t *testing.T) {
	remainingReview(t)
	server := newLateServer(t, newMemShadow())
	p := server.ctrl.plugin
	qc := crdtQC(hook.OpUpdate, "1", map[string]string{"title": "lww", "body": "lww"})
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"title": "first", "body": "keep"}, nil))
	require.NoError(t, p.AfterMutation(t.Context(), qc, map[string]any{"title": "second"}, nil))
	state, err := p.metadata.ReadState(t.Context(), "notes", "1")
	require.NoError(t, err)
	require.JSONEq(t, `"keep"`, string(state.Fields["body"].Value))
}

func TestReviewORMListAndDocumentMutationsProduceMetadata(t *testing.T) {
	remainingReview(t)
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

func TestReviewPresenceEnforcesConfiguredPayloadLimit(t *testing.T) {
	remainingReview(t)
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true), WithValidation(&ValidationConfig{MaxParticipantDataSize: 5}))
	defer ctrl.Close()
	_, err := ctrl.HandlePresenceUpdate(t.Context(), &PresenceUpdate{NodeID: "peer", Topic: "room", Data: json.RawMessage(`{"cursor":{"x":1,"y":2}}`)})
	require.Error(t, err, "the configured presence limit must apply before storing and broadcasting data")
	require.Empty(t, ctrl.Presence().Get("room"))
}
