package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serve(t *testing.T, h http.HandlerFunc, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	var req *http.Request
	if body == "" {
		req = httptest.NewRequestWithContext(t.Context(), method, target, nil)
	} else {
		req = httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	h(rec, req)

	return rec
}

// historyController is a time-travel controller over memShadow with a title
// written at ts=100 ("v1") and again at ts=200 ("v2") by another node.
func historyController(t *testing.T) *SyncController {
	t.Helper()
	p, _ := newMemPlugin(t)
	ms := p.MetadataStore()
	ctx := t.Context()
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "1", "title",
		&FieldState{Type: TypeLWW, HLC: HLC{Timestamp: 100, NodeID: "a"}, NodeID: "a", Value: json.RawMessage(`"v1"`)}))
	require.NoError(t, ms.WriteFieldState(ctx, "notes", "1", "title",
		&FieldState{Type: TypeLWW, HLC: HLC{Timestamp: 200, NodeID: "b"}, NodeID: "b", Value: json.RawMessage(`"v2"`)}))

	return NewSyncController(p, WithTimeTravelEnabled(true))
}

func TestHTTPHistory_GetAtPointInTime(t *testing.T) {
	ctrl := historyController(t)

	rec := serve(t, ctrl.httpHandleHistory, http.MethodGet, "/history?table=notes&pk=1&at_ts=150&at_c=0&at_node=z", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	resp := decodeBody[HistoryResponse](t, rec)
	assert.Equal(t, int64(150), resp.AtHLC.Timestamp)
	assert.Equal(t, "z", resp.AtHLC.NodeID)
	require.NotNil(t, resp.State.Fields["title"])
	assert.JSONEq(t, `"v1"`, string(resp.State.Fields["title"].Value), "the ts=200 write is after the cut")

	rec = serve(t, ctrl.httpHandleHistory, http.MethodGet, "/history?table=notes&pk=1", "")
	require.Equal(t, http.StatusOK, rec.Code)
	resp = decodeBody[HistoryResponse](t, rec)
	assert.JSONEq(t, `"v2"`, string(resp.State.Fields["title"].Value), "no cut means the current state")

	// Unparseable numbers are ignored, leaving the current state.
	rec = serve(t, ctrl.httpHandleHistory, http.MethodGet, "/history?table=notes&pk=1&at_ts=x&at_c=y", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, decodeBody[HistoryResponse](t, rec).AtHLC.IsZero())
}

func TestHTTPHistory_PostAndErrors(t *testing.T) {
	ctrl := historyController(t)

	rec := serve(t, ctrl.httpHandleHistory, http.MethodPost, "/history", `{"table":"notes","pk":"1","at_hlc":{"ts":120,"c":0,"node":"a"}}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "notes", decodeBody[HistoryResponse](t, rec).Table)

	rec = serve(t, ctrl.httpHandleHistory, http.MethodGet, "/history?table=notes", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "table and pk are required")

	assert.Equal(t, http.StatusBadRequest, serve(t, ctrl.httpHandleHistory, http.MethodPost, "/history", `{`).Code)

	// A plugin that denies history reads turns into a 500 on both verbs.
	ctrl.AddPlugin(&mockTimeTravelPlugin{denyHistory: true})
	assert.Equal(t, http.StatusInternalServerError, serve(t, ctrl.httpHandleHistory, http.MethodGet, "/history?table=notes&pk=1", "").Code)
	assert.Equal(t, http.StatusInternalServerError, serve(t, ctrl.httpHandleHistory, http.MethodPost, "/history", `{"table":"notes","pk":"1"}`).Code)
}

func TestHandleHistory_PluginRedactsAndFails(t *testing.T) {
	ctrl := historyController(t)
	ctrl.AddPlugin(&tracePlugin{name: "r", redact: true})

	resp, err := ctrl.HandleHistory(t.Context(), &HistoryRequest{Table: "notes", PK: "1"})
	require.NoError(t, err)
	assert.Empty(t, resp.State.Fields)

	failing := historyController(t)
	failing.AddPlugin(&tracePlugin{name: "f", readErr: errors.New("nope")})
	_, err = failing.HandleHistory(t.Context(), &HistoryRequest{Table: "notes", PK: "1"})
	require.ErrorContains(t, err, "history read plugin")
}

func fieldHistoryController(t *testing.T, queryErr error) (*SyncController, *[]any) {
	t.Helper()
	state, err := json.Marshal(&FieldState{Type: TypeLWW, Value: json.RawMessage(`"hi"`)})
	require.NoError(t, err)

	var lastArgs []any
	p := New(WithNodeID("test"))
	p.SetExecutor(&mockExecutor{queryFn: func(_ context.Context, _ string, args ...any) (Rows, error) {
		lastArgs = args
		if queryErr != nil {
			return nil, queryErr
		}

		return &mockFieldHistoryRows{rows: []fieldHistoryRow{
			{hlcTS: 300, hlcCount: 2, nodeID: "a", state: state},
			{hlcTS: 100, nodeID: "a"},
		}}, nil
	}})

	return NewSyncController(p, WithTimeTravelEnabled(true), WithTimeTravelMaxDepth(5)), &lastArgs
}

func TestHTTPFieldHistory_Get(t *testing.T) {
	ctrl, args := fieldHistoryController(t, nil)

	rec := serve(t, ctrl.httpHandleFieldHistory, http.MethodGet, "/field-history?table=notes&pk=1&field=title&limit=50", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	resp := decodeBody[FieldHistoryResponse](t, rec)
	assert.Equal(t, "title", resp.Field)
	require.Len(t, resp.Entries, 2)
	assert.Equal(t, int64(300), resp.Entries[0].HLC.Timestamp)
	assert.Equal(t, TypeLWW, resp.Entries[0].Type)
	assert.JSONEq(t, `"hi"`, string(resp.Entries[0].Value))
	assert.Nil(t, resp.Entries[1].Value, "a row without state has no value")
	assert.Equal(t, 5, (*args)[4], "the limit is capped at MaxHistoryDepth")

	rec = serve(t, ctrl.httpHandleFieldHistory, http.MethodGet, "/field-history?table=notes&pk=1&field=title&limit=bad", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 5, (*args)[4], "a bad limit falls back to the default, then the cap")

	rec = serve(t, ctrl.httpHandleFieldHistory, http.MethodGet, "/field-history?table=notes&pk=1", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "table, pk, and field are required")
}

func TestHTTPFieldHistory_PostAndErrors(t *testing.T) {
	ctrl, args := fieldHistoryController(t, nil)

	rec := serve(t, ctrl.httpHandleFieldHistory, http.MethodPost, "/field-history", `{"table":"notes","pk":"1","field":"title","limit":2}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Len(t, decodeBody[FieldHistoryResponse](t, rec).Entries, 2)
	assert.Equal(t, 2, (*args)[4])

	assert.Equal(t, http.StatusBadRequest, serve(t, ctrl.httpHandleFieldHistory, http.MethodPost, "/field-history", `{`).Code)

	broken, _ := fieldHistoryController(t, errors.New("db down"))
	assert.Equal(t, http.StatusInternalServerError,
		serve(t, broken.httpHandleFieldHistory, http.MethodGet, "/field-history?table=notes&pk=1&field=title", "").Code)
	assert.Equal(t, http.StatusInternalServerError,
		serve(t, broken.httpHandleFieldHistory, http.MethodPost, "/field-history", `{"table":"notes","pk":"1","field":"title"}`).Code)
}
