package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/crdt"
)

// rejectHook refuses inbound changes to one configurable field, the way an
// application's BeforeInboundChange hook would.
type rejectHook struct {
	crdt.BaseSyncHook
	mu    sync.Mutex
	field string
}

func (h *rejectHook) BeforeInboundChange(_ context.Context, c *crdt.ChangeRecord) (*crdt.ChangeRecord, error) {
	h.mu.Lock()
	f := h.field
	h.mu.Unlock()
	if f != "" && c.Field == f {
		return nil, fmt.Errorf("%s is locked", c.Field)
	}
	return c, nil
}

type server struct {
	plugin   *crdt.Plugin
	ctrl     *crdt.SyncController
	hook     *rejectHook
	mu       sync.Mutex
	datasets map[string]string // foundry dataset id -> shadow table
}

func newServer(tables []string, validate bool) (*server, error) {
	exec, err := openSQLite()
	if err != nil {
		return nil, err
	}
	datasets := map[string]string{"ds1": "ds_notes"}
	all := append(append([]string{}, tables...), "ds_notes")
	hook := &rejectHook{}
	plugin := crdt.New(crdt.WithNodeID("conformance"), crdt.WithTables(all...), crdt.WithSyncHook(hook))
	plugin.SetExecutor(exec)
	for _, t := range all {
		if err := plugin.EnsureShadowTable(context.Background(), t); err != nil {
			return nil, err
		}
	}
	opts := []crdt.SyncControllerOption{
		crdt.WithPresenceEnabled(true),
		crdt.WithPresenceTTL(30 * time.Second),
		crdt.WithRoomManager(true),
		crdt.WithStreamPollInterval(50 * time.Millisecond),
	}
	if validate {
		opts = append(opts, crdt.WithValidation(crdt.DefaultValidationConfig()))
	}
	return &server{plugin: plugin, ctrl: crdt.NewSyncController(plugin, opts...), hook: hook, datasets: datasets}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sync/pull", s.nativePull)
	mux.HandleFunc("POST /sync/push", s.nativePush)
	mux.HandleFunc("GET /sync/stream", s.nativeStream)
	mux.HandleFunc("GET /sync/ws", s.ws)
	mux.HandleFunc("POST /sync/presence", s.presenceUpdate)
	mux.HandleFunc("GET /sync/presence", s.presenceGet)
	rooms := http.StripPrefix("/sync", crdt.RoomHTTPHandler(s.ctrl.Rooms()))
	mux.Handle("/sync/rooms", rooms)
	mux.Handle("/sync/rooms/", rooms)
	mux.HandleFunc("POST /api/v1/datasets/{id}/sync/pull", s.dtoPull)
	mux.HandleFunc("POST /api/v1/datasets/{id}/sync/push", s.dtoPush)
	mux.HandleFunc("GET /api/v1/datasets/{id}/sync/stream", s.dtoStream)
	mux.HandleFunc("GET /api/v1/datasets/{id}/sync/ws", s.dtoWS)
	mux.HandleFunc("POST /admin/reject", s.adminReject)
	mux.HandleFunc("POST /admin/seed", s.adminSeed)
	mux.HandleFunc("DELETE /admin/datasets/{id}", s.adminDeleteDataset)
	mux.HandleFunc("GET /admin/state", s.adminState)
	return mux
}

// --- native protocol (the shape grove's Forge extension serves) ---

func (s *server) nativePull(w http.ResponseWriter, r *http.Request) {
	var req crdt.PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}
	resp, err := s.ctrl.HandlePull(r.Context(), &req)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, resp)
}

func (s *server) nativePush(w http.ResponseWriter, r *http.Request) {
	var req crdt.PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}
	resp, err := s.ctrl.HandlePush(r.Context(), &req)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, resp)
}

func sinceFromQuery(r *http.Request) crdt.HLC {
	var since crdt.HLC
	if v, err := strconv.ParseInt(r.URL.Query().Get("since_ts"), 10, 64); err == nil {
		since.Timestamp = v
	}
	if v, err := strconv.ParseUint(r.URL.Query().Get("since_count"), 10, 32); err == nil {
		since.Counter = uint32(v)
	}
	since.NodeID = r.URL.Query().Get("since_node")
	return since
}

// streamSSE mirrors the extension's handleStream: changes, presence, and a
// keep-alive comment.
func (s *server) streamSSE(w http.ResponseWriter, r *http.Request, tables []string, since crdt.HLC, withPresence bool) {
	ch, err := s.ctrl.StreamChangesSince(r.Context(), tables, since)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	flusher := w.(http.Flusher)
	if err != nil {
		fmt.Fprintf(w, "event: error\ndata: %s\n\n", err.Error())
		flusher.Flush()
		return
	}
	flusher.Flush()
	var presence <-chan crdt.PresenceEvent
	if withPresence {
		presence = s.ctrl.PresenceChannel()
	}
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case changes, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(changes)
			fmt.Fprintf(w, "event: changes\ndata: %s\n\n", data)
			flusher.Flush()
		case ev, ok := <-presence:
			if !ok {
				presence = nil
				continue
			}
			data, _ := crdt.MarshalPresenceEvent(ev)
			fmt.Fprintf(w, "event: presence\ndata: %s\n\n", data)
			flusher.Flush()
		case <-keepAlive.C:
			fmt.Fprint(w, ": keep-alive\n\n")
			flusher.Flush()
		}
	}
}

func (s *server) nativeStream(w http.ResponseWriter, r *http.Request) {
	var tables []string
	for _, t := range strings.Split(r.URL.Query().Get("tables"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			tables = append(tables, t)
		}
	}
	s.streamSSE(w, r, tables, sinceFromQuery(r), true)
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// gorillaConn adapts gorilla/websocket to crdt.WebSocketConn. The handler
// writes from its read loop and its stream goroutine, so writes are locked.
type gorillaConn struct {
	c  *websocket.Conn
	mu sync.Mutex
}

func (g *gorillaConn) ReadMessage() ([]byte, error) {
	_, data, err := g.c.ReadMessage()
	return data, err
}

func (g *gorillaConn) WriteMessage(data []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.c.WriteMessage(websocket.TextMessage, data)
}

func (g *gorillaConn) Close() error { return g.c.Close() }

func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h := crdt.NewWebSocketHandler(s.ctrl, &gorillaConn{c: c}, log.NewNoopLogger())
	_ = h.Serve(r.Context())
}

func (s *server) presenceUpdate(w http.ResponseWriter, r *http.Request) {
	var u crdt.PresenceUpdate
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	ev, err := s.ctrl.HandlePresenceUpdate(r.Context(), &u)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, ev)
}

func (s *server) presenceGet(w http.ResponseWriter, r *http.Request) {
	snap, err := s.ctrl.HandleGetPresence(r.Context(), r.URL.Query().Get("topic"))
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, snap)
}

// --- foundry's DTO dialect (grove_sync.controller.go) ---

type hlcDTO struct {
	Timestamp int64  `json:"ts"`
	Counter   uint32 `json:"counter"`
	NodeID    string `json:"nodeId"`
}

type dtoFilter struct {
	PKFilter    []string `json:"pk_filter,omitempty"`
	FieldFilter []string `json:"field_filter,omitempty"`
}

func toDTO(h crdt.HLC) *hlcDTO {
	if h.IsZero() {
		return nil
	}
	return &hlcDTO{Timestamp: h.Timestamp, Counter: h.Counter, NodeID: h.NodeID}
}

func forgeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"code": status, "message": msg})
}

func (s *server) dataset(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	s.mu.Lock()
	table, ok := s.datasets[id]
	s.mu.Unlock()
	if !ok {
		forgeError(w, 404, fmt.Sprintf("dataset %s is not collaborative or not provisioned", id))
	}
	return table, ok
}

func (s *server) dtoPull(w http.ResponseWriter, r *http.Request) {
	table, ok := s.dataset(w, r)
	if !ok {
		return
	}
	var req struct {
		Since  *hlcDTO    `json:"since,omitempty"`
		Filter *dtoFilter `json:"filter,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		forgeError(w, 400, err.Error())
		return
	}
	pull := &crdt.PullRequest{Tables: []string{table}}
	if req.Since != nil {
		pull.Since = crdt.HLC{Timestamp: req.Since.Timestamp, Counter: req.Since.Counter, NodeID: req.Since.NodeID}
	}
	if req.Filter != nil {
		pull.Filter = &crdt.SyncFilter{PKFilter: req.Filter.PKFilter, FieldFilter: req.Filter.FieldFilter}
	}
	resp, err := s.ctrl.HandlePull(r.Context(), pull)
	if err != nil {
		forgeError(w, 500, err.Error())
		return
	}
	changes := make([]json.RawMessage, 0, len(resp.Changes))
	for _, c := range resp.Changes {
		b, _ := json.Marshal(c)
		changes = append(changes, b)
	}
	writeJSON(w, 200, map[string]any{"changes": changes, "latestHlc": toDTO(resp.LatestHLC)})
}

func (s *server) dtoPush(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.dataset(w, r); !ok {
		return
	}
	var req struct {
		Changes []crdt.ChangeRecord `json:"changes"`
		NodeID  string              `json:"nodeId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		forgeError(w, 400, "invalid change record: "+err.Error())
		return
	}
	resp, err := s.ctrl.HandlePush(r.Context(), &crdt.PushRequest{Changes: req.Changes, NodeID: req.NodeID})
	if err != nil {
		forgeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"merged": resp.Merged, "latestHlc": toDTO(resp.LatestHLC)})
}

// dtoStream reproduces foundry's stream: always from HLC zero, no since.
func (s *server) dtoStream(w http.ResponseWriter, r *http.Request) {
	table, ok := s.dataset(w, r)
	if !ok {
		return
	}
	s.streamSSE(w, r, []string{table}, crdt.HLC{}, false)
}

func (s *server) dtoWS(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.dataset(w, r); !ok {
		return
	}
	s.ws(w, r)
}

// --- admin ---

func (s *server) adminReject(w http.ResponseWriter, r *http.Request) {
	s.hook.mu.Lock()
	s.hook.field = r.URL.Query().Get("field")
	s.hook.mu.Unlock()
	w.WriteHeader(204)
}

func (s *server) adminSeed(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	count, _ := strconv.Atoi(r.URL.Query().Get("count"))
	store := s.plugin.MetadataStore()
	for i := 0; i < count; i++ {
		clock := s.plugin.Clock().Now()
		reg, _ := crdt.NewLWWRegister(fmt.Sprintf("seed %d", i), clock, "conformance")
		if err := store.WriteFieldState(r.Context(), table, fmt.Sprintf("seed-%d", i), "title", reg.ToFieldState()); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
	}
	w.WriteHeader(204)
}

func (s *server) adminDeleteDataset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	delete(s.datasets, r.PathValue("id"))
	s.mu.Unlock()
	w.WriteHeader(204)
}

func (s *server) adminState(w http.ResponseWriter, r *http.Request) {
	st, err := s.plugin.Inspect(r.Context(), r.URL.Query().Get("table"), r.URL.Query().Get("pk"))
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, st)
}
