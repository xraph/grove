// Command golden writes the JSON fixtures grove_crdt's tests compare
// against. Every fixture is produced by the Go crdt package itself, so the
// Dart wire format is pinned to the Go struct tags rather than to a reading
// of them.
package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/xraph/grove/crdt"
)

type wireCase struct {
	Name string          `json:"name"`
	Type string          `json:"type"`
	JSON json.RawMessage `json:"json"`
}

type goJSONCase struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

func h(ts int64, c uint32, node string) crdt.HLC {
	return crdt.HLC{Timestamp: ts, Counter: c, NodeID: node}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func main() {
	out := flag.String("out", "../../test/fixtures", "fixture directory")
	flag.Parse()
	must(0, os.MkdirAll(*out, 0o755))
	write(filepath.Join(*out, "wire_golden.json"), wireCases())
	write(filepath.Join(*out, "go_json_golden.json"), goJSONCases())
	writeExtra(*out)
}

func write(path string, v any) {
	data := must(json.MarshalIndent(v, "", "  "))
	must(0, os.WriteFile(path, append(data, '\n'), 0o644))
}

// writeExtra is the hook later tasks extend (Task 5 adds apply_golden.json).
func writeExtra(string) {}

func wireCases() []wireCase {
	big := h(1712345678901234567, 3, "node-a")
	var cases []wireCase
	add := func(name, typ string, v any) {
		cases = append(cases, wireCase{Name: name, Type: typ, JSON: must(json.Marshal(v))})
	}

	add("hlc_zero", "HLC", crdt.HLC{})
	add("hlc_big", "HLC", big)

	base := func() crdt.ChangeRecord {
		return crdt.ChangeRecord{Table: "documents", PK: "doc-1", Field: "title", CRDTType: crdt.TypeLWW, HLC: big, NodeID: "node-a"}
	}
	c := base()
	c.Value = json.RawMessage(`"Hello <b>"`)
	add("change_lww", "ChangeRecord", c)
	c = base()
	c.Value = json.RawMessage(`null`)
	add("change_lww_null_value", "ChangeRecord", c)
	c = base()
	c.Field, c.Tombstone = "", true
	add("change_tombstone_pushed", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType, c.Tombstone = "_tombstone", "", true
	add("change_tombstone_pulled", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "views", crdt.TypeCounter
	c.CounterDelta = &crdt.CounterDelta{Increment: 7, Decrement: 2}
	add("change_counter", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "tags", crdt.TypeSet
	c.SetOp = &crdt.SetOperation{Op: crdt.SetOpAdd, Elements: json.RawMessage(`["a<b",{"k":1}]`)}
	add("change_set_add", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "tags", crdt.TypeSet
	c.SetOp = &crdt.SetOperation{Op: crdt.SetOpRemove, Elements: json.RawMessage(`["x"]`), Tags: []crdt.Tag{{NodeID: "node-b", HLC: h(5, 0, "node-b")}}}
	add("change_set_remove_tags", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "items", crdt.TypeList
	c.ListOp = &crdt.ListOp{Op: crdt.ListOpInsert, NodeID: h(10, 0, "node-a"), Value: json.RawMessage(`{"n":1}`)}
	add("change_list_insert", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "items", crdt.TypeList
	c.ListOp = &crdt.ListOp{Op: crdt.ListOpDelete, NodeID: h(10, 0, "node-a")}
	add("change_list_delete", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "body", crdt.TypeText
	c.TextOp = &crdt.TextOp{Op: crdt.TextOpInsert, Origin: big, Content: "héllo 😀"}
	add("change_text_insert", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "body", crdt.TypeText
	c.TextOp = &crdt.TextOp{Op: crdt.TextOpFormat, Spans: []crdt.TextSpan{{Origin: big, Start: 0, Length: 2}}, Attrs: map[string]json.RawMessage{"bold": json.RawMessage(`true`)}}
	add("change_text_format", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "meta", crdt.TypeDocument
	c.Value = json.RawMessage(`{"path":"address.city","value":"Lagos"}`)
	add("change_document_path", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "tags", crdt.TypeSet
	c.State = setField()
	add("change_with_state", "ChangeRecord", c)

	reg := must(crdt.NewLWWRegister("Hello", h(1, 0, "a"), "a"))
	add("field_lww", "FieldState", reg.ToFieldState())
	counter := crdt.NewPNCounterState()
	counter.Increment("a", 5)
	counter.Decrement("b", 2)
	add("field_counter", "FieldState", counter.ToFieldState(h(3, 0, "b"), "b"))
	add("field_set", "FieldState", setField())
	list := crdt.NewRGAListState()
	must(0, list.Insert("x", crdt.HLC{}, "a", h(1, 0, "a")))
	must(0, list.Insert("y", h(1, 0, "a"), "a", h(2, 0, "a")))
	list.Delete(h(1, 0, "a"))
	add("field_list", "FieldState", list.ToFieldState(h(2, 0, "a"), "a"))
	doc := crdt.NewDocumentCRDTState()
	must(0, doc.SetField("address.city", "Lagos", h(1, 0, "a"), "a"))
	views := crdt.NewPNCounterState()
	views.Increment("a", 3)
	doc.SetFieldState("views", views.ToFieldState(h(2, 0, "a"), "a"))
	add("field_document", "FieldState", doc.ToFieldState(h(2, 0, "a"), "a"))
	text := crdt.NewTextState()
	must(text.Insert(crdt.TextRef{}, "héllo", "a", h(10, 0, "a")))
	ref, _ := text.RefAt(1)
	must(text.Delete(ref, 2))
	ref0, _ := text.RefAt(0)
	must(text.Format(ref0, 1, map[string]json.RawMessage{"bold": json.RawMessage(`true`)}, "a", h(11, 0, "a")))
	add("field_text", "FieldState", text.ToFieldState(h(11, 0, "a"), "a"))

	st := crdt.NewState("documents", "doc-1")
	st.Fields["title"] = reg.ToFieldState()
	add("state_live", "DocumentState", st)
	dead := crdt.NewState("documents", "doc-2")
	dead.Tombstone, dead.TombstoneHLC = true, h(9, 0, "b")
	add("state_tombstoned", "DocumentState", dead)

	add("pull_request", "PullRequest", crdt.PullRequest{Tables: []string{"documents"}, Since: big, NodeID: "node-a", Filter: &crdt.SyncFilter{PKFilter: []string{"doc-1"}}})
	add("pull_request_zero", "PullRequest", crdt.PullRequest{Tables: []string{"documents"}, NodeID: "node-a"})
	add("pull_response_empty", "PullResponse", crdt.PullResponse{})
	lww := base()
	lww.Value = json.RawMessage(`"x"`)
	add("pull_response", "PullResponse", crdt.PullResponse{Changes: []crdt.ChangeRecord{lww}, LatestHLC: big})
	add("push_request", "PushRequest", crdt.PushRequest{Changes: []crdt.ChangeRecord{lww}, NodeID: "node-a"})
	add("push_response", "PushResponse", crdt.PushResponse{Merged: 1, LatestHLC: big})

	at := time.Date(2026, 10, 4, 12, 0, 0, 120000000, time.UTC)
	ps := crdt.PresenceState{NodeID: "node-a", Topic: "documents:doc-1", Data: json.RawMessage(`{"cursor":3}`), UpdatedAt: at, ExpiresAt: at.Add(30 * time.Second)}
	add("presence_state", "PresenceState", ps)
	add("presence_update", "PresenceUpdate", crdt.PresenceUpdate{NodeID: "node-a", Topic: "t", Data: json.RawMessage(`{"cursor":3}`)})
	add("presence_update_leave", "PresenceUpdate", crdt.PresenceUpdate{NodeID: "node-a", Topic: "t", Data: json.RawMessage(`null`)})
	add("presence_event_join", "PresenceEvent", crdt.PresenceEvent{Type: crdt.PresenceJoin, NodeID: "node-a", Topic: "t", Data: json.RawMessage(`{"cursor":3}`)})
	add("presence_event_leave", "PresenceEvent", crdt.PresenceEvent{Type: crdt.PresenceLeave, NodeID: "node-a", Topic: "t"})
	add("presence_snapshot", "PresenceSnapshot", crdt.PresenceSnapshot{Topic: "t", States: []crdt.PresenceState{ps}})

	add("ws_pull_request", "WebSocketMessage", crdt.WebSocketMessage{Type: crdt.WSPullRequest, Payload: must(json.Marshal(crdt.PullRequest{Tables: []string{"documents"}, NodeID: "node-a"})), RequestID: "r1"})
	add("ws_pong", "WebSocketMessage", crdt.WebSocketMessage{Type: crdt.WSPong, Payload: json.RawMessage(`null`)})
	add("ws_error", "WebSocketMessage", crdt.WebSocketMessage{Type: crdt.WSError, Payload: json.RawMessage(`{"error":"crdt: inbound change hook: nope"}`), RequestID: "r2"})
	add("ws_subscribe", "WebSocketMessage", crdt.WebSocketMessage{Type: crdt.WSSubscribe, Payload: json.RawMessage(`{"tables":["documents"]}`)})

	room := crdt.Room{ID: "documents:doc-1", Type: "document", Metadata: json.RawMessage(`{"table":"documents"}`), CreatedAt: at, CreatedBy: "node-a"}
	add("room", "Room", room)
	add("room_minimal", "Room", crdt.Room{ID: "lobby", CreatedAt: at})
	add("room_info", "RoomInfo", crdt.RoomInfo{Room: room, ParticipantCount: 1, Participants: []crdt.PresenceState{ps}})

	// Cases the brief's list leaves to Dart-side reasoning: each pins an
	// omitempty or zero-value rule against Go itself.
	c = base()
	c.Field, c.CRDTType = "items", crdt.TypeList
	c.ListOp = &crdt.ListOp{Op: crdt.ListOpMove, NodeID: h(10, 0, "node-a"), ParentID: h(4, 1, "node-b"), Value: json.RawMessage(`[1,2]`)}
	add("change_list_move", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "body", crdt.TypeText
	c.TextOp = &crdt.TextOp{Op: crdt.TextOpDelete, Spans: []crdt.TextSpan{{Origin: big, Start: 1, Length: 3}}}
	add("change_text_delete", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "body", crdt.TypeText
	c.TextOp = &crdt.TextOp{Op: crdt.TextOpInsert, Ref: crdt.TextRef{Origin: big, Offset: 2}, Origin: h(20, 1, "node-b"), Content: "x"}
	add("change_text_insert_ref", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "body", crdt.TypeText
	c.TextOp = &crdt.TextOp{Op: crdt.TextOpFormat, Spans: []crdt.TextSpan{{Origin: big, Start: 0, Length: 1}}, Attrs: map[string]json.RawMessage{"bold": json.RawMessage(`null`)}}
	add("change_text_format_clear", "ChangeRecord", c)
	c = base()
	c.Field, c.CRDTType = "views", crdt.TypeCounter
	c.CounterDelta = &crdt.CounterDelta{}
	add("change_counter_zero", "ChangeRecord", c)
	add("pull_request_field_filter", "PullRequest", crdt.PullRequest{Tables: []string{"documents"}, NodeID: "node-a", Filter: &crdt.SyncFilter{PKFilter: []string{"a", "b"}, FieldFilter: []string{"title"}}})
	add("pull_request_empty_filter", "PullRequest", crdt.PullRequest{Tables: []string{"documents"}, NodeID: "node-a", Filter: &crdt.SyncFilter{}})

	add("presence_state_no_expiry", "PresenceState", crdt.PresenceState{NodeID: "node-a", Topic: "t", UpdatedAt: at})
	add("room_max_participants", "Room", crdt.Room{ID: "r", MaxParticipants: 8, CreatedAt: at, Metadata: json.RawMessage(`null`)})

	add("cursor_position", "CursorPosition", crdt.CursorPosition{X: 1.5, Y: -2.25, Offset: 3, Line: 4, Column: 5, SelectionStart: 1, SelectionEnd: 6, Field: "body"})
	add("cursor_position_zero", "CursorPosition", crdt.CursorPosition{})
	add("participant_data", "ParticipantData", crdt.ParticipantData{Name: "Ada", Color: "#f00", Avatar: "https://x/y.png", Cursor: &crdt.CursorPosition{Line: 2}, IsTyping: true, ActiveField: "title", Status: "away", Extra: map[string]any{"role": "editor", "n": 2}})
	add("participant_data_minimal", "ParticipantData", crdt.ParticipantData{})
	add("participant_data_zero_cursor", "ParticipantData", crdt.ParticipantData{Name: "Bo", Cursor: &crdt.CursorPosition{}})
	return cases
}

func setField() *crdt.FieldState {
	s := crdt.NewORSetState()
	must(0, s.Add("a<b", "a", h(1, 0, "a")))
	must(0, s.Add(map[string]any{"k": 1}, "a", h(2, 0, "a")))
	must(0, s.Remove("a<b"))
	return s.ToFieldState(h(2, 0, "a"), "a")
}

func goJSONCases() []goJSONCase {
	// Numbers stay inside 2^53: Go decodes untyped numbers to float64 and
	// would round larger integers, which the protocol never relies on.
	inputs := []string{
		`"a<b>&c"`, `"  "`, `"line\nbreak\ttab\"quote\\"`, `"\u0001\u001f\b\f"`,
		`"\u007f"`, `"héllo 😀"`, `{"b":1,"a":[true,false,null],"c":{"z":"","y":0}}`,
		`{"é":1,"e":2,"Z":3}`, `0.1`, `100`, `1e20`, `1e21`, `1.5e-7`, `1e-7`,
		`0.000001`, `-3.25`, `[]`, `{}`, `[1,"1",1.5]`,
	}
	out := make([]goJSONCase, 0, len(inputs))
	for _, in := range inputs {
		var v any
		must(0, json.Unmarshal([]byte(in), &v))
		out = append(out, goJSONCase{Input: in, Output: string(must(json.Marshal(v)))})
	}
	return out
}
