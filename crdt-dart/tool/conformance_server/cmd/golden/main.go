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

type applyStep struct {
	Change json.RawMessage `json:"change"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type applyCase struct {
	Name  string      `json:"name"`
	Steps []applyStep `json:"steps"`
	// NondeterministicValue marks a case whose document states hold a leaf
	// and a nested path under it. Go's Resolve walks a map, so which of the
	// two wins in the resolved "value" changes from run to run. The fixture
	// leaves that top-level "value" out, and the Dart test drops it from its
	// own result before comparing.
	NondeterministicValue bool `json:"nondeterministic_value,omitempty"`
}

type mergeStateCase struct {
	Name   string          `json:"name"`
	Local  json.RawMessage `json:"local"`
	Remote json.RawMessage `json:"remote"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// writeExtra writes the fixtures later tasks add: the text goldens and the
// apply and merge-state goldens.
func writeExtra(out string) {
	writeTextGolden(out)
	writeCompactGolden(out)
	writeCompact(filepath.Join(out, "apply_golden.json"), map[string]any{
		"apply":       applyCases(),
		"merge_state": mergeStateCases(),
	})
}

func ch(field string, typ crdt.CRDTType, clock crdt.HLC) crdt.ChangeRecord {
	return crdt.ChangeRecord{Table: "t", PK: "1", Field: field, CRDTType: typ, HLC: clock, NodeID: clock.NodeID}
}

// run applies changes from an empty field, marshalling each step at once:
// ApplyChange mutates text and document state in place, so a pointer kept
// for later would show a later state.
func run(name string, changes ...crdt.ChangeRecord) applyCase {
	var local *crdt.FieldState
	ac := applyCase{Name: name}
	for i := range changes {
		c := changes[i]
		step := applyStep{Change: must(json.Marshal(c))}
		res, err := crdt.ApplyChange(nil, local, &c)
		if err != nil {
			step.Error = err.Error()
		} else {
			local = res
			step.Result = must(json.Marshal(res))
		}
		ac.Steps = append(ac.Steps, step)
	}
	return ac
}

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

// writeCompact writes v as compact JSON, for fixtures big enough that
// indentation would dominate their size.
func writeCompact(path string, v any) {
	data := must(json.Marshal(v))
	must(0, os.WriteFile(path, append(data, '\n'), 0o644))
}

// nondeterministic drops each step's top-level "value", which Go computes by
// walking a map when a leaf and a nested path collide.
func nondeterministic(c applyCase) applyCase {
	c.NondeterministicValue = true
	for i := range c.Steps {
		if c.Steps[i].Result == nil {
			continue
		}
		var m map[string]json.RawMessage
		must(0, json.Unmarshal(c.Steps[i].Result, &m))
		delete(m, "value")
		c.Steps[i].Result = must(json.Marshal(m))
	}
	return c
}

func applyCases() []applyCase {
	lww := func(v string, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("title", crdt.TypeLWW, clock)
		c.Value = json.RawMessage(v)
		return c
	}
	counter := func(inc, dec int64, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("views", crdt.TypeCounter, clock)
		c.CounterDelta = &crdt.CounterDelta{Increment: inc, Decrement: dec}
		return c
	}
	// Set elements are written canonically (Go json.Marshal form): Dart
	// re-derives keys from decoded values, and the server never relays set
	// ops (pulled sets carry full state), so raw bytes never reach Dart.
	set := func(op crdt.SetOp, elems string, tags []crdt.Tag, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("tags", crdt.TypeSet, clock)
		c.SetOp = &crdt.SetOperation{Op: op, Elements: json.RawMessage(elems), Tags: tags}
		return c
	}
	list := func(op crdt.ListOpType, node, parent crdt.HLC, value string, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("items", crdt.TypeList, clock)
		c.ListOp = &crdt.ListOp{Op: op, NodeID: node, ParentID: parent}
		if value != "" {
			c.ListOp.Value = json.RawMessage(value)
		}
		return c
	}
	docPath := func(path, value string, tombstone bool, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("meta", crdt.TypeDocument, clock)
		c.Value = json.RawMessage(`{"path":"` + path + `","value":` + value + `}`)
		c.Tombstone = tombstone
		return c
	}
	text := func(op *crdt.TextOp, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("body", crdt.TypeText, clock)
		c.TextOp = op
		return c
	}
	docRaw := func(value string, clock crdt.HLC) crdt.ChangeRecord {
		c := ch("meta", crdt.TypeDocument, clock)
		c.Value = json.RawMessage(value)
		return c
	}
	carrier := func(field string, typ crdt.CRDTType, fs *crdt.FieldState, clock crdt.HLC) crdt.ChangeRecord {
		c := ch(field, typ, clock)
		c.State = fs
		return c
	}
	counterState := func(inc, dec map[string]int64, clock crdt.HLC) crdt.ChangeRecord {
		cs := crdt.NewPNCounterState()
		for k, v := range inc {
			cs.Increments[k] = v
		}
		for k, v := range dec {
			cs.Decrements[k] = v
		}
		return carrier("views", crdt.TypeCounter, cs.ToFieldState(clock, clock.NodeID), clock)
	}
	listCarrier := func(clock crdt.HLC, tombstone bool) crdt.ChangeRecord {
		l := crdt.NewRGAListState()
		must(0, l.Insert("x", crdt.HLC{}, "a", h(1, 0, "a")))
		must(0, l.Insert("y", h(1, 0, "a"), clock.NodeID, clock))
		must(0, l.Insert("z", crdt.HLC{}, "b", h(1, 1, "b")))
		if tombstone {
			l.Delete(h(1, 0, "a"))
		}
		return carrier("items", crdt.TypeList, l.ToFieldState(clock, clock.NodeID), clock)
	}
	textCarrier := func(clock crdt.HLC, node, content string, format bool) crdt.ChangeRecord {
		ts := crdt.NewTextState()
		must(ts.Insert(crdt.TextRef{}, content, node, clock))
		if format {
			r0, _ := ts.RefAt(0)
			must(ts.Format(r0, 2, map[string]json.RawMessage{"bold": json.RawMessage(`true`)}, node, h(clock.Timestamp+1, 0, node)))
			r1, _ := ts.RefAt(1)
			must(ts.Delete(r1, 1))
		}
		return carrier("body", crdt.TypeText, ts.ToFieldState(clock, node), clock)
	}
	lwwCarrier := func(v string, clock crdt.HLC) crdt.ChangeRecord {
		fs := &crdt.FieldState{Type: crdt.TypeLWW, HLC: clock, NodeID: clock.NodeID, Value: json.RawMessage(v)}
		return carrier("title", crdt.TypeLWW, fs, clock)
	}
	docNested := func(clock crdt.HLC, node string) crdt.ChangeRecord {
		d := crdt.NewDocumentCRDTState()
		must(0, d.SetField("shared", "from "+node, clock, node))
		must(0, d.SetField("deep.leaf", node, clock, node))
		tags := crdt.NewORSetState()
		must(0, tags.Add("t-"+node, node, clock))
		must(0, tags.Add("t<>", node, clock))
		d.SetFieldState("tags", tags.ToFieldState(clock, node))
		items := crdt.NewRGAListState()
		must(0, items.Insert(node, crdt.HLC{}, node, clock))
		d.SetFieldState("items", items.ToFieldState(clock, node))
		// "mixed" is an lww on one side and a counter on the other: a type
		// mismatch inside a document resolves by the higher HLC.
		if node == "a" {
			must(0, d.SetField("mixed", 1, clock, node))
		} else {
			c := crdt.NewPNCounterState()
			c.Increment(node, 9)
			d.SetFieldState("mixed", c.ToFieldState(clock, node))
		}
		return carrier("meta", crdt.TypeDocument, d.ToFieldState(clock, node), clock)
	}

	// Text ops come from Go's own local edit API so they are valid by
	// construction, then replay through ApplyChange from an empty field.
	src := crdt.NewTextState()
	t1 := h(1, 0, "a")
	ins := must(src.Insert(crdt.TextRef{}, "hello world", "a", t1))
	ref6, _ := src.RefAt(6)
	t2 := h(2, 0, "a")
	del := must(src.Delete(ref6, 5))
	ref0, _ := src.RefAt(0)
	t3 := h(3, 0, "a")
	format := must(src.Format(ref0, 5, map[string]json.RawMessage{"bold": json.RawMessage(`true`)}, "a", t3))
	t4 := h(4, 0, "b")
	ref5, _ := src.RefAt(5)
	ins2 := must(src.Insert(ref5, "there", "b", t4))

	docState := func(node string, clock crdt.HLC, inc int64) crdt.ChangeRecord {
		d := crdt.NewDocumentCRDTState()
		must(0, d.SetField("title", "from "+node, clock, node))
		views := crdt.NewPNCounterState()
		views.Increment(node, inc)
		d.SetFieldState("views", views.ToFieldState(clock, node))
		c := ch("meta", crdt.TypeDocument, clock)
		c.State = d.ToFieldState(clock, node)
		return c
	}

	return []applyCase{
		run("lww_newer_wins",
			lww(`"v1"`, h(10, 0, "a")), lww(`"v0"`, h(5, 0, "b")), lww(`"v2"`, h(10, 0, "b"))),
		run("counter_cumulative",
			counter(3, 0, h(1, 0, "a")), counter(2, 1, h(2, 0, "b")), counter(5, 0, h(3, 0, "a")), counter(3, 0, h(1, 0, "a"))),
		run("set_add_remove",
			set(crdt.SetOpAdd, `["x","y"]`, nil, h(1, 0, "a")),
			set(crdt.SetOpRemove, `["x"]`, []crdt.Tag{{NodeID: "a", HLC: h(1, 0, "a")}}, h(2, 0, "a")),
			set(crdt.SetOpAdd, `["x"]`, nil, h(3, 0, "b")),
			set(crdt.SetOpRemove, `["y"]`, nil, h(4, 0, "b")),
			set(crdt.SetOpAdd, `[{"j":"\u003c","k":1}]`, nil, h(5, 0, "a"))),
		run("set_state_carrier", func() crdt.ChangeRecord { c := ch("tags", crdt.TypeSet, h(2, 0, "a")); c.State = setField(); return c }()),
		run("list_ops",
			list(crdt.ListOpInsert, crdt.HLC{}, crdt.HLC{}, `"a"`, h(1, 0, "a")),
			list(crdt.ListOpInsert, h(2, 0, "a"), h(1, 0, "a"), `"b"`, h(2, 0, "a")),
			list(crdt.ListOpDelete, h(9, 0, "c"), crdt.HLC{}, "", h(10, 0, "c")),
			list(crdt.ListOpInsert, h(9, 0, "c"), h(2, 0, "a"), `"late"`, h(9, 0, "c")),
			list(crdt.ListOpMove, h(1, 0, "a"), h(2, 0, "a"), `"a"`, h(11, 0, "b"))),
		run("text_ops", text(ins, t1), text(del, t2), text(format, t3), text(ins2, t4)),
		run("document_paths",
			docPath("a.b", `1`, false, h(2, 0, "a")),
			docPath("a.b", `0`, false, h(1, 0, "b")),
			docPath("a.c", `"x"`, false, h(3, 0, "a")),
			docPath("a.b", `null`, true, h(4, 0, "b")),
			docPath("a.c", `null`, true, h(2, 5, "b"))),
		run("document_state_carrier", docState("a", h(1, 0, "a"), 2), docState("b", h(2, 0, "b"), 3)),
		run("type_mismatch", lww(`"v"`, h(1, 0, "a")), counter(1, 0, h(2, 0, "a"))),
		run("counter_missing_delta", ch("views", crdt.TypeCounter, h(1, 0, "a"))),
		run("set_missing_op", ch("tags", crdt.TypeSet, h(1, 0, "a"))),
		run("list_missing_op", ch("items", crdt.TypeList, h(1, 0, "a"))),
		run("text_missing_op", ch("body", crdt.TypeText, h(1, 0, "a"))),
		run("untyped_change", ch("x", "", h(1, 0, "a"))),
		run("text_empty_insert", text(&crdt.TextOp{Op: crdt.TextOpInsert}, h(1, 0, "a"))),
		run("state_type_mismatch", func() crdt.ChangeRecord {
			c := ch("title", crdt.TypeLWW, h(1, 0, "a"))
			c.State = crdt.NewPNCounterState().ToFieldState(h(1, 0, "a"), "a")
			return c
		}()),
		run("document_change_errors",
			ch("meta", crdt.TypeDocument, h(1, 0, "a")),
			docRaw(`null`, h(2, 0, "a")),
			docRaw(`"x"`, h(3, 0, "a")),
			docRaw(`[1]`, h(4, 0, "a")),
			docRaw(`7`, h(5, 0, "a")),
			docRaw(`true`, h(6, 0, "a")),
			docRaw(`{"path":1,"value":1}`, h(7, 0, "a")),
			docRaw(`{"path":"","value":1}`, h(8, 0, "a")),
			docRaw(`{"value":1}`, h(9, 0, "a"))),
		run("document_path_keys",
			docRaw(`{"Path":"a.b","VALUE":3}`, h(1, 0, "a")),
			docRaw(`{"path":"x","path":"c.d","value":4}`, h(2, 0, "a")),
			docRaw(`{"path":"c.e"}`, h(3, 0, "a")),
			docRaw(`{"path":"c.f","value":null}`, h(4, 0, "a")),
			docRaw(`{"path":"c.g","value":1,"Value":2}`, h(5, 0, "a")),
			docRaw(`{"path":"c.h","value":3,"PATH":null}`, h(6, 0, "a"))),
		nondeterministic(run("document_tombstone_prefix",
			docPath("a", `1`, false, h(1, 0, "a")),
			docPath("a.b", `2`, false, h(2, 0, "a")),
			docPath("a.b.c", `3`, false, h(3, 0, "a")),
			docPath("ab", `4`, false, h(4, 0, "a")),
			docPath("a", `null`, true, h(5, 0, "a")),
			docPath("zz", `null`, true, h(6, 0, "a")))),
		run("counter_state_carrier",
			counterState(map[string]int64{"a": 4}, map[string]int64{"a": 1}, h(1, 0, "a")),
			counterState(map[string]int64{"a": 2, "b": 5}, nil, h(2, 0, "b")),
			counter(1, 0, h(3, 0, "c"))),
		run("list_state_carrier", listCarrier(h(3, 0, "a"), false), listCarrier(h(2, 0, "b"), true)),
		run("text_state_carrier", textCarrier(h(1, 0, "a"), "a", "alpha", false), textCarrier(h(2, 0, "b"), "b", "beta", true)),
		run("lww_state_carrier",
			lwwCarrier(`{"n":1}`, h(5, 0, "a")), lwwCarrier(`"old"`, h(4, 0, "b")), lwwCarrier(`null`, h(5, 0, "z"))),
		run("document_nested_types", docNested(h(1, 0, "a"), "a"), docNested(h(2, 0, "b"), "b")),
	}
}

func mergeStateCases() []mergeStateCase {
	field := func(v string, clock crdt.HLC) *crdt.FieldState {
		return must(crdt.NewLWWRegister(v, clock, clock.NodeID)).ToFieldState()
	}
	mk := func(name string, local, remote *crdt.State) mergeStateCase {
		l, r := must(json.Marshal(local)), must(json.Marshal(remote))
		res, err := crdt.NewMergeEngine().MergeState(local, remote)
		if err != nil {
			return mergeStateCase{Name: name, Local: l, Remote: r, Error: err.Error()}
		}
		return mergeStateCase{Name: name, Local: l, Remote: r, Result: must(json.Marshal(res))}
	}
	s := func(fields map[string]*crdt.FieldState, tomb bool, tombHLC crdt.HLC) *crdt.State {
		st := crdt.NewState("t", "1")
		for k, v := range fields {
			st.Fields[k] = v
		}
		st.Tombstone, st.TombstoneHLC = tomb, tombHLC
		return st
	}
	return []mergeStateCase{
		mk("remote_tombstone_newer_than_fields",
			s(map[string]*crdt.FieldState{"title": field("a", h(1, 0, "a"))}, false, crdt.HLC{}),
			s(nil, true, h(5, 0, "b"))),
		mk("remote_tombstone_older_than_a_field",
			s(map[string]*crdt.FieldState{"title": field("a", h(9, 0, "a"))}, false, crdt.HLC{}),
			s(nil, true, h(5, 0, "b"))),
		mk("both_tombstoned",
			s(nil, true, h(3, 0, "a")),
			s(nil, true, h(7, 0, "b"))),
		mk("type_mismatch_errors",
			s(map[string]*crdt.FieldState{"x": field("a", h(1, 0, "a"))}, false, crdt.HLC{}),
			s(map[string]*crdt.FieldState{"x": crdt.NewPNCounterState().ToFieldState(h(2, 0, "b"), "b")}, false, crdt.HLC{})),
		mk("local_tombstone_older_than_remote_field",
			s(nil, true, h(5, 0, "a")),
			s(map[string]*crdt.FieldState{"title": field("b", h(6, 0, "b"))}, false, crdt.HLC{})),
		mk("local_tombstone_newer_than_remote_fields",
			s(nil, true, h(8, 0, "a")),
			s(map[string]*crdt.FieldState{"title": field("b", h(6, 0, "b"))}, false, crdt.HLC{})),
		mk("equal_tombstone_clock_loses_to_field",
			s(nil, true, h(6, 0, "b")),
			s(map[string]*crdt.FieldState{"title": field("b", h(6, 0, "b"))}, false, crdt.HLC{})),
		mk("fields_union",
			s(map[string]*crdt.FieldState{"title": field("a", h(1, 0, "a"))}, false, crdt.HLC{}),
			s(map[string]*crdt.FieldState{"note": field("n", h(2, 0, "b"))}, false, crdt.HLC{})),
	}
}
