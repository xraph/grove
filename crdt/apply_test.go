package crdt

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func applyHLC(ts int64, node string) HLC {
	return HLC{Timestamp: ts, NodeID: node}
}

func mustApply(t *testing.T, local *FieldState, c *ChangeRecord) *FieldState {
	t.Helper()
	out, err := ApplyChange(NewMergeEngine(), local, c)
	if err != nil {
		t.Fatalf("ApplyChange: %v", err)
	}
	return out
}

func TestApplyChange_NilLocal_LWW(t *testing.T) {
	c := &ChangeRecord{
		CRDTType: TypeLWW,
		HLC:      applyHLC(10, "a"),
		NodeID:   "a",
		Value:    json.RawMessage(`"hello"`),
	}
	out := mustApply(t, nil, c)
	if out.Type != TypeLWW || string(out.Value) != `"hello"` {
		t.Fatalf("unexpected state: %+v", out)
	}
}

func TestApplyChange_LWW_HigherHLCWins(t *testing.T) {
	local := &FieldState{Type: TypeLWW, HLC: applyHLC(10, "a"), NodeID: "a", Value: json.RawMessage(`"old"`)}
	c := &ChangeRecord{CRDTType: TypeLWW, HLC: applyHLC(20, "b"), NodeID: "b", Value: json.RawMessage(`"new"`)}
	out := mustApply(t, local, c)
	if string(out.Value) != `"new"` {
		t.Fatalf("want new, got %s", out.Value)
	}
	// Stale write loses.
	stale := &ChangeRecord{CRDTType: TypeLWW, HLC: applyHLC(5, "c"), NodeID: "c", Value: json.RawMessage(`"stale"`)}
	out = mustApply(t, out, stale)
	if string(out.Value) != `"new"` {
		t.Fatalf("stale write should lose, got %s", out.Value)
	}
}

func TestApplyChange_CounterDelta(t *testing.T) {
	c1 := &ChangeRecord{
		CRDTType:     TypeCounter,
		HLC:          applyHLC(10, "a"),
		NodeID:       "a",
		CounterDelta: &CounterDelta{Increment: 5, Decrement: 1},
	}
	c2 := &ChangeRecord{
		CRDTType:     TypeCounter,
		HLC:          applyHLC(11, "b"),
		NodeID:       "b",
		CounterDelta: &CounterDelta{Increment: 3},
	}
	out := mustApply(t, nil, c1)
	out = mustApply(t, out, c2)
	got := CounterFromFieldState(out).Value()
	if got != 7 { // (5-1) + 3
		t.Fatalf("counter value = %d, want 7", got)
	}
	// Idempotent re-apply (same node, same delta snapshot): max-per-node keeps 7.
	out = mustApply(t, out, c1)
	if got := CounterFromFieldState(out).Value(); got != 7 {
		t.Fatalf("re-apply changed value to %d", got)
	}
}

func TestApplyChange_SetAdd(t *testing.T) {
	c := &ChangeRecord{
		CRDTType: TypeSet,
		HLC:      applyHLC(10, "a"),
		NodeID:   "a",
		SetOp:    &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x","y"]`)},
	}
	out := mustApply(t, nil, c)
	set := SetFromFieldState(out)
	if got := len(set.Elements()); got != 2 {
		t.Fatalf("want 2 elements, got %d", got)
	}
}

func TestApplyChange_SetRemove_ExactTags(t *testing.T) {
	add := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(10, "a"), NodeID: "a",
		SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)},
	}
	out := mustApply(t, nil, add)
	rm := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(20, "b"), NodeID: "b",
		SetOp: &SetOperation{
			Op:       SetOpRemove,
			Elements: json.RawMessage(`["x"]`),
			Tags:     []Tag{{NodeID: "a", HLC: applyHLC(10, "a")}},
		},
	}
	out = mustApply(t, out, rm)
	set := SetFromFieldState(out)
	if got := len(set.Elements()); got != 0 {
		t.Fatalf("want empty set, got %d elements", got)
	}
}

func TestApplyChange_SetRemove_AddWins(t *testing.T) {
	// Remove names only the tag it observed; a concurrent add with a
	// different tag survives (add-wins).
	addA := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(10, "a"), NodeID: "a",
		SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)},
	}
	addB := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(25, "b"), NodeID: "b",
		SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)},
	}
	rmA := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(20, "c"), NodeID: "c",
		SetOp: &SetOperation{
			Op:       SetOpRemove,
			Elements: json.RawMessage(`["x"]`),
			Tags:     []Tag{{NodeID: "a", HLC: applyHLC(10, "a")}},
		},
	}
	out := mustApply(t, nil, addA)
	out = mustApply(t, out, addB)
	out = mustApply(t, out, rmA)
	set := SetFromFieldState(out)
	ok, err := set.Contains("x")
	if err != nil || !ok {
		t.Fatalf("add-wins: x should survive (ok=%v err=%v)", ok, err)
	}
}

func TestApplyChange_SetRemove_LegacyNoTags(t *testing.T) {
	// Legacy removes (no tags, as crdt-js 0.0.1 emits) remove every local
	// tag older than the remove's HLC — observed-remove approximation.
	add := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(10, "a"), NodeID: "a",
		SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)},
	}
	newer := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(30, "b"), NodeID: "b",
		SetOp: &SetOperation{Op: SetOpAdd, Elements: json.RawMessage(`["x"]`)},
	}
	rm := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(20, "c"), NodeID: "c",
		SetOp: &SetOperation{Op: SetOpRemove, Elements: json.RawMessage(`["x"]`)},
	}
	out := mustApply(t, nil, add)
	out = mustApply(t, out, newer)
	out = mustApply(t, out, rm)
	set := SetFromFieldState(out)
	ok, _ := set.Contains("x")
	if !ok {
		t.Fatal("newer add (HLC 30) should survive a remove stamped at HLC 20")
	}
	// And with only the old tag present, the element goes away.
	out2 := mustApply(t, nil, add)
	out2 = mustApply(t, out2, rm)
	set2 := SetFromFieldState(out2)
	ok2, _ := set2.Contains("x")
	if ok2 {
		t.Fatal("old tag (HLC 10) should be removed by legacy remove at HLC 20")
	}
}

func TestApplyChange_ListInsertDeleteMove(t *testing.T) {
	id1 := applyHLC(10, "a")
	id2 := applyHLC(11, "a")
	ins1 := &ChangeRecord{
		CRDTType: TypeList, HLC: id1, NodeID: "a",
		ListOp: &ListOp{Op: ListOpInsert, NodeID: id1, Value: json.RawMessage(`"first"`)},
	}
	ins2 := &ChangeRecord{
		CRDTType: TypeList, HLC: id2, NodeID: "a",
		ListOp: &ListOp{Op: ListOpInsert, NodeID: id2, ParentID: id1, Value: json.RawMessage(`"second"`)},
	}
	out := mustApply(t, nil, ins1)
	out = mustApply(t, out, ins2)
	list := ListFromFieldState(out)
	els := list.Elements()
	if len(els) != 2 || string(els[0]) != `"first"` || string(els[1]) != `"second"` {
		t.Fatalf("unexpected elements: %v", els)
	}

	del := &ChangeRecord{
		CRDTType: TypeList, HLC: applyHLC(12, "b"), NodeID: "b",
		ListOp: &ListOp{Op: ListOpDelete, NodeID: id1},
	}
	out = mustApply(t, out, del)
	list = ListFromFieldState(out)
	els = list.Elements()
	if len(els) != 1 || string(els[0]) != `"second"` {
		t.Fatalf("after delete: %v", els)
	}

	// Move second to head: tombstone + re-insert under zero parent.
	mvID := applyHLC(13, "b")
	mv := &ChangeRecord{
		CRDTType: TypeList, HLC: mvID, NodeID: "b",
		ListOp: &ListOp{Op: ListOpMove, NodeID: id2, ParentID: HLC{}, Value: json.RawMessage(`"second"`)},
	}
	out = mustApply(t, out, mv)
	list = ListFromFieldState(out)
	els = list.Elements()
	if len(els) != 1 || string(els[0]) != `"second"` {
		t.Fatalf("after move: %v", els)
	}
}

func TestApplyChange_ListDelete_UnseenNodeKeepsTombstone(t *testing.T) {
	// A delete that arrives before its insert must not be lost.
	id := applyHLC(10, "a")
	del := &ChangeRecord{
		CRDTType: TypeList, HLC: applyHLC(12, "b"), NodeID: "b",
		ListOp: &ListOp{Op: ListOpDelete, NodeID: id},
	}
	ins := &ChangeRecord{
		CRDTType: TypeList, HLC: id, NodeID: "a",
		ListOp: &ListOp{Op: ListOpInsert, NodeID: id, Value: json.RawMessage(`"x"`)},
	}
	out := mustApply(t, nil, del)
	out = mustApply(t, out, ins)
	list := ListFromFieldState(out)
	if got := len(list.Elements()); got != 0 {
		t.Fatalf("late insert should stay tombstoned, got %d elements", got)
	}
}

func TestApplyChange_DocumentPathOps(t *testing.T) {
	set := &ChangeRecord{
		CRDTType: TypeDocument, HLC: applyHLC(10, "a"), NodeID: "a",
		Value: json.RawMessage(`{"path":"title","value":"Hello"}`),
	}
	nested := &ChangeRecord{
		CRDTType: TypeDocument, HLC: applyHLC(11, "a"), NodeID: "a",
		Value: json.RawMessage(`{"path":"meta.author","value":"rex"}`),
	}
	out := mustApply(t, nil, set)
	out = mustApply(t, out, nested)
	doc := DocumentFromFieldState(out)
	resolved := doc.Resolve()
	if resolved["title"] != "Hello" {
		t.Fatalf("title = %v", resolved["title"])
	}
	meta, ok := resolved["meta"].(map[string]any)
	if !ok || meta["author"] != "rex" {
		t.Fatalf("meta = %v", resolved["meta"])
	}

	// Path delete via tombstoned record.
	del := &ChangeRecord{
		CRDTType: TypeDocument, HLC: applyHLC(12, "b"), NodeID: "b",
		Tombstone: true,
		Value:     json.RawMessage(`{"path":"title"}`),
	}
	out = mustApply(t, out, del)
	doc = DocumentFromFieldState(out)
	resolved = doc.Resolve()
	if _, exists := resolved["title"]; exists {
		t.Fatalf("title should be deleted, resolved=%v", resolved)
	}
}

func TestApplyChange_FullStateCarrier(t *testing.T) {
	// A change carrying a full FieldState merges state-based.
	remoteSet := NewORSetState()
	if err := remoteSet.Add("x", "a", applyHLC(10, "a")); err != nil {
		t.Fatal(err)
	}
	if err := remoteSet.Add("y", "a", applyHLC(11, "a")); err != nil {
		t.Fatal(err)
	}
	if err := remoteSet.Remove("x"); err != nil {
		t.Fatal(err)
	}
	c := &ChangeRecord{
		CRDTType: TypeSet, HLC: applyHLC(11, "a"), NodeID: "a",
		State: remoteSet.ToFieldState(applyHLC(11, "a"), "a"),
	}
	out := mustApply(t, nil, c)
	set := SetFromFieldState(out)
	els := set.Elements()
	if len(els) != 1 || string(els[0]) != `"y"` {
		t.Fatalf("state-carrier merge: %v", els)
	}
}

func TestApplyChange_TypeMismatch(t *testing.T) {
	local := &FieldState{Type: TypeLWW, HLC: applyHLC(10, "a"), NodeID: "a", Value: json.RawMessage(`1`)}
	c := &ChangeRecord{CRDTType: TypeCounter, HLC: applyHLC(11, "b"), NodeID: "b", CounterDelta: &CounterDelta{Increment: 1}}
	if _, err := ApplyChange(NewMergeEngine(), local, c); err == nil {
		t.Fatal("want type-mismatch error")
	}
}

func TestApplyChange_NilChange(t *testing.T) {
	if _, err := ApplyChange(NewMergeEngine(), nil, nil); err == nil {
		t.Fatal("want error for nil change")
	}
}

// Regression: inbound set ops used to be dropped by mergeRemoteChange (only
// CounterDelta was reconstructed). This drives a remove through the syncer
// and asserts the removed tag lands in the persisted field state.
func TestSyncer_InboundSetOp_Persisted(t *testing.T) {
	var written []string
	exec := &mockExecutor{
		execFn: func(_ context.Context, _ string, args ...any) (ExecResult, error) {
			for _, a := range args {
				switch v := a.(type) {
				case []byte:
					written = append(written, string(v))
				case string:
					written = append(written, v)
				}
			}
			return &mockExecResult{}, nil
		},
	}
	p := New(WithNodeID("server"))
	p.SetExecutor(exec)
	s := NewSyncer(p)

	rm := ChangeRecord{
		Table: "notes", PK: "1", Field: "tags",
		CRDTType: TypeSet,
		HLC:      applyHLC(20, "client"),
		NodeID:   "client",
		SetOp: &SetOperation{
			Op:       SetOpRemove,
			Elements: json.RawMessage(`["x"]`),
			Tags:     []Tag{{NodeID: "a", HLC: applyHLC(10, "a")}},
		},
	}
	if err := s.mergeRemoteChange(context.Background(), rm); err != nil {
		t.Fatalf("mergeRemoteChange: %v", err)
	}

	found := false
	for _, w := range written {
		if strings.Contains(w, `"removed"`) && strings.Contains(w, "a:") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("persisted state missing removed tag; writes: %v", written)
	}
}
