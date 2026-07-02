package crdt

import (
	"encoding/json"
	"testing"
)

// typeText spells sequential chars from one node, returning the state and
// the ops it emitted (in order).
func typeText(t *testing.T, node string, startTS int64, chunks ...string) (*TextState, []*TextOp) {
	t.Helper()
	st := NewTextState()
	var ops []*TextOp
	ts := startTS
	for _, chunk := range chunks {
		ref := TextRef{}
		if l := st.Len(); l > 0 {
			var ok bool
			ref, ok = st.RefAt(l - 1)
			if !ok {
				t.Fatalf("RefAt(%d) not found", l-1)
			}
		}
		op, err := st.Insert(ref, chunk, node, HLC{Timestamp: ts, NodeID: node})
		if err != nil {
			t.Fatalf("insert %q: %v", chunk, err)
		}
		ops = append(ops, op)
		ts++
	}
	return st, ops
}

// replay applies ops to a fresh state (simulating a remote replica).
func replay(t *testing.T, ops []*TextOp, metas []opMeta) *TextState {
	t.Helper()
	st := NewTextState()
	for i, op := range ops {
		if err := st.Apply(op, metas[i].node, metas[i].hlc); err != nil {
			t.Fatalf("apply op %d: %v", i, err)
		}
	}
	return st
}

type opMeta struct {
	node string
	hlc  HLC
}

func metasFor(node string, startTS int64, n int) []opMeta {
	out := make([]opMeta, n)
	for i := range out {
		out[i] = opMeta{node: node, hlc: HLC{Timestamp: startTS + int64(i), NodeID: node}}
	}
	return out
}

func TestText_TypingCoalesces(t *testing.T) {
	st, ops := typeText(t, "a", 1, "h", "e", "l", "l", "o")
	if got := st.Value(); got != "hello" {
		t.Fatalf("value = %q", got)
	}
	// Sequential same-node typing extends one origin: 5 ops, 1 origin.
	if got := len(st.Frags); got != 1 {
		t.Fatalf("want 1 origin, got %d", got)
	}
	// Later ops are extensions of the first op's origin.
	first := ops[0].Origin
	for i, op := range ops[1:] {
		if op.Origin != first {
			t.Fatalf("op %d origin %v, want %v", i+1, op.Origin, first)
		}
	}
}

func TestText_ReplayMatchesLocal(t *testing.T) {
	st, ops := typeText(t, "a", 1, "he", "llo", " world")
	remote := replay(t, ops, metasFor("a", 1, len(ops)))
	if remote.Value() != st.Value() {
		t.Fatalf("replay %q != local %q", remote.Value(), st.Value())
	}
}

func TestText_InsertMiddleSplits(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello")
	ref, ok := st.RefAt(1) // after 'e'
	if !ok {
		t.Fatal("RefAt(1)")
	}
	if _, err := st.Insert(ref, "XY", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if got := st.Value(); got != "heXYllo" {
		t.Fatalf("value = %q", got)
	}
}

func TestText_InsertAtHead(t *testing.T) {
	st, _ := typeText(t, "a", 10, "world")
	if _, err := st.Insert(TextRef{}, "hello ", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if got := st.Value(); got != "hello world" {
		t.Fatalf("value = %q", got)
	}
}

func TestText_DeleteRange(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello world")
	ref, _ := st.RefAt(5) // the space
	op, err := st.Delete(ref, 6)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Value(); got != "hello" {
		t.Fatalf("value = %q", got)
	}
	if len(op.Spans) == 0 {
		t.Fatal("delete op must carry resolved spans")
	}
	if got := st.Len(); got != 5 {
		t.Fatalf("len = %d", got)
	}
}

func TestText_DeleteAcrossOrigins(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello")
	ref, _ := st.RefAt(4)
	if _, err := st.Insert(ref, " world", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	// Delete "o wo" — spans both origins.
	dref, _ := st.RefAt(4)
	if _, err := st.Delete(dref, 4); err != nil {
		t.Fatal(err)
	}
	if got := st.Value(); got != "hellrld" {
		t.Fatalf("value = %q", got)
	}
}

func TestText_FormatAndDelta(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello world")
	ref, _ := st.RefAt(0)
	bold := map[string]json.RawMessage{"bold": json.RawMessage(`true`)}
	if _, err := st.Format(ref, 5, bold, "a", HLC{Timestamp: 50, NodeID: "a"}); err != nil {
		t.Fatal(err)
	}
	delta := st.Delta()
	if len(delta) != 2 {
		t.Fatalf("delta segments = %d (%+v)", len(delta), delta)
	}
	if delta[0].Insert != "hello" || string(delta[0].Attributes["bold"]) != "true" {
		t.Fatalf("segment 0: %+v", delta[0])
	}
	if delta[1].Insert != " world" || len(delta[1].Attributes) != 0 {
		t.Fatalf("segment 1: %+v", delta[1])
	}
	// Value is unaffected by formatting.
	if st.Value() != "hello world" {
		t.Fatalf("value = %q", st.Value())
	}
}

func TestText_FormatLWW(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hi")
	ref, _ := st.RefAt(0)
	bold := map[string]json.RawMessage{"bold": json.RawMessage(`true`)}
	unbold := map[string]json.RawMessage{"bold": json.RawMessage(`null`)}
	if _, err := st.Format(ref, 2, bold, "a", HLC{Timestamp: 50, NodeID: "a"}); err != nil {
		t.Fatal(err)
	}
	// Older conflicting format loses.
	if _, err := st.Format(ref, 2, unbold, "b", HLC{Timestamp: 40, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if d := st.Delta(); string(d[0].Attributes["bold"]) != "true" {
		t.Fatalf("older format should lose: %+v", d)
	}
	// Newer clears it (null tombstones the attribute).
	if _, err := st.Format(ref, 2, unbold, "b", HLC{Timestamp: 60, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if d := st.Delta(); len(d[0].Attributes) != 0 {
		t.Fatalf("newer null should clear: %+v", d)
	}
}

func TestText_RefSurvivesConcurrentEdits(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello world")
	// Anchor a cursor after "hello".
	anchor, ok := st.RefAt(4)
	if !ok {
		t.Fatal("anchor")
	}
	// Concurrent-ish head insert shifts indices but not the ref.
	if _, err := st.Insert(TextRef{}, ">> ", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	idx, ok := st.IndexOf(anchor)
	if !ok {
		t.Fatal("IndexOf")
	}
	if idx != 7 { // ">> hello" — 'o' now at index 7
		t.Fatalf("anchor index = %d, want 7 (%q)", idx, st.Value())
	}
}

func TestText_IndexOfTombstonedCollapses(t *testing.T) {
	st, _ := typeText(t, "a", 1, "abcdef")
	anchor, _ := st.RefAt(2) // 'c'
	ref, _ := st.RefAt(1)    // delete "bcd"
	if _, err := st.Delete(ref, 3); err != nil {
		t.Fatal(err)
	}
	if st.Value() != "aef" {
		t.Fatalf("value = %q", st.Value())
	}
	idx, ok := st.IndexOf(anchor)
	if !ok {
		t.Fatal("tombstoned anchor should still resolve")
	}
	if idx != 1 { // collapses to where 'c' would be: before 'e'
		t.Fatalf("collapsed index = %d, want 1", idx)
	}
}

func TestText_ConcurrentInsertSamePoint_Convergent(t *testing.T) {
	base, baseOps := typeText(t, "a", 1, "ab")
	// Two replicas insert at the same point (after 'a') concurrently.
	r1 := replay(t, baseOps, metasFor("a", 1, len(baseOps)))
	r2 := replay(t, baseOps, metasFor("a", 1, len(baseOps)))
	ref, _ := base.RefAt(0)

	op1, err := r1.Insert(ref, "X", "n1", HLC{Timestamp: 100, NodeID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	op2, err := r2.Insert(ref, "Y", "n2", HLC{Timestamp: 101, NodeID: "n2"})
	if err != nil {
		t.Fatal(err)
	}

	// Cross-apply in opposite orders.
	if err := r1.Apply(op2, "n2", HLC{Timestamp: 101, NodeID: "n2"}); err != nil {
		t.Fatal(err)
	}
	if err := r2.Apply(op1, "n1", HLC{Timestamp: 100, NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}

	if r1.Value() != r2.Value() {
		t.Fatalf("diverged: %q vs %q", r1.Value(), r2.Value())
	}
	// Newer insert wins the position closest to the anchor.
	if r1.Value() != "aYXb" {
		t.Fatalf("value = %q, want aYXb", r1.Value())
	}
}

func TestText_EmptyState(t *testing.T) {
	st := NewTextState()
	if st.Value() != "" || st.Len() != 0 {
		t.Fatal("empty state")
	}
	if d := st.Delta(); len(d) != 0 {
		t.Fatalf("delta = %+v", d)
	}
	if _, ok := st.RefAt(0); ok {
		t.Fatal("RefAt on empty")
	}
}

func TestText_JSONRoundTrip(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello")
	ref, _ := st.RefAt(4)
	if _, err := st.Insert(ref, " world", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var back TextState
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Value() != st.Value() {
		t.Fatalf("round-trip %q != %q", back.Value(), st.Value())
	}
}

func TestText_UnicodeRunes(t *testing.T) {
	st, _ := typeText(t, "a", 1, "héllo 🌍")
	if st.Len() != 7 {
		t.Fatalf("rune len = %d, want 7", st.Len())
	}
	ref, _ := st.RefAt(5) // after the space
	if _, err := st.Insert(ref, "brave ", "b", HLC{Timestamp: 100, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if st.Value() != "héllo brave 🌍" {
		t.Fatalf("value = %q", st.Value())
	}
}
