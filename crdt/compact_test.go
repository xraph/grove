package crdt

import (
	"encoding/json"
	"testing"
)

func TestListCompact_DropsLeafTombstones(t *testing.T) {
	l := NewRGAListState()
	a := HLC{Timestamp: 1, NodeID: "a"}
	b := HLC{Timestamp: 2, NodeID: "a"}
	c := HLC{Timestamp: 3, NodeID: "a"}
	_ = l.Insert("a", HLC{}, "a", a)
	_ = l.Insert("b", a, "a", b)
	_ = l.Insert("c", b, "a", c)
	l.Delete(c) // leaf tombstone

	dropped := l.Compact(HLC{Timestamp: 100, NodeID: "z"})
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	els := l.Elements()
	if len(els) != 2 || string(els[0]) != `"a"` || string(els[1]) != `"b"` {
		t.Fatalf("elements after GC: %v", els)
	}
	if len(l.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(l.Nodes))
	}
}

func TestListCompact_KeepsAnchoredTombstones(t *testing.T) {
	l := NewRGAListState()
	a := HLC{Timestamp: 1, NodeID: "a"}
	b := HLC{Timestamp: 2, NodeID: "b"}
	_ = l.Insert("a", HLC{}, "a", a)
	_ = l.Insert("b", a, "b", b) // b anchored to a
	l.Delete(a)                  // a tombstoned but is b's parent

	if dropped := l.Compact(HLC{Timestamp: 100, NodeID: "z"}); dropped != 0 {
		t.Fatalf("dropped = %d, want 0 (anchored)", dropped)
	}
	els := l.Elements()
	if len(els) != 1 || string(els[0]) != `"b"` {
		t.Fatalf("elements: %v", els)
	}
}

func TestListCompact_CascadesToFixpoint(t *testing.T) {
	l := NewRGAListState()
	a := HLC{Timestamp: 1, NodeID: "a"}
	b := HLC{Timestamp: 2, NodeID: "a"}
	_ = l.Insert("a", HLC{}, "a", a)
	_ = l.Insert("b", a, "a", b)
	l.Delete(a)
	l.Delete(b)
	// b is the leaf; once dropped, a becomes a leaf too.
	if dropped := l.Compact(HLC{Timestamp: 100, NodeID: "z"}); dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if len(l.Nodes) != 0 {
		t.Fatalf("nodes = %d, want 0", len(l.Nodes))
	}
}

func TestListCompact_HorizonGates(t *testing.T) {
	l := NewRGAListState()
	a := HLC{Timestamp: 50, NodeID: "a"}
	_ = l.Insert("a", HLC{}, "a", a)
	l.Delete(a)
	// Horizon older than the node: nothing dropped.
	if dropped := l.Compact(HLC{Timestamp: 10, NodeID: "z"}); dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if dropped := l.Compact(HLC{Timestamp: 100, NodeID: "z"}); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}

func TestSetCompact_DropsRemovedTags(t *testing.T) {
	s := NewORSetState()
	old := HLC{Timestamp: 10, NodeID: "a"}
	fresh := HLC{Timestamp: 90, NodeID: "b"}
	_ = s.Add("x", "a", old)
	_ = s.Add("x", "b", fresh) // second, newer tag
	// Remove only the old tag (observed-remove of the first add).
	s.Removed[tagKey(Tag{NodeID: "a", HLC: old})] = true

	dropped := s.Compact(HLC{Timestamp: 50, NodeID: "z"})
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	// Add-wins is preserved: x still present via the fresh tag.
	ok, _ := s.Contains("x")
	if !ok {
		t.Fatal("x must survive")
	}
	if len(s.Entries[`"x"`]) != 1 {
		t.Fatalf("tags = %d, want 1", len(s.Entries[`"x"`]))
	}
	if len(s.Removed) != 0 {
		t.Fatalf("removed markers = %d, want 0", len(s.Removed))
	}
}

func TestSetCompact_ElementFullyRemoved(t *testing.T) {
	s := NewORSetState()
	old := HLC{Timestamp: 10, NodeID: "a"}
	_ = s.Add("x", "a", old)
	_ = s.Remove("x")
	if dropped := s.Compact(HLC{Timestamp: 50, NodeID: "z"}); dropped != 1 {
		t.Fatalf("dropped = %d", dropped)
	}
	if _, exists := s.Entries[`"x"`]; exists {
		t.Fatal("empty entry should be pruned")
	}
	if got := len(s.Elements()); got != 0 {
		t.Fatalf("elements = %d", got)
	}
}

func TestTextCompact_SkeletonizesAndPreservesOrder(t *testing.T) {
	st, _ := typeText(t, "a", 1, "hello world")
	ref, _ := st.RefAt(5)
	if _, err := st.Delete(ref, 6); err != nil { // " world"
		t.Fatal(err)
	}
	// A live anchor inside the surviving range.
	anchor, _ := st.RefAt(2)

	dropped := st.Compact(HLC{Timestamp: 100, NodeID: "z"})
	if dropped == 0 {
		t.Fatal("expected skeletonized fragments")
	}
	if st.Value() != "hello" {
		t.Fatalf("value = %q", st.Value())
	}
	// Tombstoned content is gone but addressing is intact.
	for _, frags := range st.Frags {
		for _, f := range frags {
			if f.Tombstone && f.Content != "" {
				t.Fatalf("content not freed: %+v", f)
			}
		}
	}
	if idx, ok := st.IndexOf(anchor); !ok || idx != 2 {
		t.Fatalf("anchor after GC: idx=%d ok=%v", idx, ok)
	}
	// Concurrent-style insert anchored at a surviving char still lands.
	iref, _ := st.RefAt(4)
	if _, err := st.Insert(iref, "!", "b", HLC{Timestamp: 200, NodeID: "b"}); err != nil {
		t.Fatal(err)
	}
	if st.Value() != "hello!" {
		t.Fatalf("value = %q", st.Value())
	}
}

func TestTextCompact_CoalescesSkeletons(t *testing.T) {
	st, _ := typeText(t, "a", 1, "abcdef")
	r1, _ := st.RefAt(0)
	if _, err := st.Delete(r1, 2); err != nil {
		t.Fatal(err)
	}
	r2, _ := st.RefAt(0) // now 'c'
	if _, err := st.Delete(r2, 2); err != nil {
		t.Fatal(err)
	}
	st.Compact(HLC{Timestamp: 100, NodeID: "z"})
	if st.Value() != "ef" {
		t.Fatalf("value = %q", st.Value())
	}
	// Adjacent tombstoned skeletons merged into one fragment.
	total := 0
	skeletons := 0
	for _, frags := range st.Frags {
		for _, f := range frags {
			total++
			if f.Tombstone {
				skeletons++
			}
		}
	}
	if skeletons != 1 {
		t.Fatalf("skeleton fragments = %d, want 1 (total %d)", skeletons, total)
	}
}

func TestStateCompact_WalksFields(t *testing.T) {
	st := NewState("tbl", "pk1")

	l := NewRGAListState()
	a := HLC{Timestamp: 1, NodeID: "a"}
	_ = l.Insert("x", HLC{}, "a", a)
	l.Delete(a)
	st.Fields["list"] = l.ToFieldState(a, "a")

	s := NewORSetState()
	_ = s.Add("x", "a", a)
	_ = s.Remove("x")
	st.Fields["set"] = s.ToFieldState(a, "a")

	txt, _ := typeText(t, "a", 1, "hi")
	tref, _ := txt.RefAt(0)
	if _, err := txt.Delete(tref, 2); err != nil {
		t.Fatal(err)
	}
	st.Fields["text"] = txt.ToFieldState(a, "a")

	st.Fields["plain"] = &FieldState{Type: TypeLWW, HLC: a, NodeID: "a", Value: json.RawMessage(`1`)}

	if dropped := st.Compact(HLC{Timestamp: 100, NodeID: "z"}); dropped == 0 {
		t.Fatal("expected drops across fields")
	}
	if got := len(ListFromFieldState(st.Fields["list"]).Nodes); got != 0 {
		t.Fatalf("list nodes = %d", got)
	}
	if got := len(SetFromFieldState(st.Fields["set"]).Elements()); got != 0 {
		t.Fatalf("set elements = %d", got)
	}
}

func TestCompact_ZeroHorizonNoOp(t *testing.T) {
	l := NewRGAListState()
	a := HLC{Timestamp: 1, NodeID: "a"}
	_ = l.Insert("x", HLC{}, "a", a)
	l.Delete(a)
	if dropped := l.Compact(HLC{}); dropped != 0 {
		t.Fatalf("zero horizon must be a no-op, dropped %d", dropped)
	}
}
