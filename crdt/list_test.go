package crdt

import (
	"encoding/json"
	"testing"
)

func TestRGAList_InsertAndElements(t *testing.T) {
	l := NewRGAListState()
	_ = l.Insert("first", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})
	_ = l.Insert("second", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1", HLC{Timestamp: 2, NodeID: "node-1"})
	_ = l.Insert("third", HLC{Timestamp: 2, NodeID: "node-1"}, "node-1", HLC{Timestamp: 3, NodeID: "node-1"})

	elems := l.Elements()
	if len(elems) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elems))
	}

	vals := make([]string, 0, len(elems))
	for _, e := range elems {
		var v string
		_ = json.Unmarshal(e, &v)
		vals = append(vals, v)
	}

	if vals[0] != "first" || vals[1] != "second" || vals[2] != "third" {
		t.Errorf("unexpected order: %v", vals)
	}
}

func TestRGAList_Delete(t *testing.T) {
	l := NewRGAListState()
	id := HLC{Timestamp: 1, NodeID: "node-1"}
	_ = l.Insert("item", HLC{}, "node-1", id)

	if l.Len() != 1 {
		t.Fatalf("expected 1 element, got %d", l.Len())
	}

	l.Delete(id)
	if l.Len() != 0 {
		t.Errorf("expected 0 elements after delete, got %d", l.Len())
	}

	elems := l.Elements()
	if len(elems) != 0 {
		t.Errorf("expected empty elements, got %d", len(elems))
	}
}

func TestRGAList_Move(t *testing.T) {
	l := NewRGAListState()
	id1 := HLC{Timestamp: 1, NodeID: "node-1"}
	id2 := HLC{Timestamp: 2, NodeID: "node-1"}
	id3 := HLC{Timestamp: 3, NodeID: "node-1"}

	_ = l.Insert("A", HLC{}, "node-1", id1)
	_ = l.Insert("B", id1, "node-1", id2)
	_ = l.Insert("C", id2, "node-1", id3)

	// Move C to after A (before B).
	moveID := HLC{Timestamp: 4, NodeID: "node-1"}
	l.Move(id3, id1, "node-1", moveID)

	elems := l.Elements()
	if len(elems) != 3 {
		t.Fatalf("expected 3 elements after move, got %d", len(elems))
	}

	vals := make([]string, 0, len(elems))
	for _, e := range elems {
		var v string
		_ = json.Unmarshal(e, &v)
		vals = append(vals, v)
	}

	// A, C (moved), B
	if vals[0] != "A" || vals[1] != "C" || vals[2] != "B" {
		t.Errorf("expected [A, C, B] after move, got %v", vals)
	}
}

func TestMergeList_TwoNodes(t *testing.T) {
	l1 := NewRGAListState()
	_ = l1.Insert("from-node1", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	l2 := NewRGAListState()
	_ = l2.Insert("from-node2", HLC{}, "node-2", HLC{Timestamp: 2, NodeID: "node-2"})

	merged := MergeList(l1, l2)
	if merged.Len() != 2 {
		t.Fatalf("expected 2 elements after merge, got %d", merged.Len())
	}
}

func TestMergeList_ConcurrentInsertAtSamePosition(t *testing.T) {
	// Both nodes insert at the same position (head).
	l1 := NewRGAListState()
	_ = l1.Insert("A", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	l2 := NewRGAListState()
	_ = l2.Insert("B", HLC{}, "node-2", HLC{Timestamp: 1, NodeID: "node-2"})

	merged := MergeList(l1, l2)
	if merged.Len() != 2 {
		t.Fatalf("expected 2 elements, got %d", merged.Len())
	}

	// Both should be present; order is deterministic by HLC tiebreak.
	elems := merged.Elements()
	vals := make([]string, 0, len(elems))
	for _, e := range elems {
		var v string
		_ = json.Unmarshal(e, &v)
		vals = append(vals, v)
	}
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
}

func TestMergeList_TombstonePreserved(t *testing.T) {
	l1 := NewRGAListState()
	id := HLC{Timestamp: 1, NodeID: "node-1"}
	_ = l1.Insert("item", HLC{}, "node-1", id)

	l2 := MergeList(NewRGAListState(), l1)
	l2.Delete(id)

	merged := MergeList(l1, l2)
	if merged.Len() != 0 {
		t.Errorf("expected tombstoned element removed from view, got %d visible", merged.Len())
	}
}

func TestMergeList_Commutative(t *testing.T) {
	l1 := NewRGAListState()
	_ = l1.Insert("A", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	l2 := NewRGAListState()
	_ = l2.Insert("B", HLC{}, "node-2", HLC{Timestamp: 2, NodeID: "node-2"})

	ab := MergeList(l1, l2)
	ba := MergeList(l2, l1)

	if ab.Len() != ba.Len() {
		t.Errorf("merge not commutative: %d vs %d", ab.Len(), ba.Len())
	}
}

func TestMergeList_Idempotent(t *testing.T) {
	l := NewRGAListState()
	_ = l.Insert("A", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	m1 := MergeList(l, l)
	m2 := MergeList(m1, l)

	if m1.Len() != m2.Len() {
		t.Errorf("merge not idempotent: %d vs %d", m1.Len(), m2.Len())
	}
}

func TestMergeList_NilHandling(t *testing.T) {
	l := NewRGAListState()
	_ = l.Insert("A", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	if MergeList(nil, l) == nil {
		t.Error("nil local should return remote")
	}
	if MergeList(l, nil) == nil {
		t.Error("nil remote should return local")
	}
}

func TestRGAList_ToFieldState(t *testing.T) {
	l := NewRGAListState()
	_ = l.Insert("hello", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	fs := l.ToFieldState(HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	if fs == nil {
		t.Fatal("expected non-nil FieldState")
	}
	if fs.Type != TypeList {
		t.Errorf("expected TypeList, got %s", fs.Type)
	}
	if fs.ListState == nil {
		t.Error("expected ListState to be set")
	}

	// Round-trip.
	restored := ListFromFieldState(fs)
	if restored == nil {
		t.Fatal("expected non-nil restored list")
	}
	if restored.Len() != 1 {
		t.Errorf("expected 1 element, got %d", restored.Len())
	}
}

func TestMergeEngine_MergeField_List(t *testing.T) {
	engine := NewMergeEngine()

	l1 := NewRGAListState()
	_ = l1.Insert("A", HLC{}, "node-1", HLC{Timestamp: 1, NodeID: "node-1"})
	fs1 := l1.ToFieldState(HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	l2 := NewRGAListState()
	_ = l2.Insert("B", HLC{}, "node-2", HLC{Timestamp: 2, NodeID: "node-2"})
	fs2 := l2.ToFieldState(HLC{Timestamp: 2, NodeID: "node-2"}, "node-2")

	merged, err := engine.MergeField(fs1, fs2)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Type != TypeList {
		t.Errorf("expected TypeList, got %s", merged.Type)
	}
	if merged.ListState.Len() != 2 {
		t.Errorf("expected 2 elements in merged list, got %d", merged.ListState.Len())
	}
}
