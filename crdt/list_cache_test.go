package crdt

import (
	"encoding/json"
	"testing"
)

func TestRGAList_OrderCacheInvalidation(t *testing.T) {
	l := NewRGAListState()
	clock := HLC{Timestamp: 1, NodeID: "a"}
	if err := l.Insert("one", HLC{}, "a", clock); err != nil {
		t.Fatal(err)
	}
	if got := len(l.Elements()); got != 1 {
		t.Fatalf("want 1, got %d", got)
	}
	// Cached read returns the same content.
	if got := len(l.Elements()); got != 1 {
		t.Fatalf("cached read: want 1, got %d", got)
	}
	// Mutation invalidates: new element appears.
	clock2 := HLC{Timestamp: 2, NodeID: "a"}
	if err := l.Insert("two", clock, "a", clock2); err != nil {
		t.Fatal(err)
	}
	els := l.Elements()
	if len(els) != 2 || string(els[0]) != `"one"` || string(els[1]) != `"two"` {
		t.Fatalf("after insert: %v", els)
	}
	// Delete invalidates.
	l.Delete(clock)
	els = l.Elements()
	if len(els) != 1 || string(els[0]) != `"two"` {
		t.Fatalf("after delete: %v", els)
	}
	// JSON round-trip starts with a cold cache and still orders correctly.
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	var back RGAListState
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	els = back.Elements()
	if len(els) != 1 || string(els[0]) != `"two"` {
		t.Fatalf("round-trip: %v", els)
	}
	// Merge result orders correctly (fresh cache).
	other := NewRGAListState()
	clock3 := HLC{Timestamp: 3, NodeID: "b"}
	if err := other.Insert("three", clock2, "b", clock3); err != nil {
		t.Fatal(err)
	}
	merged := MergeList(&back, other)
	els = merged.Elements()
	if len(els) != 2 || string(els[1]) != `"three"` {
		t.Fatalf("merged: %v", els)
	}
}
