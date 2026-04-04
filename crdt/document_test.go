package crdt

import (
	"encoding/json"
	"testing"
)

func TestDocumentCRDT_SetAndResolve(t *testing.T) {
	doc := NewDocumentCRDTState()
	_ = doc.SetField("name", "Alice", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	_ = doc.SetField("address.city", "NYC", HLC{Timestamp: 2, NodeID: "node-1"}, "node-1")
	_ = doc.SetField("address.zip", "10001", HLC{Timestamp: 3, NodeID: "node-1"}, "node-1")

	resolved := doc.Resolve()

	if resolved["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", resolved["name"])
	}

	addr, ok := resolved["address"].(map[string]any)
	if !ok {
		t.Fatal("expected address to be a map")
	}
	if addr["city"] != "NYC" {
		t.Errorf("expected city=NYC, got %v", addr["city"])
	}
	if addr["zip"] != "10001" {
		t.Errorf("expected zip=10001, got %v", addr["zip"])
	}
}

func TestDocumentCRDT_GetField(t *testing.T) {
	doc := NewDocumentCRDTState()
	_ = doc.SetField("title", "Hello", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	fs := doc.GetField("title")
	if fs == nil {
		t.Fatal("expected field state")
	}
	if fs.Type != TypeLWW {
		t.Errorf("expected LWW, got %s", fs.Type)
	}

	var val string
	_ = json.Unmarshal(fs.Value, &val)
	if val != "Hello" {
		t.Errorf("expected Hello, got %s", val)
	}
}

func TestDocumentCRDT_DeleteField(t *testing.T) {
	doc := NewDocumentCRDTState()
	_ = doc.SetField("address.city", "NYC", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	_ = doc.SetField("address.zip", "10001", HLC{Timestamp: 2, NodeID: "node-1"}, "node-1")
	_ = doc.SetField("name", "Alice", HLC{Timestamp: 3, NodeID: "node-1"}, "node-1")

	doc.DeleteField("address")

	if doc.GetField("address.city") != nil {
		t.Error("expected address.city to be deleted")
	}
	if doc.GetField("address.zip") != nil {
		t.Error("expected address.zip to be deleted")
	}
	if doc.GetField("name") == nil {
		t.Error("expected name to still exist")
	}
}

func TestDocumentCRDT_CounterField(t *testing.T) {
	doc := NewDocumentCRDTState()

	counter := NewPNCounterState()
	counter.Increment("node-1", 5)
	doc.SetFieldState("views", counter.ToFieldState(HLC{Timestamp: 1, NodeID: "node-1"}, "node-1"))

	resolved := doc.Resolve()
	if resolved["views"] != int64(5) {
		t.Errorf("expected views=5, got %v", resolved["views"])
	}
}

func TestMergeDocument_TwoNodes(t *testing.T) {
	d1 := NewDocumentCRDTState()
	_ = d1.SetField("name", "Alice", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	_ = d1.SetField("email", "alice@example.com", HLC{Timestamp: 2, NodeID: "node-1"}, "node-1")

	d2 := NewDocumentCRDTState()
	_ = d2.SetField("name", "Bob", HLC{Timestamp: 3, NodeID: "node-2"}, "node-2")
	_ = d2.SetField("phone", "555-1234", HLC{Timestamp: 4, NodeID: "node-2"}, "node-2")

	merged, err := MergeDocument(d1, d2)
	if err != nil {
		t.Fatal(err)
	}

	resolved := merged.Resolve()

	// name: node-2 wins (ts=3 > ts=1)
	if resolved["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", resolved["name"])
	}
	// email: only in d1
	if resolved["email"] != "alice@example.com" {
		t.Errorf("expected email from d1, got %v", resolved["email"])
	}
	// phone: only in d2
	if resolved["phone"] != "555-1234" {
		t.Errorf("expected phone from d2, got %v", resolved["phone"])
	}
}

func TestMergeDocument_NestedPaths(t *testing.T) {
	d1 := NewDocumentCRDTState()
	_ = d1.SetField("address.city", "NYC", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	_ = d1.SetField("address.zip", "10001", HLC{Timestamp: 2, NodeID: "node-1"}, "node-1")

	d2 := NewDocumentCRDTState()
	_ = d2.SetField("address.city", "LA", HLC{Timestamp: 3, NodeID: "node-2"}, "node-2")
	_ = d2.SetField("address.state", "CA", HLC{Timestamp: 4, NodeID: "node-2"}, "node-2")

	merged, err := MergeDocument(d1, d2)
	if err != nil {
		t.Fatal(err)
	}

	resolved := merged.Resolve()
	addr := resolved["address"].(map[string]any)

	if addr["city"] != "LA" {
		t.Errorf("expected city=LA (d2 wins), got %v", addr["city"])
	}
	if addr["zip"] != "10001" {
		t.Errorf("expected zip=10001 (only in d1), got %v", addr["zip"])
	}
	if addr["state"] != "CA" {
		t.Errorf("expected state=CA (only in d2), got %v", addr["state"])
	}
}

func TestMergeDocument_Commutative(t *testing.T) {
	d1 := NewDocumentCRDTState()
	_ = d1.SetField("a", "1", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	d2 := NewDocumentCRDTState()
	_ = d2.SetField("b", "2", HLC{Timestamp: 2, NodeID: "node-2"}, "node-2")

	ab, err1 := MergeDocument(d1, d2)
	ba, err2 := MergeDocument(d2, d1)

	if err1 != nil || err2 != nil {
		t.Fatal("merge errors")
	}

	if len(ab.Fields) != len(ba.Fields) {
		t.Errorf("merge not commutative: %d vs %d fields", len(ab.Fields), len(ba.Fields))
	}
}

func TestMergeDocument_NilHandling(t *testing.T) {
	d := NewDocumentCRDTState()
	_ = d.SetField("a", "1", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	m1, err := MergeDocument(nil, d)
	if err != nil || m1 == nil {
		t.Error("nil local should return remote")
	}

	m2, err := MergeDocument(d, nil)
	if err != nil || m2 == nil {
		t.Error("nil remote should return local")
	}
}

func TestDocumentCRDT_ToFieldState(t *testing.T) {
	doc := NewDocumentCRDTState()
	_ = doc.SetField("title", "Hello", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	fs := doc.ToFieldState(HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	if fs == nil {
		t.Fatal("expected non-nil FieldState")
	}
	if fs.Type != TypeDocument {
		t.Errorf("expected TypeDocument, got %s", fs.Type)
	}
	if fs.DocState == nil {
		t.Error("expected DocState to be set")
	}

	restored := DocumentFromFieldState(fs)
	if restored == nil {
		t.Fatal("expected non-nil restored doc")
	}

	resolved := restored.Resolve()
	if resolved["title"] != "Hello" {
		t.Errorf("expected title=Hello, got %v", resolved["title"])
	}
}

func TestMergeEngine_MergeField_Document(t *testing.T) {
	engine := NewMergeEngine()

	d1 := NewDocumentCRDTState()
	_ = d1.SetField("a", "1", HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")
	fs1 := d1.ToFieldState(HLC{Timestamp: 1, NodeID: "node-1"}, "node-1")

	d2 := NewDocumentCRDTState()
	_ = d2.SetField("b", "2", HLC{Timestamp: 2, NodeID: "node-2"}, "node-2")
	fs2 := d2.ToFieldState(HLC{Timestamp: 2, NodeID: "node-2"}, "node-2")

	merged, err := engine.MergeField(fs1, fs2)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Type != TypeDocument {
		t.Errorf("expected TypeDocument, got %s", merged.Type)
	}
	if len(merged.DocState.Fields) != 2 {
		t.Errorf("expected 2 fields in merged doc, got %d", len(merged.DocState.Fields))
	}
}

func TestDocumentCRDT_MixedTypes(t *testing.T) {
	doc := NewDocumentCRDTState()
	_ = doc.SetField("name", "Project X", HLC{Timestamp: 1, NodeID: "n1"}, "n1")

	counter := NewPNCounterState()
	counter.Increment("n1", 42)
	doc.SetFieldState("stats.views", counter.ToFieldState(HLC{Timestamp: 2, NodeID: "n1"}, "n1"))

	set := NewORSetState()
	_ = set.Add("tag1", "n1", HLC{Timestamp: 3, NodeID: "n1"})
	_ = set.Add("tag2", "n1", HLC{Timestamp: 4, NodeID: "n1"})
	doc.SetFieldState("tags", set.ToFieldState(HLC{Timestamp: 4, NodeID: "n1"}, "n1"))

	resolved := doc.Resolve()

	if resolved["name"] != "Project X" {
		t.Errorf("expected name=Project X, got %v", resolved["name"])
	}

	stats := resolved["stats"].(map[string]any)
	if stats["views"] != int64(42) {
		t.Errorf("expected views=42, got %v", stats["views"])
	}

	tags := resolved["tags"]
	if tags == nil {
		t.Error("expected tags to be present")
	}
}
