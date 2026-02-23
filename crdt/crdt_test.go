package crdt

import (
	"encoding/json"
	"testing"
	"time"
)

// --- HLC Tests ---

func TestHLC_Compare(t *testing.T) {
	a := HLC{Timestamp: 100, Counter: 0, NodeID: "a"}
	b := HLC{Timestamp: 200, Counter: 0, NodeID: "b"}

	if a.Compare(b) != -1 {
		t.Error("expected a < b")
	}
	if b.Compare(a) != 1 {
		t.Error("expected b > a")
	}
	if a.Compare(a) != 0 {
		t.Error("expected a == a")
	}
}

func TestHLC_Compare_SameTimestamp(t *testing.T) {
	a := HLC{Timestamp: 100, Counter: 1, NodeID: "a"}
	b := HLC{Timestamp: 100, Counter: 2, NodeID: "b"}

	if a.Compare(b) != -1 {
		t.Error("expected counter tiebreak: a < b")
	}
}

func TestHLC_Compare_SameTimestampAndCounter(t *testing.T) {
	a := HLC{Timestamp: 100, Counter: 1, NodeID: "a"}
	b := HLC{Timestamp: 100, Counter: 1, NodeID: "b"}

	if a.Compare(b) != -1 {
		t.Error("expected node ID tiebreak: a < b")
	}
}

func TestHybridClock_Now(t *testing.T) {
	clock := NewHybridClock("node-1")
	h1 := clock.Now()
	h2 := clock.Now()

	if !h2.After(h1) {
		t.Errorf("expected h2 > h1, got h1=%s h2=%s", h1, h2)
	}
}

func TestHybridClock_Update(t *testing.T) {
	clock := NewHybridClock("node-1")
	local := clock.Now()

	// Simulate a remote clock that's ahead.
	remote := HLC{Timestamp: local.Timestamp + 1000000, Counter: 5, NodeID: "node-2"}
	clock.Update(remote)

	after := clock.Now()
	if !after.After(remote) {
		t.Errorf("expected after > remote, got after=%s remote=%s", after, remote)
	}
}

func TestHybridClock_MaxDrift(t *testing.T) {
	now := time.Now()
	clock := NewHybridClock("node-1",
		WithMaxDrift(1*time.Second),
		WithNowFunc(func() time.Time { return now }),
	)

	// Remote clock far in the future.
	remote := HLC{Timestamp: now.Add(10 * time.Second).UnixNano(), Counter: 0, NodeID: "node-2"}
	clock.Update(remote)

	after := clock.Now()
	maxAllowed := now.Add(1 * time.Second).UnixNano()
	if after.Timestamp > maxAllowed+1 { // +1 for counter increment
		t.Errorf("expected timestamp clamped to drift, got %d (max %d)", after.Timestamp, maxAllowed)
	}
}

// --- LWW Register Tests ---

func TestMergeLWW_RemoteWins(t *testing.T) {
	local := &LWWRegister{
		Value:  json.RawMessage(`"old"`),
		Clock:  HLC{Timestamp: 100, Counter: 0, NodeID: "a"},
		NodeID: "a",
	}
	remote := &LWWRegister{
		Value:  json.RawMessage(`"new"`),
		Clock:  HLC{Timestamp: 200, Counter: 0, NodeID: "b"},
		NodeID: "b",
	}

	result := MergeLWW(local, remote)
	var val string
	_ = json.Unmarshal(result.Value, &val)
	if val != "new" {
		t.Errorf("expected remote to win, got %q", val)
	}
}

func TestMergeLWW_LocalWins(t *testing.T) {
	local := &LWWRegister{
		Value:  json.RawMessage(`"keep"`),
		Clock:  HLC{Timestamp: 300, Counter: 0, NodeID: "a"},
		NodeID: "a",
	}
	remote := &LWWRegister{
		Value:  json.RawMessage(`"discard"`),
		Clock:  HLC{Timestamp: 200, Counter: 0, NodeID: "b"},
		NodeID: "b",
	}

	result := MergeLWW(local, remote)
	var val string
	_ = json.Unmarshal(result.Value, &val)
	if val != "keep" {
		t.Errorf("expected local to win, got %q", val)
	}
}

func TestMergeLWW_TiebreakByNodeID(t *testing.T) {
	local := &LWWRegister{
		Value:  json.RawMessage(`"from-a"`),
		Clock:  HLC{Timestamp: 100, Counter: 0, NodeID: "a"},
		NodeID: "a",
	}
	remote := &LWWRegister{
		Value:  json.RawMessage(`"from-b"`),
		Clock:  HLC{Timestamp: 100, Counter: 0, NodeID: "b"},
		NodeID: "b",
	}

	result := MergeLWW(local, remote)
	// "b" > "a" lexicographically, so remote wins.
	var val string
	_ = json.Unmarshal(result.Value, &val)
	if val != "from-b" {
		t.Errorf("expected node ID tiebreak to pick 'b', got %q", val)
	}
}

func TestMergeLWW_NilHandling(t *testing.T) {
	reg := &LWWRegister{Value: json.RawMessage(`"val"`), Clock: HLC{Timestamp: 1}, NodeID: "a"}

	if MergeLWW(nil, reg) != reg {
		t.Error("expected nil local to return remote")
	}
	if MergeLWW(reg, nil) != reg {
		t.Error("expected nil remote to return local")
	}
}

// --- PN-Counter Tests ---

func TestPNCounter_IncrementDecrement(t *testing.T) {
	c := NewPNCounterState()
	c.Increment("node-1", 10)
	c.Increment("node-2", 5)
	c.Decrement("node-1", 3)

	if v := c.Value(); v != 12 {
		t.Errorf("expected 12, got %d", v)
	}
}

func TestMergeCounter_TwoNodes(t *testing.T) {
	local := NewPNCounterState()
	local.Increment("node-1", 10)
	local.Decrement("node-1", 2)

	remote := NewPNCounterState()
	remote.Increment("node-2", 5)
	remote.Increment("node-1", 8) // Remote saw node-1 at 8 (less than local's 10).

	merged := MergeCounter(local, remote)

	// node-1 inc: max(10, 8) = 10
	// node-2 inc: 5
	// node-1 dec: 2
	// Total: 10 + 5 - 2 = 13
	if v := merged.Value(); v != 13 {
		t.Errorf("expected 13, got %d", v)
	}
}

func TestMergeCounter_Idempotent(t *testing.T) {
	a := NewPNCounterState()
	a.Increment("node-1", 10)

	merged1 := MergeCounter(a, a)
	merged2 := MergeCounter(merged1, a)

	if merged1.Value() != merged2.Value() {
		t.Errorf("merge not idempotent: %d vs %d", merged1.Value(), merged2.Value())
	}
}

func TestMergeCounter_Commutative(t *testing.T) {
	a := NewPNCounterState()
	a.Increment("node-1", 10)

	b := NewPNCounterState()
	b.Increment("node-2", 5)

	ab := MergeCounter(a, b)
	ba := MergeCounter(b, a)

	if ab.Value() != ba.Value() {
		t.Errorf("merge not commutative: %d vs %d", ab.Value(), ba.Value())
	}
}

func TestMergeCounter_NilHandling(t *testing.T) {
	c := NewPNCounterState()
	c.Increment("a", 5)

	if MergeCounter(nil, c).Value() != 5 {
		t.Error("nil local should return remote")
	}
	if MergeCounter(c, nil).Value() != 5 {
		t.Error("nil remote should return local")
	}
}

// --- OR-Set Tests ---

func TestORSet_AddAndContains(t *testing.T) {
	s := NewORSetState()
	_ = s.Add("hello", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})
	_ = s.Add("world", "node-1", HLC{Timestamp: 2, NodeID: "node-1"})

	has, _ := s.Contains("hello")
	if !has {
		t.Error("expected 'hello' to be in set")
	}

	elements := s.Elements()
	if len(elements) != 2 {
		t.Errorf("expected 2 elements, got %d", len(elements))
	}
}

func TestORSet_Remove(t *testing.T) {
	s := NewORSetState()
	_ = s.Add("hello", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})
	_ = s.Remove("hello")

	has, _ := s.Contains("hello")
	if has {
		t.Error("expected 'hello' to be removed")
	}
}

func TestORSet_ConcurrentAddRemove_AddWins(t *testing.T) {
	// Node-1 adds "x", Node-2 observes and removes "x",
	// Node-1 concurrently adds "x" again with a new tag.
	s1 := NewORSetState()
	_ = s1.Add("x", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	// Node-2 sees s1 and removes "x".
	s2 := MergeSet(NewORSetState(), s1)
	_ = s2.Remove("x")

	// Node-1 concurrently adds "x" again.
	_ = s1.Add("x", "node-1", HLC{Timestamp: 2, NodeID: "node-1"})

	// Merge: the new add should win.
	merged := MergeSet(s1, s2)
	has, _ := merged.Contains("x")
	if !has {
		t.Error("expected concurrent add to win over remove (add-wins semantics)")
	}
}

func TestMergeSet_Commutative(t *testing.T) {
	a := NewORSetState()
	_ = a.Add("x", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	b := NewORSetState()
	_ = b.Add("y", "node-2", HLC{Timestamp: 2, NodeID: "node-2"})

	ab := MergeSet(a, b)
	ba := MergeSet(b, a)

	abElems := ab.Elements()
	baElems := ba.Elements()

	if len(abElems) != len(baElems) {
		t.Errorf("merge not commutative: %d vs %d elements", len(abElems), len(baElems))
	}
}

func TestMergeSet_Idempotent(t *testing.T) {
	a := NewORSetState()
	_ = a.Add("x", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	merged1 := MergeSet(a, a)
	merged2 := MergeSet(merged1, a)

	if len(merged1.Elements()) != len(merged2.Elements()) {
		t.Errorf("merge not idempotent: %d vs %d elements", len(merged1.Elements()), len(merged2.Elements()))
	}
}

func TestMergeSet_NilHandling(t *testing.T) {
	s := NewORSetState()
	_ = s.Add("x", "node-1", HLC{Timestamp: 1, NodeID: "node-1"})

	if MergeSet(nil, s) == nil {
		t.Error("nil local should return remote")
	}
	if MergeSet(s, nil) == nil {
		t.Error("nil remote should return local")
	}
}

// --- Merge Engine Tests ---

func TestMergeEngine_MergeField_LWW(t *testing.T) {
	engine := NewMergeEngine()

	local := &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 100, NodeID: "a"},
		NodeID: "a",
		Value:  json.RawMessage(`"local"`),
	}
	remote := &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 200, NodeID: "b"},
		NodeID: "b",
		Value:  json.RawMessage(`"remote"`),
	}

	merged, err := engine.MergeField(local, remote)
	if err != nil {
		t.Fatal(err)
	}

	var val string
	_ = json.Unmarshal(merged.Value, &val)
	if val != "remote" {
		t.Errorf("expected 'remote', got %q", val)
	}
}

func TestMergeEngine_MergeField_TypeMismatch(t *testing.T) {
	engine := NewMergeEngine()

	local := &FieldState{Type: TypeLWW}
	remote := &FieldState{Type: TypeCounter}

	_, err := engine.MergeField(local, remote)
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestMergeEngine_MergeState(t *testing.T) {
	engine := NewMergeEngine()

	local := NewState("users", "1")
	local.Fields["name"] = &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 100, NodeID: "a"},
		NodeID: "a",
		Value:  json.RawMessage(`"Alice"`),
	}

	remote := NewState("users", "1")
	remote.Fields["name"] = &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 200, NodeID: "b"},
		NodeID: "b",
		Value:  json.RawMessage(`"Bob"`),
	}
	remote.Fields["email"] = &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 150, NodeID: "b"},
		NodeID: "b",
		Value:  json.RawMessage(`"bob@example.com"`),
	}

	merged, err := engine.MergeState(local, remote)
	if err != nil {
		t.Fatal(err)
	}

	// name: remote wins (200 > 100).
	var name string
	_ = json.Unmarshal(merged.Fields["name"].Value, &name)
	if name != "Bob" {
		t.Errorf("expected 'Bob', got %q", name)
	}

	// email: only in remote, should be present.
	if _, ok := merged.Fields["email"]; !ok {
		t.Error("expected email field from remote")
	}
}

// --- Shadow Table DDL Tests ---

func TestShadowTableDDL(t *testing.T) {
	ddl := ShadowTableDDL("documents")
	if ddl == "" {
		t.Error("expected non-empty DDL")
	}
	if got := ShadowTableName("documents"); got != "_documents_crdt" {
		t.Errorf("expected _documents_crdt, got %s", got)
	}
}

// --- Inspect Tests ---

func TestInspectState(t *testing.T) {
	state := NewState("docs", "1")
	state.Fields["title"] = &FieldState{
		Type:   TypeLWW,
		HLC:    HLC{Timestamp: 100, NodeID: "a"},
		NodeID: "a",
		Value:  json.RawMessage(`"Hello"`),
	}

	counter := NewPNCounterState()
	counter.Increment("a", 10)
	counter.Increment("b", 5)
	state.Fields["views"] = counter.ToFieldState(HLC{Timestamp: 200, NodeID: "a"}, "a")

	result := InspectState(state)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Fields["title"].Value != "Hello" {
		t.Errorf("expected title 'Hello', got %v", result.Fields["title"].Value)
	}
	if result.Fields["views"].CounterValue != 15 {
		t.Errorf("expected counter value 15, got %d", result.Fields["views"].CounterValue)
	}

	// String should not panic.
	s := result.String()
	if s == "" {
		t.Error("expected non-empty string")
	}
}

// --- ValidCRDTType Tests ---

func TestValidCRDTType(t *testing.T) {
	if !ValidCRDTType("lww") {
		t.Error("lww should be valid")
	}
	if !ValidCRDTType("counter") {
		t.Error("counter should be valid")
	}
	if !ValidCRDTType("set") {
		t.Error("set should be valid")
	}
	if ValidCRDTType("invalid") {
		t.Error("invalid should not be valid")
	}
}
