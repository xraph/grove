package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// --- Mock SyncHook implementations ---

// recordingSyncHook records all method invocations for verification.
type recordingSyncHook struct {
	BaseSyncHook
	inboundCalls  []*ChangeRecord
	outboundCalls []*ChangeRecord
	afterCalls    []*ChangeRecord
	readCalls     int
}

func (h *recordingSyncHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	h.inboundCalls = append(h.inboundCalls, c)
	return c, nil
}

func (h *recordingSyncHook) AfterInboundChange(_ context.Context, c *ChangeRecord) error {
	h.afterCalls = append(h.afterCalls, c)
	return nil
}

func (h *recordingSyncHook) BeforeOutboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	h.outboundCalls = append(h.outboundCalls, c)
	return c, nil
}

func (h *recordingSyncHook) BeforeOutboundRead(_ context.Context, cs []ChangeRecord) ([]ChangeRecord, error) {
	h.readCalls++
	return cs, nil
}

// filteringSyncHook filters out changes for a specific table.
type filteringSyncHook struct {
	BaseSyncHook
	blockedTable string
}

func (h *filteringSyncHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	if c.Table == h.blockedTable {
		return nil, nil // Skip this change.
	}
	return c, nil
}

func (h *filteringSyncHook) BeforeOutboundRead(_ context.Context, cs []ChangeRecord) ([]ChangeRecord, error) {
	var filtered []ChangeRecord
	for _, c := range cs {
		if c.Table != h.blockedTable {
			filtered = append(filtered, c)
		}
	}
	return filtered, nil
}

// rejectingSyncHook returns an error for changes from a specific node.
type rejectingSyncHook struct {
	BaseSyncHook
	blockedNode string
}

func (h *rejectingSyncHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	if c.NodeID == h.blockedNode {
		return nil, errors.New("rejected: untrusted node")
	}
	return c, nil
}

// transformingSyncHook modifies change values.
type transformingSyncHook struct {
	BaseSyncHook
	suffix string
}

func (h *transformingSyncHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	// Append suffix to the value (if it's a string).
	var s string
	if json.Unmarshal(c.Value, &s) == nil {
		modified := *c
		modified.Value, _ = json.Marshal(s + h.suffix)
		return &modified, nil
	}
	return c, nil
}

// --- BaseSyncHook Tests ---

func TestBaseSyncHook_NoOpDefaults(t *testing.T) {
	var h BaseSyncHook
	ctx := context.Background()
	change := &ChangeRecord{Table: "t", PK: "1", NodeID: "n"}

	// BeforeInboundChange should pass through.
	got, err := h.BeforeInboundChange(ctx, change)
	if err != nil {
		t.Errorf("BeforeInboundChange error: %v", err)
	}
	if got != change {
		t.Error("BeforeInboundChange should return the same change")
	}

	// AfterInboundChange should return nil.
	if err = h.AfterInboundChange(ctx, change); err != nil {
		t.Errorf("AfterInboundChange error: %v", err)
	}

	// BeforeOutboundChange should pass through.
	got, err = h.BeforeOutboundChange(ctx, change)
	if err != nil {
		t.Errorf("BeforeOutboundChange error: %v", err)
	}
	if got != change {
		t.Error("BeforeOutboundChange should return the same change")
	}

	// BeforeOutboundRead should pass through.
	changes := []ChangeRecord{*change}
	gotSlice, err := h.BeforeOutboundRead(ctx, changes)
	if err != nil {
		t.Errorf("BeforeOutboundRead error: %v", err)
	}
	if len(gotSlice) != 1 {
		t.Errorf("BeforeOutboundRead should return same slice, got %d elements", len(gotSlice))
	}
}

// --- SyncHookChain Tests ---

func TestSyncHookChain_BeforeInboundChange_MultipleHooks(t *testing.T) {
	h1 := &recordingSyncHook{}
	h2 := &recordingSyncHook{}
	chain := NewSyncHookChain(h1, h2)

	change := &ChangeRecord{Table: "docs", PK: "1", NodeID: "node-1"}
	got, err := chain.BeforeInboundChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected non-nil change")
	}
	if len(h1.inboundCalls) != 1 {
		t.Errorf("hook1 should be called once, got %d", len(h1.inboundCalls))
	}
	if len(h2.inboundCalls) != 1 {
		t.Errorf("hook2 should be called once, got %d", len(h2.inboundCalls))
	}
}

func TestSyncHookChain_BeforeInboundChange_NilSkipsChange(t *testing.T) {
	filter := &filteringSyncHook{blockedTable: "secret"}
	recorder := &recordingSyncHook{}
	chain := NewSyncHookChain(filter, recorder)

	change := &ChangeRecord{Table: "secret", PK: "1", NodeID: "node-1"}
	got, err := chain.BeforeInboundChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil (skipped change)")
	}

	// Second hook should NOT be called because first returned nil.
	if len(recorder.inboundCalls) != 0 {
		t.Errorf("recorder should not be called after filter returns nil, got %d calls", len(recorder.inboundCalls))
	}
}

func TestSyncHookChain_BeforeInboundChange_ErrorAborts(t *testing.T) {
	rejecter := &rejectingSyncHook{blockedNode: "evil-node"}
	recorder := &recordingSyncHook{}
	chain := NewSyncHookChain(rejecter, recorder)

	change := &ChangeRecord{Table: "docs", PK: "1", NodeID: "evil-node"}
	_, err := chain.BeforeInboundChange(context.Background(), change)
	if err == nil {
		t.Fatal("expected error from rejecting hook")
	}
	if err.Error() != "rejected: untrusted node" {
		t.Errorf("unexpected error: %v", err)
	}

	// Second hook should NOT be called after error.
	if len(recorder.inboundCalls) != 0 {
		t.Errorf("recorder should not be called after error, got %d calls", len(recorder.inboundCalls))
	}
}

func TestSyncHookChain_BeforeInboundChange_TransformChain(t *testing.T) {
	// Two transforming hooks that each append a suffix.
	h1 := &transformingSyncHook{suffix: "_first"}
	h2 := &transformingSyncHook{suffix: "_second"}
	chain := NewSyncHookChain(h1, h2)

	change := &ChangeRecord{
		Table:  "docs",
		PK:     "1",
		NodeID: "n",
		Value:  json.RawMessage(`"hello"`),
	}
	got, err := chain.BeforeInboundChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	var val string
	_ = json.Unmarshal(got.Value, &val)
	if val != "hello_first_second" {
		t.Errorf("expected 'hello_first_second', got %q", val)
	}
}

func TestSyncHookChain_AfterInboundChange_AllHooksCalled(t *testing.T) {
	h1 := &recordingSyncHook{}
	h2 := &recordingSyncHook{}
	chain := NewSyncHookChain(h1, h2)

	change := &ChangeRecord{Table: "docs", PK: "1"}
	err := chain.AfterInboundChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}

	if len(h1.afterCalls) != 1 {
		t.Errorf("hook1 AfterInboundChange should be called once, got %d", len(h1.afterCalls))
	}
	if len(h2.afterCalls) != 1 {
		t.Errorf("hook2 AfterInboundChange should be called once, got %d", len(h2.afterCalls))
	}
}

func TestSyncHookChain_AfterInboundChange_ErrorsContinue(t *testing.T) {
	// AfterInboundChange should call all hooks even if one errors.
	errHook := &errorAfterHook{err: errors.New("oops")}
	recorder := &recordingSyncHook{}
	chain := NewSyncHookChain(errHook, recorder)

	change := &ChangeRecord{Table: "docs", PK: "1"}
	err := chain.AfterInboundChange(context.Background(), change)
	if err == nil {
		t.Fatal("expected error")
	}

	// Second hook should still be called.
	if len(recorder.afterCalls) != 1 {
		t.Errorf("recorder should be called even after first hook errors, got %d calls", len(recorder.afterCalls))
	}
}

func TestSyncHookChain_BeforeOutboundChange_ChainPassThrough(t *testing.T) {
	h1 := &recordingSyncHook{}
	h2 := &recordingSyncHook{}
	chain := NewSyncHookChain(h1, h2)

	change := &ChangeRecord{Table: "docs", PK: "1", NodeID: "n"}
	got, err := chain.BeforeOutboundChange(context.Background(), change)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if len(h1.outboundCalls) != 1 || len(h2.outboundCalls) != 1 {
		t.Error("both hooks should be called")
	}
}

func TestSyncHookChain_BeforeOutboundRead_FilterReducesSlice(t *testing.T) {
	filter := &filteringSyncHook{blockedTable: "secret"}
	chain := NewSyncHookChain(filter)

	changes := []ChangeRecord{
		{Table: "docs", PK: "1"},
		{Table: "secret", PK: "2"},
		{Table: "docs", PK: "3"},
	}
	got, err := chain.BeforeOutboundRead(context.Background(), changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 changes after filter, got %d", len(got))
	}
	for _, c := range got {
		if c.Table == "secret" {
			t.Error("secret table should be filtered out")
		}
	}
}

func TestSyncHookChain_EmptyChain_PassesThrough(t *testing.T) {
	chain := NewSyncHookChain()
	ctx := context.Background()
	change := &ChangeRecord{Table: "t", PK: "1"}

	got, err := chain.BeforeInboundChange(ctx, change)
	if err != nil || got != change {
		t.Error("empty chain should pass through BeforeInboundChange")
	}

	if err = chain.AfterInboundChange(ctx, change); err != nil {
		t.Error("empty chain should pass through AfterInboundChange")
	}

	got, err = chain.BeforeOutboundChange(ctx, change)
	if err != nil || got != change {
		t.Error("empty chain should pass through BeforeOutboundChange")
	}

	changes := []ChangeRecord{*change}
	gotSlice, err := chain.BeforeOutboundRead(ctx, changes)
	if err != nil || len(gotSlice) != 1 {
		t.Error("empty chain should pass through BeforeOutboundRead")
	}
}

func TestSyncHookChain_NilChain_PassesThrough(t *testing.T) {
	var chain *SyncHookChain
	ctx := context.Background()
	change := &ChangeRecord{Table: "t", PK: "1"}

	got, err := chain.BeforeInboundChange(ctx, change)
	if err != nil || got != change {
		t.Error("nil chain should pass through BeforeInboundChange")
	}

	if err = chain.AfterInboundChange(ctx, change); err != nil {
		t.Error("nil chain should pass through AfterInboundChange")
	}

	got, err = chain.BeforeOutboundChange(ctx, change)
	if err != nil || got != change {
		t.Error("nil chain should pass through BeforeOutboundChange")
	}

	changes := []ChangeRecord{*change}
	gotSlice, err := chain.BeforeOutboundRead(ctx, changes)
	if err != nil || len(gotSlice) != 1 {
		t.Error("nil chain should pass through BeforeOutboundRead")
	}
}

func TestSyncHookChain_Add_AppendsHook(t *testing.T) {
	chain := NewSyncHookChain()
	if chain.Len() != 0 {
		t.Errorf("expected 0 hooks, got %d", chain.Len())
	}

	chain.Add(&recordingSyncHook{})
	if chain.Len() != 1 {
		t.Errorf("expected 1 hook, got %d", chain.Len())
	}

	chain.Add(&recordingSyncHook{})
	if chain.Len() != 2 {
		t.Errorf("expected 2 hooks, got %d", chain.Len())
	}

	// Adding nil should not increase count.
	chain.Add(nil)
	if chain.Len() != 2 {
		t.Errorf("expected 2 hooks after adding nil, got %d", chain.Len())
	}
}

func TestSyncHookChain_Len_NilChain(t *testing.T) {
	var chain *SyncHookChain
	if chain.Len() != 0 {
		t.Errorf("nil chain should have len 0, got %d", chain.Len())
	}
}

// --- Helper mock ---

type errorAfterHook struct {
	BaseSyncHook
	err error
}

func (h *errorAfterHook) AfterInboundChange(_ context.Context, _ *ChangeRecord) error {
	return h.err
}
