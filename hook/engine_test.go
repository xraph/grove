package hook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock hooks
// ---------------------------------------------------------------------------

// mockPreQueryHook records calls and returns a configurable decision.
type mockPreQueryHook struct {
	called   bool
	calledQC *QueryContext
	decision Decision
	filters  []ExtraFilter
	hookErr  error // error returned from the hook itself (second return)
	denyErr  error // error placed in HookResult.Error when Decision == Deny
}

func (m *mockPreQueryHook) BeforeQuery(_ context.Context, qc *QueryContext) (*HookResult, error) {
	m.called = true
	m.calledQC = qc
	if m.hookErr != nil {
		return nil, m.hookErr
	}
	return &HookResult{
		Decision: m.decision,
		Filters:  m.filters,
		Error:    m.denyErr,
	}, nil
}

// mockPostQueryHook records that AfterQuery was called and the result it saw.
type mockPostQueryHook struct {
	called     bool
	calledQC   *QueryContext
	seenResult any
	err        error
}

func (m *mockPostQueryHook) AfterQuery(_ context.Context, qc *QueryContext, result any) error {
	m.called = true
	m.calledQC = qc
	m.seenResult = result
	return m.err
}

// mockPreMutationHook records calls for BeforeMutation.
type mockPreMutationHook struct {
	called   bool
	calledQC *QueryContext
	seenData any
	decision Decision
	filters  []ExtraFilter
	hookErr  error
	denyErr  error
}

func (m *mockPreMutationHook) BeforeMutation(_ context.Context, qc *QueryContext, data any) (*HookResult, error) {
	m.called = true
	m.calledQC = qc
	m.seenData = data
	if m.hookErr != nil {
		return nil, m.hookErr
	}
	return &HookResult{
		Decision: m.decision,
		Filters:  m.filters,
		Error:    m.denyErr,
	}, nil
}

// mockPostMutationHook records calls for AfterMutation.
type mockPostMutationHook struct {
	called     bool
	calledQC   *QueryContext
	seenData   any
	seenResult any
	err        error
}

func (m *mockPostMutationHook) AfterMutation(_ context.Context, qc *QueryContext, data any, result any) error {
	m.called = true
	m.calledQC = qc
	m.seenData = data
	m.seenResult = result
	return m.err
}

// orderRecorder is a pre-query hook that appends its id to a shared slice when called.
// Used for verifying execution order.
type orderRecorder struct {
	id    string
	order *[]string
}

func (o *orderRecorder) BeforeQuery(_ context.Context, _ *QueryContext) (*HookResult, error) {
	*o.order = append(*o.order, o.id)
	return &HookResult{Decision: Allow}, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestEngine_AddHook_PreQuery(t *testing.T) {
	eng := NewEngine()
	h := &mockPreQueryHook{decision: Allow}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.True(t, h.called, "hook should have been called")
	assert.Equal(t, Allow, result.Decision)
	assert.Equal(t, qc, h.calledQC, "hook should receive the QueryContext")
}

func TestEngine_PreQuery_Deny(t *testing.T) {
	eng := NewEngine()
	denyErr := errors.New("access denied")
	h := &mockPreQueryHook{decision: Deny, denyErr: denyErr}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "secrets"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	assert.True(t, h.called)
	assert.Equal(t, Deny, result.Decision)
	assert.ErrorIs(t, err, denyErr)
}

func TestEngine_PreQuery_Modify(t *testing.T) {
	eng := NewEngine()

	f1 := ExtraFilter{Clause: "tenant_id = $1", Args: []any{42}}
	h1 := &mockPreQueryHook{decision: Modify, filters: []ExtraFilter{f1}}

	f2 := ExtraFilter{Clause: "deleted_at IS NULL"}
	h2 := &mockPreQueryHook{decision: Modify, filters: []ExtraFilter{f2}}

	eng.AddHook(h1)
	eng.AddHook(h2)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.True(t, h1.called)
	assert.True(t, h2.called)
	assert.Equal(t, Modify, result.Decision)
	require.Len(t, result.Filters, 2)
	assert.Equal(t, "tenant_id = $1", result.Filters[0].Clause)
	assert.Equal(t, "deleted_at IS NULL", result.Filters[1].Clause)
}

func TestEngine_PostQuery(t *testing.T) {
	eng := NewEngine()
	h := &mockPostQueryHook{}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	rows := []string{"alice", "bob"}

	err := eng.RunPostQuery(context.Background(), qc, rows)

	require.NoError(t, err)
	assert.True(t, h.called)
	assert.Equal(t, qc, h.calledQC)
	assert.Equal(t, rows, h.seenResult)
}

func TestEngine_PostQuery_Error(t *testing.T) {
	eng := NewEngine()
	h := &mockPostQueryHook{err: errors.New("post-query failed")}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	err := eng.RunPostQuery(context.Background(), qc, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "post-query")
}

func TestEngine_PreMutation(t *testing.T) {
	eng := NewEngine()
	h := &mockPreMutationHook{decision: Allow}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpInsert, Table: "users"}
	data := map[string]any{"name": "alice"}

	result, err := eng.RunPreMutation(context.Background(), qc, data)

	require.NoError(t, err)
	assert.True(t, h.called)
	assert.Equal(t, qc, h.calledQC)
	assert.Equal(t, data, h.seenData)
	assert.Equal(t, Allow, result.Decision)
}

func TestEngine_PreMutation_Deny(t *testing.T) {
	eng := NewEngine()
	denyErr := errors.New("mutation denied")
	h := &mockPreMutationHook{decision: Deny, denyErr: denyErr}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpDelete, Table: "users"}
	result, err := eng.RunPreMutation(context.Background(), qc, nil)

	assert.True(t, h.called)
	assert.Equal(t, Deny, result.Decision)
	assert.ErrorIs(t, err, denyErr)
}

func TestEngine_PostMutation(t *testing.T) {
	eng := NewEngine()
	h := &mockPostMutationHook{}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpInsert, Table: "orders"}
	data := map[string]any{"item": "widget"}
	mutResult := map[string]any{"id": 1}

	err := eng.RunPostMutation(context.Background(), qc, data, mutResult)

	require.NoError(t, err)
	assert.True(t, h.called)
	assert.Equal(t, qc, h.calledQC)
	assert.Equal(t, data, h.seenData)
	assert.Equal(t, mutResult, h.seenResult)
}

func TestEngine_PostMutation_Error(t *testing.T) {
	eng := NewEngine()
	h := &mockPostMutationHook{err: errors.New("audit log failed")}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpUpdate, Table: "users"}
	err := eng.RunPostMutation(context.Background(), qc, nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "post-mutation")
}

func TestEngine_ScopeFiltering_Tables(t *testing.T) {
	eng := NewEngine()
	h := &mockPreQueryHook{decision: Allow}
	eng.AddHook(h, Scope{Tables: []string{"users"}, Priority: 1})

	// Query on "posts" should not trigger the hook scoped to "users".
	qc := &QueryContext{Operation: OpSelect, Table: "posts"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.False(t, h.called, "hook scoped to 'users' should NOT fire for 'posts'")
	assert.Equal(t, Allow, result.Decision)

	// Query on "users" should trigger the hook.
	qc2 := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err = eng.RunPreQuery(context.Background(), qc2)

	require.NoError(t, err)
	assert.True(t, h.called, "hook scoped to 'users' SHOULD fire for 'users'")
}

func TestEngine_ScopeFiltering_Operations(t *testing.T) {
	eng := NewEngine()
	h := &mockPreQueryHook{decision: Allow}
	eng.AddHook(h, Scope{Operations: []Operation{OpSelect}, Priority: 1})

	// OpInsert should not match a hook scoped to OpSelect.
	qc := &QueryContext{Operation: OpInsert, Table: "users"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.False(t, h.called, "hook scoped to OpSelect should NOT fire for OpInsert")
	assert.Equal(t, Allow, result.Decision)

	// OpSelect should match.
	qc2 := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err = eng.RunPreQuery(context.Background(), qc2)

	require.NoError(t, err)
	assert.True(t, h.called, "hook scoped to OpSelect SHOULD fire for OpSelect")
}

func TestEngine_Priority(t *testing.T) {
	eng := NewEngine()
	var order []string

	// Add hooks in reverse priority order to confirm sorting.
	eng.AddHook(&orderRecorder{id: "C", order: &order}, Scope{Priority: 300})
	eng.AddHook(&orderRecorder{id: "A", order: &order}, Scope{Priority: 1})
	eng.AddHook(&orderRecorder{id: "B", order: &order}, Scope{Priority: 50})

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B", "C"}, order, "hooks should run in priority order (lower first)")
}

func TestEngine_GlobalHook(t *testing.T) {
	eng := NewEngine()
	h := &mockPreQueryHook{decision: Allow}
	// No scope means global -- should match every table and operation.
	eng.AddHook(h)

	tables := []string{"users", "posts", "orders", "sessions"}
	ops := []Operation{OpSelect, OpInsert, OpUpdate, OpDelete}

	for _, table := range tables {
		for _, op := range ops {
			h.called = false
			qc := &QueryContext{Operation: op, Table: table}
			_, err := eng.RunPreQuery(context.Background(), qc)

			require.NoError(t, err)
			assert.True(t, h.called, "global hook should fire for table=%s op=%s", table, op.String())
		}
	}
}

func TestEngine_MultipleHooks(t *testing.T) {
	eng := NewEngine()
	var order []string

	h1 := &orderRecorder{id: "first", order: &order}
	h2 := &orderRecorder{id: "second", order: &order}
	h3 := &orderRecorder{id: "third", order: &order}

	// All same priority -- should execute in registration order (stable sort).
	eng.AddHook(h1)
	eng.AddHook(h2)
	eng.AddHook(h3)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err := eng.RunPreQuery(context.Background(), qc)

	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second", "third"}, order, "hooks with equal priority should run in registration order")
}

func TestEngine_PreQuery_HookError(t *testing.T) {
	eng := NewEngine()
	h := &mockPreQueryHook{hookErr: errors.New("internal hook failure")}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	result, err := eng.RunPreQuery(context.Background(), qc)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pre-query")
	assert.Contains(t, err.Error(), "internal hook failure")
}

func TestEngine_PreMutation_HookError(t *testing.T) {
	eng := NewEngine()
	h := &mockPreMutationHook{hookErr: errors.New("internal mutation failure")}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpInsert, Table: "users"}
	result, err := eng.RunPreMutation(context.Background(), qc, nil)

	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pre-mutation")
}

func TestEngine_DenyStopsChain(t *testing.T) {
	eng := NewEngine()
	denyErr := errors.New("denied")
	h1 := &mockPreQueryHook{decision: Deny, denyErr: denyErr}
	h2 := &mockPreQueryHook{decision: Allow}

	eng.AddHook(h1, Scope{Priority: 1})
	eng.AddHook(h2, Scope{Priority: 50})

	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err := eng.RunPreQuery(context.Background(), qc)

	assert.ErrorIs(t, err, denyErr)
	assert.True(t, h1.called, "first hook should have been called")
	assert.False(t, h2.called, "second hook should NOT run after Deny")
}

func TestEngine_NoHooksRegistered(t *testing.T) {
	eng := NewEngine()
	qc := &QueryContext{Operation: OpSelect, Table: "users"}

	result, err := eng.RunPreQuery(context.Background(), qc)
	require.NoError(t, err)
	assert.Equal(t, Allow, result.Decision)

	err = eng.RunPostQuery(context.Background(), qc, nil)
	require.NoError(t, err)

	mResult, err := eng.RunPreMutation(context.Background(), qc, nil)
	require.NoError(t, err)
	assert.Equal(t, Allow, mResult.Decision)

	err = eng.RunPostMutation(context.Background(), qc, nil, nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Mock StreamRowHook
// ---------------------------------------------------------------------------

// mockStreamRowHook implements StreamRowHook and records calls.
type mockStreamRowHook struct {
	called   bool
	calledQC *QueryContext
	seenRow  any
	decision Decision
	hookErr  error
}

func (m *mockStreamRowHook) OnStreamRow(_ context.Context, qc *QueryContext, row any) (Decision, error) {
	m.called = true
	m.calledQC = qc
	m.seenRow = row
	if m.hookErr != nil {
		return Deny, m.hookErr
	}
	return m.decision, nil
}

// ---------------------------------------------------------------------------
// StreamRowHook tests
// ---------------------------------------------------------------------------

func TestEngine_RunStreamRowHook_Allow(t *testing.T) {
	eng := NewEngine()
	h := &mockStreamRowHook{decision: Allow}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	row := map[string]any{"id": 1, "name": "test"}

	dec, err := eng.RunStreamRowHook(context.Background(), qc, row)

	require.NoError(t, err)
	assert.Equal(t, int(Allow), dec, "Allow decision should let iteration continue")
	assert.True(t, h.called)
	assert.Equal(t, qc, h.calledQC)
	assert.Equal(t, row, h.seenRow)
}

func TestEngine_RunStreamRowHook_Skip(t *testing.T) {
	eng := NewEngine()
	h := &mockStreamRowHook{decision: Skip}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Skip), dec, "Skip decision should return Skip")
	assert.True(t, h.called)
}

func TestEngine_RunStreamRowHook_Deny(t *testing.T) {
	eng := NewEngine()
	h := &mockStreamRowHook{decision: Deny}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Deny), dec, "Deny decision should return Deny")
	assert.True(t, h.called)
}

func TestEngine_RunStreamRowHook_Error(t *testing.T) {
	eng := NewEngine()
	hookErr := errors.New("permission check failed")
	h := &mockStreamRowHook{decision: Deny, hookErr: hookErr}
	eng.AddHook(h)

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	assert.Equal(t, int(Deny), dec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream-row")
	assert.ErrorIs(t, err, hookErr)
}

func TestEngine_RunStreamRowHook_ScopeFiltering_Tables(t *testing.T) {
	eng := NewEngine()
	h := &mockStreamRowHook{decision: Skip}
	eng.AddHook(h, Scope{Tables: []string{"events"}})

	// Query on "users" should NOT trigger the hook scoped to "events".
	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Allow), dec, "hook scoped to 'events' should not fire for 'users'")
	assert.False(t, h.called)

	// Query on "events" SHOULD trigger the hook.
	qc2 := &QueryContext{Operation: OpSelect, Table: "events"}
	dec2, err := eng.RunStreamRowHook(context.Background(), qc2, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Skip), dec2, "hook scoped to 'events' should fire for 'events'")
	assert.True(t, h.called)
}

func TestEngine_RunStreamRowHook_ScopeFiltering_Operations(t *testing.T) {
	eng := NewEngine()
	h := &mockStreamRowHook{decision: Skip}
	eng.AddHook(h, Scope{Operations: []Operation{OpSelect}})

	// OpInsert should NOT match a hook scoped to OpSelect.
	qc := &QueryContext{Operation: OpInsert, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Allow), dec)
	assert.False(t, h.called)

	// OpSelect SHOULD match.
	qc2 := &QueryContext{Operation: OpSelect, Table: "events"}
	dec2, err := eng.RunStreamRowHook(context.Background(), qc2, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Skip), dec2)
	assert.True(t, h.called)
}

func TestEngine_RunStreamRowHook_NoHooks(t *testing.T) {
	eng := NewEngine()

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Allow), dec, "no hooks registered should default to Allow")
}

func TestEngine_RunStreamRowHook_MultipleHooks_FirstDenies(t *testing.T) {
	eng := NewEngine()

	h1 := &mockStreamRowHook{decision: Deny}
	h2 := &mockStreamRowHook{decision: Allow}

	eng.AddHook(h1, Scope{Priority: 1})
	eng.AddHook(h2, Scope{Priority: 50})

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Deny), dec)
	assert.True(t, h1.called, "first hook should have been called")
	assert.False(t, h2.called, "second hook should NOT run after Deny")
}

func TestEngine_RunStreamRowHook_NonStreamHookIgnored(t *testing.T) {
	eng := NewEngine()

	// Register a PreQueryHook (not a StreamRowHook). It should be ignored.
	preQ := &mockPreQueryHook{decision: Allow}
	eng.AddHook(preQ)

	qc := &QueryContext{Operation: OpSelect, Table: "events"}
	dec, err := eng.RunStreamRowHook(context.Background(), qc, "row-data")

	require.NoError(t, err)
	assert.Equal(t, int(Allow), dec)
	assert.False(t, preQ.called, "PreQueryHook should not be invoked by RunStreamRowHook")
}

func TestEngine_MixedHookTypes(t *testing.T) {
	eng := NewEngine()

	preQ := &mockPreQueryHook{decision: Allow}
	postQ := &mockPostQueryHook{}
	preM := &mockPreMutationHook{decision: Allow}
	postM := &mockPostMutationHook{}

	eng.AddHook(preQ)
	eng.AddHook(postQ)
	eng.AddHook(preM)
	eng.AddHook(postM)

	// RunPreQuery should only invoke PreQueryHook, not the others.
	qc := &QueryContext{Operation: OpSelect, Table: "users"}
	_, err := eng.RunPreQuery(context.Background(), qc)
	require.NoError(t, err)

	assert.True(t, preQ.called)
	assert.False(t, postQ.called)
	assert.False(t, preM.called)
	assert.False(t, postM.called)
}
