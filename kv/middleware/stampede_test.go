package middleware_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/middleware"
)

func TestStampedeHook_OnlyGET(t *testing.T) {
	h := middleware.NewStampede()

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpSet,
		RawQuery:  "key1",
		Values:    make(map[string]any),
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasCall := qc.Values["_stampede_call"]
	_, hasKey := qc.Values["_stampede_key"]
	assert.False(t, hasCall, "non-GET operations should not track stampede calls")
	assert.False(t, hasKey, "non-GET operations should not track stampede keys")
}

func TestStampedeHook_TracksFirstRequest(t *testing.T) {
	h := middleware.NewStampede()

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key1",
		Values:    make(map[string]any),
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasCall := qc.Values["_stampede_call"]
	assert.True(t, hasCall, "first GET request should set _stampede_call")

	key, hasKey := qc.Values["_stampede_key"].(string)
	assert.True(t, hasKey, "first GET request should set _stampede_key")
	assert.Equal(t, "key1", key)

	// Clean up so the test does not hang.
	h.Complete("key1")
}

func TestStampedeHook_Complete_CleansUp(t *testing.T) {
	h := middleware.NewStampede()

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key1",
		Values:    make(map[string]any),
	}

	_, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)

	// Complete should clean up the in-flight call.
	h.Complete("key1")

	// A new request for the same key should be tracked as the first again.
	qc2 := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key1",
		Values:    make(map[string]any),
	}

	result, err := h.BeforeQuery(ctx, qc2)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasCall := qc2.Values["_stampede_call"]
	assert.True(t, hasCall, "after Complete, a new request should be the first caller again")

	h.Complete("key1")
}

func TestStampedeHook_DifferentKeys_NoSharing(t *testing.T) {
	h := middleware.NewStampede()

	ctx := context.Background()

	qc1 := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key-alpha",
		Values:    make(map[string]any),
	}
	_, err := h.BeforeQuery(ctx, qc1)
	require.NoError(t, err)

	qc2 := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key-beta",
		Values:    make(map[string]any),
	}
	_, err = h.BeforeQuery(ctx, qc2)
	require.NoError(t, err)

	// Both should be tracked as first callers (not shared).
	_, hasCall1 := qc1.Values["_stampede_call"]
	_, hasCall2 := qc2.Values["_stampede_call"]
	assert.True(t, hasCall1, "key-alpha should be tracked independently")
	assert.True(t, hasCall2, "key-beta should be tracked independently")

	key1 := qc1.Values["_stampede_key"].(string)
	key2 := qc2.Values["_stampede_key"].(string)
	assert.Equal(t, "key-alpha", key1)
	assert.Equal(t, "key-beta", key2)

	h.Complete("key-alpha")
	h.Complete("key-beta")
}
