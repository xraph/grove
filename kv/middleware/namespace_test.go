package middleware_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/kv/middleware"
)

func TestNamespaceHook_PrefixesKey(t *testing.T) {
	h := middleware.NewNamespace("tenant:acme")

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
		Values: map[string]any{
			"_kv_keys": []string{"key"},
		},
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Modify, result.Decision)

	keys := qc.Values["_kv_keys"].([]string)
	assert.Equal(t, []string{"tenant:acme:key"}, keys)
	assert.Equal(t, "tenant:acme:key", qc.RawQuery)
}

func TestNamespaceHook_CustomSeparator(t *testing.T) {
	h := middleware.NewNamespace("tenant", "/")

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpSet,
		RawQuery:  "key",
		Values: map[string]any{
			"_kv_keys": []string{"key"},
		},
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Modify, result.Decision)

	keys := qc.Values["_kv_keys"].([]string)
	assert.Equal(t, []string{"tenant/key"}, keys)
	assert.Equal(t, "tenant/key", qc.RawQuery)
}

func TestNamespaceHook_MultipleKeys(t *testing.T) {
	h := middleware.NewNamespace("ns")

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpMGet,
		RawQuery:  "k1",
		Values: map[string]any{
			"_kv_keys": []string{"k1", "k2", "k3"},
		},
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Modify, result.Decision)

	keys := qc.Values["_kv_keys"].([]string)
	assert.Equal(t, []string{"ns:k1", "ns:k2", "ns:k3"}, keys)
	assert.Equal(t, "ns:k1", qc.RawQuery)
}

func TestNamespaceHook_ReturnsModify(t *testing.T) {
	h := middleware.NewNamespace("pfx")

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "x",
		Values: map[string]any{
			"_kv_keys": []string{"x"},
		},
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Modify, result.Decision, "NamespaceHook should always return Modify decision")
}

func TestNamespaceHook_IntegrationWithStore(t *testing.T) {
	nsHook := middleware.NewNamespace("tenant:acme")
	store := kvtest.SetupStore(t, kv.WithHook(nsHook))

	ctx := context.Background()

	// Set a value through the store (namespace hook prefixes the key).
	err := store.Set(ctx, "user:1", "alice")
	require.NoError(t, err)

	// The driver should have the namespaced key.
	// Get through the store also applies the namespace prefix.
	var val string
	err = store.Get(ctx, "user:1", &val)
	require.NoError(t, err)
	assert.Equal(t, "alice", val)
}
