package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommandContext_SingleKey(t *testing.T) {
	qc := newCommandContext(OpGet, []string{"user:42"}, nil)

	assert.Equal(t, OpGet, qc.Operation)
	assert.Equal(t, "user:42", qc.RawQuery)

	keys, ok := qc.Values["_kv_keys"].([]string)
	require.True(t, ok)
	assert.Equal(t, []string{"user:42"}, keys)
}

func TestNewCommandContext_MultipleKeys(t *testing.T) {
	qc := newCommandContext(OpDelete, []string{"k1", "k2", "k3"}, nil)

	assert.Equal(t, OpDelete, qc.Operation)
	// RawQuery should be the first key.
	assert.Equal(t, "k1", qc.RawQuery)

	keys, ok := qc.Values["_kv_keys"].([]string)
	require.True(t, ok)
	assert.Equal(t, []string{"k1", "k2", "k3"}, keys)
}

func TestNewCommandContext_WithExtra(t *testing.T) {
	extra := map[string]any{
		"_kv_ttl": 5,
		"custom":  "data",
	}
	qc := newCommandContext(OpSet, []string{"key"}, extra)

	assert.Equal(t, 5, qc.Values["_kv_ttl"])
	assert.Equal(t, "data", qc.Values["custom"])
	// _kv_keys should still be present.
	keys, ok := qc.Values["_kv_keys"].([]string)
	require.True(t, ok)
	assert.Equal(t, []string{"key"}, keys)
}

func TestNewCommandContext_EmptyKeys(t *testing.T) {
	qc := newCommandContext(OpScan, []string{}, nil)

	assert.Equal(t, "", qc.RawQuery)

	keys, ok := qc.Values["_kv_keys"].([]string)
	require.True(t, ok)
	assert.Empty(t, keys)
}
