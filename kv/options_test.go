package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/codec"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	require.NotNil(t, o.codec)
	assert.Equal(t, "json", o.codec.Name())
	assert.Empty(t, o.hooks)
}

func TestWithCodec_Option(t *testing.T) {
	o := defaultOptions()
	o.apply([]Option{WithCodec(codec.MsgPack())})

	assert.Equal(t, "msgpack", o.codec.Name())
}

func TestWithHook_Option(t *testing.T) {
	o := defaultOptions()
	assert.Empty(t, o.hooks)

	// A dummy hook value (the type does not matter for this test).
	dummy := struct{}{}
	o.apply([]Option{WithHook(dummy)})

	assert.Len(t, o.hooks, 1)

	// Adding a second hook grows the list.
	o.apply([]Option{WithHook(dummy)})
	assert.Len(t, o.hooks, 2)
}

func TestApplySetOptions_Defaults(t *testing.T) {
	so := applySetOptions(nil)
	assert.Equal(t, time.Duration(0), so.ttl)
	assert.False(t, so.nx)
	assert.False(t, so.xx)
	assert.Equal(t, uint64(0), so.cas)
}

func TestApplySetOptions_WithTTL(t *testing.T) {
	so := applySetOptions([]SetOption{WithTTL(3 * time.Second)})
	assert.Equal(t, 3*time.Second, so.ttl)
}

func TestApplySetOptions_WithNX(t *testing.T) {
	so := applySetOptions([]SetOption{WithNX()})
	assert.True(t, so.nx)
}

func TestApplySetOptions_WithXX(t *testing.T) {
	so := applySetOptions([]SetOption{WithXX()})
	assert.True(t, so.xx)
}

func TestApplySetOptions_WithCAS(t *testing.T) {
	so := applySetOptions([]SetOption{WithCAS(42)})
	assert.Equal(t, uint64(42), so.cas)
}
