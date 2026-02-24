package kvcrdt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv/codec"
)

func TestOption_WithNodeID(t *testing.T) {
	cfg := defaultConfig()
	WithNodeID("test-node")(cfg)

	assert.Equal(t, "test-node", cfg.nodeID)
}

func TestOption_WithClock(t *testing.T) {
	cfg := defaultConfig()
	clock := crdt.NewHybridClock("clock-node")
	WithClock(clock)(cfg)

	require.NotNil(t, cfg.clock)
	assert.Equal(t, clock, cfg.clock)
}

func TestOption_WithCodec(t *testing.T) {
	cfg := defaultConfig()
	cc := codec.JSON()
	WithCodec(cc)(cfg)

	require.NotNil(t, cfg.codec)
	assert.Equal(t, cc, cfg.codec)
}

func TestSyncerOption_WithSyncInterval(t *testing.T) {
	cfg := defaultSyncerConfig()
	WithSyncInterval(100 * time.Millisecond)(cfg)

	assert.Equal(t, 100*time.Millisecond, cfg.interval)
}

func TestSyncerOption_WithKeyPattern(t *testing.T) {
	cfg := defaultSyncerConfig()
	WithKeyPattern("myprefix:*")(cfg)

	assert.Equal(t, "myprefix:*", cfg.keyPattern)
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	assert.Equal(t, "default", cfg.nodeID)
	require.NotNil(t, cfg.codec)
	assert.Equal(t, "json", cfg.codec.Name())
}

func TestDefaultSyncerConfig(t *testing.T) {
	cfg := defaultSyncerConfig()

	assert.Equal(t, 5*time.Second, cfg.interval)
	assert.Equal(t, "crdt:*", cfg.keyPattern)
}
