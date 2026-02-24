// Package kvcrdt bridges grove/crdt types into KV storage, enabling
// distributed, eventually-consistent state without a relational database.
//
// It provides CRDT-backed distributed counters, registers, sets, and maps
// that use a KV Store for persistence and can be synchronized across stores.
package kvcrdt

import (
	"time"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv/codec"
)

// Option configures a CRDT KV type.
type Option func(*crdtConfig)

type crdtConfig struct {
	nodeID string
	clock  crdt.Clock
	codec  codec.Codec
}

func defaultConfig() *crdtConfig {
	return &crdtConfig{
		nodeID: "default",
		codec:  codec.JSON(),
	}
}

// WithNodeID sets the node identifier for CRDT operations.
// Each node in a distributed system should have a unique ID.
func WithNodeID(id string) Option {
	return func(c *crdtConfig) { c.nodeID = id }
}

// WithClock sets the Clock implementation for CRDT timestamps.
// The clock must implement crdt.Clock (e.g., crdt.NewHybridClock).
func WithClock(clock crdt.Clock) Option {
	return func(c *crdtConfig) { c.clock = clock }
}

// WithCodec sets the codec for serializing CRDT state.
func WithCodec(cc codec.Codec) Option {
	return func(c *crdtConfig) { c.codec = cc }
}

// SyncerOption configures the CRDT Syncer.
type SyncerOption func(*syncerConfig)

type syncerConfig struct {
	interval   time.Duration
	keyPattern string
}

func defaultSyncerConfig() *syncerConfig {
	return &syncerConfig{
		interval:   5 * time.Second,
		keyPattern: "crdt:*",
	}
}

// WithSyncInterval sets the interval between sync rounds.
func WithSyncInterval(d time.Duration) SyncerOption {
	return func(c *syncerConfig) { c.interval = d }
}

// WithKeyPattern sets the key pattern for scanning CRDT keys during sync.
func WithKeyPattern(pattern string) SyncerOption {
	return func(c *syncerConfig) { c.keyPattern = pattern }
}
