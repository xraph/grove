// Package driver defines the interface contract that every KV backend must implement.
//
// The core Driver interface handles raw byte-level operations. All serialization
// and deserialization happens at the Store layer via codecs, keeping drivers simple.
//
// Optional capabilities are expressed as separate interfaces (BatchDriver, TTLDriver, etc.)
// that drivers can implement. The Store checks for these at runtime via type assertions.
package driver

import (
	"context"
	"time"
)

// Driver is the core interface every KV backend implements.
type Driver interface {
	// Name returns the driver identifier (e.g., "redis", "memcached", "badger").
	Name() string

	// Open initializes a connection using the given DSN.
	Open(ctx context.Context, dsn string, opts ...Option) error

	// Close terminates all connections and releases resources.
	Close() error

	// Ping checks connectivity to the backend.
	Ping(ctx context.Context) error

	// Info returns driver metadata and capabilities.
	Info() DriverInfo

	// Get retrieves the raw bytes for a key. Returns kv.ErrNotFound if missing.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores raw bytes under key with an optional TTL.
	// A zero TTL means no expiry.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete removes keys. Returns the count of keys actually deleted.
	Delete(ctx context.Context, keys ...string) (int64, error)

	// Exists returns the count of keys that exist.
	Exists(ctx context.Context, keys ...string) (int64, error)
}

// BatchDriver is an optional interface for drivers that support multi-key operations.
type BatchDriver interface {
	MGet(ctx context.Context, keys []string) ([][]byte, error)
	MSet(ctx context.Context, pairs map[string][]byte, ttl time.Duration) error
}

// TTLDriver is an optional interface for drivers that support TTL operations.
type TTLDriver interface {
	TTL(ctx context.Context, key string) (time.Duration, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
}

// ScanDriver is an optional interface for drivers that support key scanning/iteration.
type ScanDriver interface {
	Scan(ctx context.Context, pattern string, fn func(key string) error) error
}

// CASDriver is an optional interface for drivers that support Compare-And-Swap operations.
type CASDriver interface {
	// SetNX sets the key only if it does not exist. Returns true if set.
	SetNX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
	// SetXX sets the key only if it already exists. Returns true if set.
	SetXX(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)
}

// PubSubDriver is an optional interface for drivers that support Pub/Sub messaging.
type PubSubDriver interface {
	Publish(ctx context.Context, channel string, message []byte) error
	Subscribe(ctx context.Context, channel string, handler func(msg []byte)) error
}

// TransactionDriver is an optional interface for drivers that support multi-key transactions.
type TransactionDriver interface {
	// Watch starts an optimistic lock on the given keys and executes fn atomically.
	Watch(ctx context.Context, fn func(tx Transaction) error, keys ...string) error
}

// Transaction represents an atomic KV transaction.
type Transaction interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) (int64, error)
}
