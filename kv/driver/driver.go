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

// ScoredMember is one member of a sorted set with its score.
type ScoredMember struct {
	Member string
	Score  float64
}

// RangeSpec bounds a sorted-set range query.
//
// Min and Max are inclusive. Because a driver has to express unbounded
// ends somehow, and every numeric sentinel is a value some caller might
// legitimately use, the two Has flags say whether each end is set rather
// than reserving a magic number.
type RangeSpec struct {
	Min, Max       float64
	HasMin, HasMax bool
	// Reverse returns members highest score first.
	Reverse bool
	// Offset skips members after ordering. Zero starts at the first.
	Offset int64
	// Count caps how many members are returned. Zero means no limit.
	Count int64
}

// SortedSetDriver is an optional interface for drivers with sorted sets.
//
// It exists because ordering by score is the one structure a job queue
// cannot emulate over plain keys: claiming the next due item has to be an
// ordered read, and a keyspace scan is neither ordered nor atomic.
type SortedSetDriver interface {
	// ZAdd inserts or updates members, returning how many were new.
	ZAdd(ctx context.Context, key string, members ...ScoredMember) (int64, error)
	// ZRange returns members within the spec, ordered by score.
	ZRange(ctx context.Context, key string, spec RangeSpec) ([]string, error)
	// ZRangeWithScores is ZRange carrying each member's score.
	ZRangeWithScores(ctx context.Context, key string, spec RangeSpec) ([]ScoredMember, error)
	// ZRem removes members, returning how many were present.
	ZRem(ctx context.Context, key string, members ...string) (int64, error)
	// ZCard returns the number of members.
	ZCard(ctx context.Context, key string) (int64, error)
	// ZScore returns a member's score, and false when it is absent.
	ZScore(ctx context.Context, key, member string) (float64, bool, error)
}

// SetDriver is an optional interface for drivers with unordered sets.
type SetDriver interface {
	// SAdd adds members, returning how many were new.
	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	// SRem removes members, returning how many were present.
	SRem(ctx context.Context, key string, members ...string) (int64, error)
	// SMembers returns every member.
	SMembers(ctx context.Context, key string) ([]string, error)
	// SCard returns the number of members.
	SCard(ctx context.Context, key string) (int64, error)
	// SIsMember reports whether a member is present.
	SIsMember(ctx context.Context, key, member string) (bool, error)
}

// HashDriver is an optional interface for drivers with hash maps.
type HashDriver interface {
	// HSet sets fields, returning how many were new.
	HSet(ctx context.Context, key string, fields map[string][]byte) (int64, error)
	// HGet returns one field's value. Returns kv.ErrNotFound if absent.
	HGet(ctx context.Context, key, field string) ([]byte, error)
	// HGetAll returns every field.
	HGetAll(ctx context.Context, key string) (map[string][]byte, error)
	// HDel removes fields, returning how many were present.
	HDel(ctx context.Context, key string, fields ...string) (int64, error)
	// HLen returns the number of fields.
	HLen(ctx context.Context, key string) (int64, error)
}
