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

// ScriptDriver is an optional interface for drivers that run server-side
// scripts.
//
// It exists for the compare-and-set a lease handoff needs: checking a
// field and conditionally writing it has to be one round trip, or two
// workers can both conclude they hold the same lease. CASDriver covers
// whole-value conditions; this covers conditions on the contents.
type ScriptDriver interface {
	// Eval runs a script against the given keys and arguments.
	//
	// The result is whatever the script returned, in the driver's native
	// representation: callers are expected to know the shape of the
	// script they wrote.
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
	// EvalSHA runs a previously loaded script by digest, returning
	// ErrScriptNotLoaded when the server has forgotten it.
	EvalSHA(ctx context.Context, sha string, keys []string, args ...any) (any, error)
	// ScriptLoad caches a script and returns its digest.
	ScriptLoad(ctx context.Context, script string) (string, error)
}

// StreamMessage is one entry in a stream.
type StreamMessage struct {
	// ID orders the entry within its stream. Drivers assign it when the
	// caller does not.
	ID string
	// Values are the entry's fields.
	Values map[string][]byte
}

// StreamDriver is an optional interface for drivers with append-only
// streams.
//
// A stream differs from Pub/Sub in the way that matters to a consumer: an
// entry persists until it is deleted, so a subscriber that was not
// connected when it was written can still read it. That is what makes a
// stream usable for delivery and Pub/Sub only usable for notification.
type StreamDriver interface {
	// XAdd appends an entry and returns its assigned ID.
	XAdd(ctx context.Context, stream string, values map[string][]byte) (string, error)
	// XRange returns entries between start and stop inclusive, oldest
	// first. Empty bounds mean unbounded; a count of zero means no limit.
	XRange(ctx context.Context, stream, start, stop string, count int64) ([]StreamMessage, error)
	// XDel removes entries by ID, returning how many were present.
	XDel(ctx context.Context, stream string, ids ...string) (int64, error)
	// XLen returns the number of entries in a stream.
	XLen(ctx context.Context, stream string) (int64, error)
}
