package kv

import "time"

// Entry is a typed wrapper around a KV value that carries metadata.
type Entry[T any] struct {
	// Key is the full key string.
	Key string

	// Value is the decoded value.
	Value T

	// TTL is the remaining time-to-live. Zero means no expiry.
	TTL time.Duration

	// Version is the CAS version (if supported by the driver).
	Version uint64

	// Metadata carries arbitrary key-value metadata.
	Metadata map[string]string
}

// HasTTL returns true if the entry has a TTL set.
func (e Entry[T]) HasTTL() bool {
	return e.TTL > 0
}
