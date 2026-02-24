package kv

import "errors"

// Sentinel errors returned by KV operations.
var (
	// ErrNotFound is returned when a key does not exist.
	ErrNotFound = errors.New("kv: key not found")

	// ErrConflict is returned when a CAS operation detects a version conflict.
	ErrConflict = errors.New("kv: version conflict (CAS mismatch)")

	// ErrStoreClosed is returned when an operation is attempted on a closed store.
	ErrStoreClosed = errors.New("kv: store has been closed")

	// ErrNotSupported is returned when a driver does not support the requested operation.
	ErrNotSupported = errors.New("kv: operation not supported by driver")

	// ErrHookDenied is returned when a hook denies the operation.
	ErrHookDenied = errors.New("kv: hook denied the operation")

	// ErrCodecEncode is returned when value encoding fails.
	ErrCodecEncode = errors.New("kv: codec encode failed")

	// ErrCodecDecode is returned when value decoding fails.
	ErrCodecDecode = errors.New("kv: codec decode failed")

	// ErrKeyEmpty is returned when an empty key is provided.
	ErrKeyEmpty = errors.New("kv: key must not be empty")

	// ErrNilValue is returned when a nil value is provided to Set.
	ErrNilValue = errors.New("kv: value must not be nil")
)
