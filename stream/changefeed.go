package stream

import (
	"context"
	"sync"
	"time"
)

// ChangeOp represents the type of change in a CDC event.
type ChangeOp int

const (
	ChangeInsert ChangeOp = iota
	ChangeUpdate
	ChangeDelete
	ChangeReplace // MongoDB-specific
)

// String returns a human-readable name for the change operation.
func (op ChangeOp) String() string {
	switch op {
	case ChangeInsert:
		return "INSERT"
	case ChangeUpdate:
		return "UPDATE"
	case ChangeDelete:
		return "DELETE"
	case ChangeReplace:
		return "REPLACE"
	default:
		return "UNKNOWN"
	}
}

// ChangeEvent represents a single change from a CDC stream.
type ChangeEvent[T any] struct {
	// Operation is the type of change.
	Operation ChangeOp

	// Before is the previous state (nil for inserts; available if driver supports it).
	Before *T

	// After is the new state (nil for deletes).
	After *T

	// Timestamp is the server timestamp of the change.
	Timestamp time.Time

	// ResumeToken is an opaque token for resuming the stream after disconnect.
	ResumeToken any
}

// ChangeSource is the interface that drivers implement to provide CDC events.
type ChangeSource[T any] interface {
	// Next blocks until the next change event is available.
	// Returns false when the stream is closed or an error occurred.
	Next(ctx context.Context) bool

	// Event returns the current change event.
	Event() ChangeEvent[T]

	// Err returns any error encountered.
	Err() error

	// Close stops the change stream.
	Close() error

	// ResumeToken returns the current resume token for reconnection.
	ResumeToken() any
}

// ChangeStream is a long-lived iterator over real-time database changes.
// It wraps a ChangeSource and supports automatic reconnection with resume tokens.
type ChangeStream[T any] struct {
	source  ChangeSource[T]
	current ChangeEvent[T]
	done    bool
	err     error
	once    sync.Once
}

// NewChangeStream creates a new ChangeStream from a driver-specific source.
func NewChangeStream[T any](source ChangeSource[T]) *ChangeStream[T] {
	return &ChangeStream[T]{source: source}
}

// Next blocks until the next change event is available.
func (cs *ChangeStream[T]) Next(ctx context.Context) bool {
	if cs.done {
		return false
	}

	if !cs.source.Next(ctx) {
		cs.done = true
		cs.err = cs.source.Err()
		return false
	}

	cs.current = cs.source.Event()
	return true
}

// Event returns the current change event.
func (cs *ChangeStream[T]) Event() ChangeEvent[T] {
	return cs.current
}

// Err returns any error encountered.
func (cs *ChangeStream[T]) Err() error {
	return cs.err
}

// Close stops the change stream and releases resources.
func (cs *ChangeStream[T]) Close() error {
	var err error
	cs.once.Do(func() {
		cs.done = true
		err = cs.source.Close()
	})
	return err
}

// ResumeToken returns the current resume token for reconnection.
func (cs *ChangeStream[T]) ResumeToken() any {
	return cs.source.ResumeToken()
}

// All returns a range-over-func iterator for change events.
func (cs *ChangeStream[T]) All(yield func(ChangeEvent[T], error) bool) {
	ctx := context.Background()
	defer func() { _ = cs.Close() }()

	for cs.Next(ctx) {
		if !yield(cs.Event(), nil) {
			return
		}
	}
	if cs.Err() != nil {
		var zero ChangeEvent[T]
		yield(zero, cs.Err())
	}
}
