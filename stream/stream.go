// Package stream provides a generic streaming iterator for database results.
//
// Stream[T] is a lazy, pull-based iterator over database results. It holds
// an open server-side cursor and decodes one row at a time. Supports Go 1.23+
// range-over-func via the All method.
//
// The streaming system supports composable transforms (Map, Filter, Reduce,
// Chunk, Take, ForEach) as zero-allocation pipeline operations.
package stream

import (
	"context"
	"fmt"
	"sync"
)

// HookRunner is an optional interface for per-row hook execution.
// This avoids importing the hook package directly.
type HookRunner interface {
	RunStreamRowHook(ctx context.Context, qc any, row any) (int, error) // returns Decision as int
}

// Stream is a lazy, pull-based iterator over database results.
type Stream[T any] struct {
	cursor  Cursor
	decode  DecodeFunc[T]
	hooks   HookRunner // optional, nil means no hooks
	qc      any        // *hook.QueryContext, stored as any to avoid import cycle
	current T
	done    bool
	err     error
	once    sync.Once
}

// New creates a new Stream from a cursor and a decode function.
func New[T any](cursor Cursor, decode DecodeFunc[T]) *Stream[T] {
	return &Stream[T]{
		cursor: cursor,
		decode: decode,
	}
}

// NewWithHooks creates a new Stream with per-row hook execution.
func NewWithHooks[T any](cursor Cursor, decode DecodeFunc[T], hooks HookRunner, qc any) *Stream[T] {
	return &Stream[T]{
		cursor: cursor,
		decode: decode,
		hooks:  hooks,
		qc:     qc,
	}
}

// WithHooks sets the hook runner and query context on the stream, returning
// the stream for chaining. If runner is nil, hooks are disabled.
func (s *Stream[T]) WithHooks(runner HookRunner, qc any) *Stream[T] {
	s.hooks = runner
	s.qc = qc
	return s
}

// Hook decision constants mirroring hook.Decision to avoid importing hook.
const (
	hookAllow = 0 // hook.Allow
	hookDeny  = 1 // hook.Deny
	hookSkip  = 3 // hook.Skip
)

// Next advances the cursor and decodes the next row.
// Returns false when exhausted or on error (check Err()).
//
// If hooks are configured, RunStreamRowHook is called after decoding each row.
// A Skip decision causes the row to be skipped (continue to next).
// A Deny decision stops iteration with an error.
func (s *Stream[T]) Next(ctx context.Context) bool {
	if s.done {
		return false
	}

	for {
		// Check context cancellation.
		if ctx.Err() != nil {
			s.err = ctx.Err()
			s.done = true
			return false
		}

		if !s.cursor.Next() {
			s.done = true
			s.err = s.cursor.Err()
			return false
		}

		val, err := s.decode(s.cursor)
		if err != nil {
			s.err = err
			s.done = true
			return false
		}

		// Run per-row hooks if configured.
		if s.hooks != nil {
			decision, hookErr := s.hooks.RunStreamRowHook(ctx, s.qc, val)
			if hookErr != nil {
				s.err = hookErr
				s.done = true
				return false
			}
			switch decision {
			case hookSkip:
				continue // skip this row, fetch next
			case hookDeny:
				s.err = fmt.Errorf("stream: row denied by hook")
				s.done = true
				return false
			}
		}

		s.current = val
		return true
	}
}

// Value returns the current decoded row. Only valid after Next() returns true.
func (s *Stream[T]) Value() T {
	return s.current
}

// Err returns the first error encountered during iteration.
func (s *Stream[T]) Err() error {
	return s.err
}

// Close releases the server-side cursor and underlying connection.
// Always defer this.
func (s *Stream[T]) Close() error {
	var err error
	s.once.Do(func() {
		s.done = true
		err = s.cursor.Close()
	})
	return err
}

// All returns a range-over-func iterator for use with Go 1.23+ for-range.
//
//	for user, err := range userStream.All {
//	    ...
//	}
func (s *Stream[T]) All(yield func(T, error) bool) {
	ctx := context.Background()
	defer func() { _ = s.Close() }()

	for s.Next(ctx) {
		if !yield(s.Value(), nil) {
			return
		}
	}
	if s.Err() != nil {
		var zero T
		yield(zero, s.Err())
	}
}

// Collect drains the stream into a slice.
func (s *Stream[T]) Collect(ctx context.Context) ([]T, error) {
	defer func() { _ = s.Close() }()

	var result []T
	for s.Next(ctx) {
		result = append(result, s.Value())
	}
	if s.Err() != nil {
		return result, s.Err()
	}
	return result, nil
}

// Count drains the stream counting rows without allocating models.
func (s *Stream[T]) Count(ctx context.Context) (int64, error) {
	defer func() { _ = s.Close() }()

	var count int64
	for s.Next(ctx) {
		count++
	}
	return count, s.Err()
}
