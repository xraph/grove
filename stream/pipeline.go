package stream

import (
	"context"
	"errors"
	"fmt"
)

// Map transforms each element in the stream.
func Map[T, U any](s *Stream[T], fn func(T) (U, error)) *Stream[U] {
	return &Stream[U]{
		closeSource: s.Close,
		pull: func(ctx context.Context) (U, bool, error) {
			if !s.Next(ctx) {
				var zero U
				return zero, false, s.Err()
			}
			value, err := fn(s.Value())
			return value, err == nil, err
		},
	}
}

// Filter yields only elements where the predicate returns true.
func Filter[T any](s *Stream[T], fn func(T) bool) *Stream[T] {
	return &Stream[T]{
		closeSource: s.Close,
		pull: func(ctx context.Context) (T, bool, error) {
			for s.Next(ctx) {
				value := s.Value()
				if fn(value) {
					return value, true, nil
				}
			}
			var zero T
			return zero, false, s.Err()
		},
	}
}

// Take yields at most n elements without fetching another row after the limit.
func Take[T any](s *Stream[T], n int) *Stream[T] {
	return &Stream[T]{
		closeSource: s.Close,
		pull: func(ctx context.Context) (T, bool, error) {
			var zero T
			if n <= 0 {
				return zero, false, nil
			}
			if !s.Next(ctx) {
				return zero, false, s.Err()
			}
			n--
			return s.Value(), true, nil
		},
	}
}

// Reduce accumulates a value over the entire stream.
func Reduce[T, A any](s *Stream[T], initial A, fn func(A, T) A) (result A, err error) {
	ctx := context.Background()
	defer func() { err = errors.Join(err, s.Close()) }()

	acc := initial
	for s.Next(ctx) {
		acc = fn(acc, s.Value())
	}
	return acc, s.Err()
}

// ForEach processes every element with a callback. It reports iteration,
// callback, and cursor cleanup errors.
func ForEach[T any](s *Stream[T], fn func(T) error) (err error) {
	ctx := context.Background()
	defer func() { err = errors.Join(err, s.Close()) }()

	for s.Next(ctx) {
		if callbackErr := fn(s.Value()); callbackErr != nil {
			return callbackErr
		}
	}
	return s.Err()
}

// Chunk groups elements into fixed-size batches. A non-positive size returns
// an error without fetching. A partial batch is followed by any source error.
func Chunk[T any](s *Stream[T], size int) *Stream[[]T] {
	out := &Stream[[]T]{closeSource: s.Close}
	if size <= 0 {
		out.done = true
		out.err = fmt.Errorf("stream: chunk size must be positive")
		return out
	}
	out.pull = func(ctx context.Context) ([]T, bool, error) {
		batch := make([]T, 0, size)
		for len(batch) < size && s.Next(ctx) {
			batch = append(batch, s.Value())
		}
		if len(batch) > 0 {
			return batch, true, nil
		}
		return nil, false, s.Err()
	}
	return out
}
