package stream

import "context"

// Map transforms each element in the stream.
func Map[T, U any](s *Stream[T], fn func(T) (U, error)) *Stream[U] {
	return &Stream[U]{
		cursor: s.cursor,
		decode: func(c Cursor) (U, error) {
			// Stream.Next has already advanced the cursor to this row.
			val, err := s.decode(c)
			if err != nil {
				var zero U
				return zero, err
			}
			return fn(val)
		},
	}
}

// Filter yields only elements where the predicate returns true.
func Filter[T any](s *Stream[T], fn func(T) bool) *Stream[T] {
	original := s.decode
	return &Stream[T]{
		cursor: s.cursor,
		decode: func(c Cursor) (T, error) {
			for {
				val, err := original(c)
				if err != nil {
					return val, err
				}
				if fn(val) {
					return val, nil
				}
				// Skip this value, try next.
				if !c.Next() {
					var zero T
					return zero, errEndOfStream
				}
			}
		},
	}
}

// Take yields at most n elements then marks the stream as done.
func Take[T any](s *Stream[T], n int) *Stream[T] {
	cursor := &takeCursor{Cursor: s.cursor, remaining: n}
	original := s.decode
	return &Stream[T]{
		cursor: cursor,
		decode: func(c Cursor) (T, error) {
			val, err := original(c)
			cursor.remaining--
			return val, err
		},
	}
}

// takeCursor checks the limit before advancing, including advances made by
// downstream transforms. Decode counts output elements so upstream Filter or
// Chunk can still consume all rows needed for one element.
type takeCursor struct {
	Cursor
	remaining int
}

func (c *takeCursor) Next() bool {
	return c.remaining > 0 && c.Cursor.Next()
}

func (c *takeCursor) NextContext(ctx context.Context) bool {
	if c.remaining <= 0 {
		return false
	}
	if contextual, ok := c.Cursor.(ContextCursor); ok {
		return contextual.NextContext(ctx)
	}
	return c.Cursor.Next()
}

// Reduce accumulates a value over the entire stream.
func Reduce[T, A any](s *Stream[T], initial A, fn func(A, T) A) (A, error) {
	ctx := context.Background()
	defer func() { _ = s.Close() }()

	acc := initial
	for s.Next(ctx) {
		acc = fn(acc, s.Value())
	}
	return acc, s.Err()
}

// ForEach processes every element with a callback. Returns first error.
func ForEach[T any](s *Stream[T], fn func(T) error) error {
	ctx := context.Background()
	defer func() { _ = s.Close() }()

	for s.Next(ctx) {
		if err := fn(s.Value()); err != nil {
			return err
		}
	}
	return s.Err()
}

// Chunk groups elements into fixed-size batches.
func Chunk[T any](s *Stream[T], size int) *Stream[[]T] {
	return &Stream[[]T]{
		cursor: s.cursor,
		decode: func(c Cursor) ([]T, error) {
			batch := make([]T, 0, size)
			for len(batch) < size {
				val, err := s.decode(c)
				if err != nil {
					if len(batch) > 0 {
						return batch, nil
					}
					return nil, err
				}
				batch = append(batch, val)
				if len(batch) < size && !c.Next() {
					break
				}
			}
			if len(batch) == 0 {
				return nil, c.Err()
			}
			return batch, nil
		},
	}
}
