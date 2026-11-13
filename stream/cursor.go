package stream

import "context"

// Cursor is the interface that database drivers implement to provide
// streaming row access. Each driver wraps its native cursor type.
type Cursor interface {
	// Next advances the cursor to the next row.
	// Returns false when there are no more rows or an error occurred.
	Next() bool

	// Scan copies the current row's column values into dest.
	Scan(dest ...any) error

	// Columns returns the column names from the result set.
	Columns() ([]string, error)

	// Close releases the cursor and underlying resources.
	Close() error

	// Err returns any error encountered during iteration.
	Err() error
}

// DecodeFunc decodes a cursor row into a value of type T.
type DecodeFunc[T any] func(cursor Cursor) (T, error)

// ContextCursor optionally supports cancellation while Next waits for the
// database. Stream passes its consumer context through this interface; existing
// cursors implementing only Cursor remain supported.
type ContextCursor interface {
	Cursor
	NextContext(ctx context.Context) bool
}

// contextualCursor also propagates the consumer context through transforms
// whose decode functions advance the cursor to skip or group rows.
type contextualCursor struct {
	ContextCursor
	ctx context.Context
}

func (c contextualCursor) Next() bool { return c.NextContext(c.ctx) }
