package stream

import "errors"

// BatchCursor wraps a base cursor-creating function to implement
// batched fetch semantics. It fetches batchSize rows at a time
// from the underlying source, yielding them one at a time.
type BatchCursor struct {
	fetchBatch func(offset, limit int) (Cursor, error)
	batchSize  int
	current    Cursor
	batchRows  int
	offset     int
	done       bool
	err        error
}

// NewBatchCursor creates a BatchCursor that fetches rows in batches of batchSize.
// The fetchBatch function is called each time a new batch is needed, with
// the current offset and the batch size as limit.
// A batch containing fewer than batchSize rows marks the end of the source.
// Non-positive batch sizes return a cursor whose Err reports the invalid size.
func NewBatchCursor(fetchBatch func(offset, limit int) (Cursor, error), batchSize int) *BatchCursor {
	if batchSize <= 0 {
		return &BatchCursor{done: true, err: errors.New("stream: batch size must be positive")}
	}
	return &BatchCursor{
		fetchBatch: fetchBatch,
		batchSize:  batchSize,
	}
}

// Next advances to the next row, fetching a new batch when the current
// batch is exhausted. Returns false when no more rows are available.
func (bc *BatchCursor) Next() bool {
	if bc.done {
		return false
	}

	// If we have a current cursor, try to advance it.
	if bc.current != nil {
		if bc.current.Next() {
			bc.batchRows++
			return true
		}
		bc.err = errors.Join(bc.current.Err(), bc.closeCurrent())
		if bc.err != nil || bc.batchRows < bc.batchSize {
			bc.done = true
			return false
		}
		bc.offset += bc.batchSize
	}

	// Fetch the next batch.
	cursor, err := bc.fetchBatch(bc.offset, bc.batchSize)
	if err != nil {
		bc.err = err
		bc.done = true
		return false
	}
	bc.current = cursor
	bc.batchRows = 0

	if !bc.current.Next() {
		// Empty batch means we've reached the end.
		bc.done = true
		bc.err = errors.Join(bc.current.Err(), bc.closeCurrent())
		return false
	}
	bc.batchRows++
	return true
}

// Scan copies the current row's column values into dest.
func (bc *BatchCursor) Scan(dest ...any) error {
	if bc.current == nil {
		return bc.Err()
	}
	return bc.current.Scan(dest...)
}

// Columns returns the column names from the current batch's result set.
func (bc *BatchCursor) Columns() ([]string, error) {
	if bc.current == nil {
		return nil, nil
	}
	return bc.current.Columns()
}

// Close releases the current batch cursor and marks the BatchCursor as done.
func (bc *BatchCursor) Close() error {
	bc.done = true
	return bc.closeCurrent()
}

func (bc *BatchCursor) closeCurrent() error {
	if bc.current != nil {
		cursor := bc.current
		bc.current = nil
		return cursor.Close()
	}
	return nil
}

// Err returns any error encountered during iteration.
func (bc *BatchCursor) Err() error {
	return bc.err
}
