package stream

// BatchCursor wraps a base cursor-creating function to implement
// batched fetch semantics. It fetches batchSize rows at a time
// from the underlying source, yielding them one at a time.
type BatchCursor struct {
	fetchBatch func(offset, limit int) (Cursor, error)
	batchSize  int
	current    Cursor
	offset     int
	done       bool
	err        error
}

// NewBatchCursor creates a BatchCursor that fetches rows in batches of batchSize.
// The fetchBatch function is called each time a new batch is needed, with
// the current offset and the batch size as limit.
func NewBatchCursor(fetchBatch func(offset, limit int) (Cursor, error), batchSize int) *BatchCursor {
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
			return true
		}
		// Current batch exhausted — check for iteration error.
		if bc.current.Err() != nil {
			bc.err = bc.current.Err()
			bc.done = true
			return false
		}
		// Close the exhausted cursor.
		if err := bc.current.Close(); err != nil {
			bc.err = err
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

	if !bc.current.Next() {
		// Empty batch means we've reached the end.
		bc.done = true
		bc.err = bc.current.Err()
		_ = bc.current.Close()
		return false
	}
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
	if bc.current != nil {
		return bc.current.Close()
	}
	return nil
}

// Err returns any error encountered during iteration.
func (bc *BatchCursor) Err() error {
	return bc.err
}
