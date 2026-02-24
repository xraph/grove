package pgdriver

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/stream"
)

// cursorCounter generates unique cursor names across the process.
var cursorCounter atomic.Int64

// pgCursor implements stream.Cursor using PG server-side cursors
// (DECLARE CURSOR / FETCH). It operates within a transaction and
// fetches rows in batches of fetchSize.
type pgCursor struct {
	tx         driver.Tx
	cursorName string
	fetchSize  int
	ownerTx    bool // if true, commit/rollback tx on Close

	currentRows driver.Rows // current batch of rows from FETCH
	done        bool
	err         error
}

var _ stream.Cursor = (*pgCursor)(nil)

// newPgCursor declares a server-side cursor within the given transaction
// and returns a pgCursor that fetches rows in batches. If ownerTx is true,
// the cursor will commit the transaction on Close.
func newPgCursor(ctx context.Context, tx driver.Tx, query string, args []any, fetchSize int, ownerTx bool) (*pgCursor, error) {
	name := fmt.Sprintf("grove_cursor_%d", cursorCounter.Add(1))

	// DECLARE name CURSOR FOR <query>
	declareSQL := fmt.Sprintf("DECLARE %s CURSOR FOR %s", name, query)
	if _, err := tx.Exec(ctx, declareSQL, args...); err != nil {
		return nil, fmt.Errorf("pgdriver: declare cursor: %w", err)
	}

	c := &pgCursor{
		tx:         tx,
		cursorName: name,
		fetchSize:  fetchSize,
		ownerTx:    ownerTx,
	}

	return c, nil
}

// fetchBatch fetches the next batch of rows from the server-side cursor.
func (c *pgCursor) fetchBatch(ctx context.Context) error {
	if c.currentRows != nil {
		_ = c.currentRows.Close()
		c.currentRows = nil
	}

	fetchSQL := fmt.Sprintf("FETCH %d FROM %s", c.fetchSize, c.cursorName)
	rows, err := c.tx.Query(ctx, fetchSQL)
	if err != nil {
		return fmt.Errorf("pgdriver: fetch cursor: %w", err)
	}

	c.currentRows = rows
	return nil
}

// Next advances to the next row. Transparently fetches new batches when
// the current batch is exhausted.
func (c *pgCursor) Next() bool {
	if c.done {
		return false
	}

	ctx := context.Background()

	// If we have no current rows, fetch the first batch.
	if c.currentRows == nil {
		if err := c.fetchBatch(ctx); err != nil {
			c.err = err
			c.done = true
			return false
		}
	}

	// Try to advance within the current batch.
	if c.currentRows.Next() {
		return true
	}

	// Check for errors in the current batch.
	if c.currentRows.Err() != nil {
		c.err = c.currentRows.Err()
		c.done = true
		return false
	}

	// Current batch exhausted — fetch the next batch.
	if err := c.fetchBatch(ctx); err != nil {
		c.err = err
		c.done = true
		return false
	}

	// Try to advance in the new batch.
	if c.currentRows.Next() {
		return true
	}

	// Empty batch means we've reached the end.
	c.done = true
	c.err = c.currentRows.Err()
	return false
}

// Scan copies the current row's column values into dest.
func (c *pgCursor) Scan(dest ...any) error {
	if c.currentRows == nil {
		return fmt.Errorf("pgdriver: cursor: no current row")
	}
	return c.currentRows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (c *pgCursor) Columns() ([]string, error) {
	if c.currentRows == nil {
		return nil, nil
	}
	return c.currentRows.Columns()
}

// Close releases the server-side cursor and underlying resources.
// If the cursor owns the transaction, it commits it.
func (c *pgCursor) Close() error {
	c.done = true

	if c.currentRows != nil {
		_ = c.currentRows.Close()
		c.currentRows = nil
	}

	// Close the server-side cursor.
	closeSQL := fmt.Sprintf("CLOSE %s", c.cursorName)
	_, _ = c.tx.Exec(context.Background(), closeSQL)

	if c.ownerTx {
		return c.tx.Commit()
	}
	return nil
}

// Err returns any error encountered during iteration.
func (c *pgCursor) Err() error {
	return c.err
}
