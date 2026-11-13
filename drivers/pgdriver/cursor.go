package pgdriver

import (
	"context"
	"errors"
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
	ctx        context.Context
	closed     bool
	closeErr   error
	tx         driver.Tx
	cursorName string
	fetchSize  int
	ownerTx    bool // if true, commit/rollback tx on Close

	currentRows    driver.Rows // current batch of rows from FETCH
	releaseContext context.CancelFunc
	done           bool
	err            error
}

var _ stream.Cursor = (*pgCursor)(nil)
var _ stream.ContextCursor = (*pgCursor)(nil)

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
		ctx:        ctx,
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
		if c.releaseContext != nil {
			c.releaseContext()
		}
	}

	// Keep both the stream lifetime and the consumer's deadline active until
	// this result batch is closed; canceling when Next returns would abort Scan.
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	release := func() { stop(); cancel() }
	fetchSQL := fmt.Sprintf("FETCH %d FROM %s", c.fetchSize, c.cursorName)
	rows, err := c.tx.Query(ctx, fetchSQL)
	if err != nil {
		release()
		return fmt.Errorf("pgdriver: fetch cursor: %w", err)
	}

	c.releaseContext = release
	c.currentRows = rows
	return nil
}

// Next advances to the next row. Transparently fetches new batches when
// the current batch is exhausted.
func (c *pgCursor) Next() bool { return c.NextContext(c.ctx) }

// NextContext fetches using the consumer's deadline as well as the stream's.
func (c *pgCursor) NextContext(ctx context.Context) bool {
	if c.done {
		return false
	}

	if err := c.ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}
	if err := ctx.Err(); err != nil {
		c.err = err
		c.done = true
		return false
	}

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
	if c.closed {
		return c.closeErr
	}
	c.closed = true
	if c.releaseContext != nil {
		defer c.releaseContext()
	}
	c.done = true
	if c.currentRows != nil {
		c.closeErr = c.currentRows.Close()
		if c.err == nil {
			c.err = c.currentRows.Err()
		}
		c.currentRows = nil
	}
	// A failed/canceled owned stream must release its transaction, without
	// trying to commit the failure. Borrowed transactions remain caller-owned.
	if c.ownerTx && (c.err != nil || c.ctx.Err() != nil || c.closeErr != nil) {
		c.closeErr = errors.Join(c.closeErr, c.tx.Rollback())
		return c.closeErr
	}
	closeSQL := fmt.Sprintf("CLOSE %s", c.cursorName)
	_, closeErr := c.tx.Exec(context.Background(), closeSQL)
	c.closeErr = errors.Join(c.closeErr, closeErr)
	if c.ownerTx {
		if c.closeErr != nil {
			c.closeErr = errors.Join(c.closeErr, c.tx.Rollback())
		} else {
			c.closeErr = c.tx.Commit()
		}
	}
	return c.closeErr
}

// Err returns any error encountered during iteration.
func (c *pgCursor) Err() error {
	return c.err
}
