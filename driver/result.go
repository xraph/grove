package driver

// Result represents the outcome of an exec query (INSERT, UPDATE, DELETE).
// It mirrors the standard database/sql.Result interface so that concrete
// drivers can return the native result directly or wrap it.
type Result interface {
	// RowsAffected returns the number of rows affected by the statement.
	RowsAffected() (int64, error)

	// LastInsertId returns the last auto-generated ID (if supported by the
	// driver). Drivers that do not support auto-increment IDs (e.g.,
	// PostgreSQL without RETURNING) may return 0 and an error.
	LastInsertId() (int64, error)
}

// Rows represents a result set from a query. Callers must call Close when
// finished iterating, typically via a defer.
type Rows interface {
	// Next advances to the next row. It returns false when no more rows are
	// available or an error occurred during iteration.
	Next() bool

	// Scan copies the current row's columns into dest. The number of dest
	// values must match the number of columns in the result set.
	Scan(dest ...any) error

	// Columns returns the column names of the result set.
	Columns() ([]string, error)

	// Close closes the rows iterator, releasing any held resources.
	Close() error

	// Err returns any error encountered during iteration (other than
	// io.EOF). It should be checked after the Next loop completes.
	Err() error
}

// Row represents a single row result from QueryRow. If the query returns no
// rows, Scan will return an appropriate error (e.g., sql.ErrNoRows).
type Row interface {
	// Scan copies the row's columns into dest.
	Scan(dest ...any) error
}
