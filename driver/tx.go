package driver

import "context"

// IsolationLevel represents a SQL transaction isolation level.
type IsolationLevel int

const (
	// LevelDefault uses the database's default isolation level.
	LevelDefault IsolationLevel = iota
	// LevelReadUncommitted allows reading uncommitted changes from other
	// transactions.
	LevelReadUncommitted
	// LevelReadCommitted only sees data committed before the query began.
	LevelReadCommitted
	// LevelRepeatableRead ensures that re-reading the same row within a
	// transaction yields the same data.
	LevelRepeatableRead
	// LevelSerializable is the strictest level; transactions execute as if
	// they were serialized one after another.
	LevelSerializable
)

// String returns a human-readable name for the isolation level.
func (l IsolationLevel) String() string {
	switch l {
	case LevelDefault:
		return "Default"
	case LevelReadUncommitted:
		return "Read Uncommitted"
	case LevelReadCommitted:
		return "Read Committed"
	case LevelRepeatableRead:
		return "Repeatable Read"
	case LevelSerializable:
		return "Serializable"
	default:
		return "Unknown"
	}
}

// TxOptions holds transaction configuration.
type TxOptions struct {
	// IsolationLevel sets the isolation level for the transaction.
	IsolationLevel IsolationLevel
	// ReadOnly marks the transaction as read-only when true. The database
	// may use this hint for optimisation.
	ReadOnly bool
}

// Tx represents a database transaction. All queries executed through a Tx
// participate in the same underlying database transaction and share its
// isolation guarantees.
type Tx interface {
	// Exec executes a query within the transaction.
	Exec(ctx context.Context, query string, args ...any) (Result, error)

	// Query executes a query that returns rows within the transaction.
	Query(ctx context.Context, query string, args ...any) (Rows, error)

	// QueryRow executes a query that returns a single row within the
	// transaction.
	QueryRow(ctx context.Context, query string, args ...any) Row

	// Commit commits the transaction. After Commit returns successfully,
	// all changes made within the transaction are durable.
	Commit() error

	// Rollback rolls back the transaction, discarding all changes.
	// Rollback is safe to call after Commit; it will be a no-op if the
	// transaction has already been committed.
	Rollback() error
}
