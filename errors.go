package grove

import "errors"

// Sentinel errors returned by Grove operations.
// Use errors.Is() to check for specific error conditions.
var (
	// ErrNoRows is returned when a query expected at least one row but found none.
	ErrNoRows = errors.New("grove: no rows in result set")

	// ErrModelNotRegistered is returned when a query references a model type
	// that was not registered via DB.RegisterModel().
	ErrModelNotRegistered = errors.New("grove: model not registered")

	// ErrNotSupported is returned when an operation is not supported by the
	// current driver (e.g., RETURNING on MySQL, transactions on some NoSQL drivers).
	ErrNotSupported = errors.New("grove: operation not supported by driver")

	// ErrDriverClosed is returned when an operation is attempted on a closed DB.
	ErrDriverClosed = errors.New("grove: driver connection pool has been closed")

	// ErrTxDone is returned when an operation is attempted on a committed
	// or rolled-back transaction.
	ErrTxDone = errors.New("grove: transaction has already been committed or rolled back")

	// ErrInvalidDSN is returned when the data source name is malformed.
	ErrInvalidDSN = errors.New("grove: invalid data source name")

	// ErrHookDenied is returned when a privacy hook denies the operation.
	ErrHookDenied = errors.New("grove: hook denied the operation")

	// ErrHookPanic is returned when a hook panics during execution (recovered).
	ErrHookPanic = errors.New("grove: hook panicked during execution")

	// ErrMigrationFailed is returned when a migration function returns an error.
	ErrMigrationFailed = errors.New("grove: migration failed")

	// ErrMigrationLocked is returned when another process holds the migration lock.
	ErrMigrationLocked = errors.New("grove: migration lock held by another process")

	// ErrCyclicDependency is returned when migration group dependencies form a cycle.
	ErrCyclicDependency = errors.New("grove: cyclic dependency in migration groups")

	// ErrDuplicateVersion is returned when two migrations share the same version string.
	ErrDuplicateVersion = errors.New("grove: duplicate migration version")

	// ErrInvalidTag is returned when a struct tag has invalid syntax.
	ErrInvalidTag = errors.New("grove: invalid struct tag syntax")

	// ErrNoPrimaryKey is returned when a model has no field marked as pk.
	ErrNoPrimaryKey = errors.New("grove: model has no primary key field")

	// ErrInvalidRelation is returned when a relation definition is incomplete or incorrect.
	ErrInvalidRelation = errors.New("grove: invalid relation definition")
)
