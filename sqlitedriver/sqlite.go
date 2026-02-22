package sqlitedriver

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
)

// SqliteDB implements driver.Driver for SQLite using database/sql
// with the modernc.org/sqlite pure-Go driver.
// Call New() to create an instance and then Open() to establish the
// database connection.
//
// When txConn is set (by SqliteTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type SqliteDB struct {
	db      *sql.DB
	dialect *SqliteDialect
	opts    *driver.DriverOptions
	txConn  driver.Tx    // non-nil when operating inside a transaction
	hooks   *hook.Engine // optional hook engine for lifecycle hooks
}

var _ driver.Driver = (*SqliteDB)(nil)

// New creates a new unconnected SqliteDB. Call Open to establish a connection.
func New() *SqliteDB {
	return &SqliteDB{
		dialect: &SqliteDialect{},
	}
}

// Name returns the driver identifier.
func (db *SqliteDB) Name() string { return "sqlite" }

// Dialect returns the SQLite dialect.
func (db *SqliteDB) Dialect() driver.Dialect { return db.dialect }

// SupportsReturning returns true because SQLite 3.35+ supports
// INSERT ... RETURNING.
func (db *SqliteDB) SupportsReturning() bool { return true }

// Open parses the DSN, applies configuration options, opens the SQLite
// database, and configures WAL mode and foreign keys.
func (db *SqliteDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("sqlitedriver: open: %w", err)
	}

	// SQLite only supports one writer at a time; enable WAL mode for better concurrency.
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		sqlDB.Close()
		return fmt.Errorf("sqlitedriver: enable WAL: %w", err)
	}

	// Enable foreign keys.
	if _, err := sqlDB.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		sqlDB.Close()
		return fmt.Errorf("sqlitedriver: enable foreign keys: %w", err)
	}

	if db.opts.PoolSize > 0 {
		sqlDB.SetMaxOpenConns(db.opts.PoolSize)
	}

	db.db = sqlDB
	return nil
}

// Close terminates all connections.
func (db *SqliteDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks (pre/post query and mutation).
// If engine is nil, hooks are disabled.
func (db *SqliteDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Ping verifies that the database is reachable.
func (db *SqliteDB) Ping(ctx context.Context) error {
	if db.db == nil {
		return fmt.Errorf("sqlitedriver: database is not initialized; call Open first")
	}
	return db.db.PingContext(ctx)
}

// Exec executes a query that does not return rows (INSERT, UPDATE, DELETE, DDL)
// and returns a driver.Result.
func (db *SqliteDB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if db.txConn != nil {
		return db.txConn.Exec(ctx, query, args...)
	}
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: exec: %w", err)
	}
	return &sqliteResult{res: res}, nil
}

// Query executes a query that returns rows and wraps the result in a
// driver.Rows.
func (db *SqliteDB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if db.txConn != nil {
		return db.txConn.Query(ctx, query, args...)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: query: %w", err)
	}
	return &sqliteRows{rows: rows}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *SqliteDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	row := db.db.QueryRowContext(ctx, query, args...)
	return &sqliteRow{row: row}
}

// BeginTx starts a new database transaction with the specified options.
func (db *SqliteDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	sqlOpts := &sql.TxOptions{}

	if opts != nil {
		sqlOpts.Isolation = mapIsolationLevel(opts.IsolationLevel)
		sqlOpts.ReadOnly = opts.ReadOnly
	}

	tx, err := db.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: begin tx: %w", err)
	}
	return &sqliteTx{tx: tx}, nil
}

// GroveTx is the adapter method for grove.DB.BeginTx(). It bridges the
// grove package's generic transaction interface with SqliteDB's typed BeginTx.
// The isolationLevel parameter maps to driver.IsolationLevel constants.
func (db *SqliteDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return db.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *SqliteDB) GroveSelect(model ...any) any { return db.NewSelect(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *SqliteDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *SqliteDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *SqliteDB) GroveDelete(model any) any { return db.NewDelete(model) }

// mapIsolationLevel converts a driver.IsolationLevel to the corresponding
// sql.IsolationLevel constant. SQLite only truly supports Serializable,
// but we map levels for compatibility.
func mapIsolationLevel(level driver.IsolationLevel) sql.IsolationLevel {
	switch level {
	case driver.LevelReadUncommitted:
		return sql.LevelReadUncommitted
	case driver.LevelReadCommitted:
		return sql.LevelReadCommitted
	case driver.LevelRepeatableRead:
		return sql.LevelRepeatableRead
	case driver.LevelSerializable:
		return sql.LevelSerializable
	default:
		// LevelDefault: return default isolation level.
		return sql.LevelDefault
	}
}
