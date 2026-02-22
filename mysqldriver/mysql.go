package mysqldriver

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
)

// MysqlDB implements driver.Driver for MySQL using database/sql.
// Call New() to create an instance and then Open() to establish the
// connection pool.
//
// When txConn is set (by MysqlTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type MysqlDB struct {
	db      *sql.DB
	dialect *MysqlDialect
	opts    *driver.DriverOptions
	txConn  driver.Tx    // non-nil when operating inside a transaction
	hooks   *hook.Engine // optional hook engine for lifecycle hooks
}

var _ driver.Driver = (*MysqlDB)(nil)

// New creates a new unconnected MysqlDB. Call Open to establish a connection pool.
func New() *MysqlDB {
	return &MysqlDB{
		dialect: &MysqlDialect{},
	}
}

// Name returns the driver identifier.
func (db *MysqlDB) Name() string { return "mysql" }

// Dialect returns the MySQL dialect.
func (db *MysqlDB) Dialect() driver.Dialect { return db.dialect }

// SupportsReturning returns false because MySQL does not support
// INSERT ... RETURNING.
func (db *MysqlDB) SupportsReturning() bool { return false }

// Open parses the DSN, applies configuration options, creates the database/sql
// connection pool, and verifies connectivity.
func (db *MysqlDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysqldriver: open: %w", err)
	}

	// Apply pool size from options.
	if db.opts.PoolSize > 0 {
		sqlDB.SetMaxOpenConns(db.opts.PoolSize)
	}

	db.db = sqlDB
	return db.db.PingContext(ctx)
}

// Close terminates all connections in the pool.
func (db *MysqlDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks (pre/post query and mutation).
// If engine is nil, hooks are disabled.
func (db *MysqlDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Ping verifies that the database is reachable.
func (db *MysqlDB) Ping(ctx context.Context) error {
	if db.db == nil {
		return fmt.Errorf("mysqldriver: connection pool is not initialized; call Open first")
	}
	return db.db.PingContext(ctx)
}

// Exec executes a query that does not return rows (INSERT, UPDATE, DELETE, DDL)
// and returns a driver.Result.
func (db *MysqlDB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if db.txConn != nil {
		return db.txConn.Exec(ctx, query, args...)
	}
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: exec: %w", err)
	}
	return &mysqlResult{res: res}, nil
}

// Query executes a query that returns rows and wraps the result in a
// driver.Rows.
func (db *MysqlDB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if db.txConn != nil {
		return db.txConn.Query(ctx, query, args...)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: query: %w", err)
	}
	return &mysqlRows{rows: rows}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *MysqlDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	row := db.db.QueryRowContext(ctx, query, args...)
	return &mysqlRow{row: row}
}

// BeginTx starts a new database transaction with the specified options.
func (db *MysqlDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	sqlOpts := &sql.TxOptions{}

	if opts != nil {
		sqlOpts.Isolation = mapIsolationLevel(opts.IsolationLevel)
		sqlOpts.ReadOnly = opts.ReadOnly
	}

	tx, err := db.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: begin tx: %w", err)
	}
	return &mysqlTx{tx: tx}, nil
}

// GroveTx is the adapter method for grove.DB.BeginTx(). It bridges the
// grove package's generic transaction interface with MysqlDB's typed BeginTx.
// The isolationLevel parameter maps to driver.IsolationLevel constants.
func (db *MysqlDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return db.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *MysqlDB) GroveSelect(model ...any) any { return db.NewSelect(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *MysqlDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *MysqlDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *MysqlDB) GroveDelete(model any) any { return db.NewDelete(model) }

// mapIsolationLevel converts a driver.IsolationLevel to the corresponding
// sql.IsolationLevel constant.
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
		// LevelDefault: return default so database uses its default.
		return sql.LevelDefault
	}
}
