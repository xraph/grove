package clickhousedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/schema"
)

// ClickHouseDB implements driver.Driver for ClickHouse using database/sql
// with the github.com/ClickHouse/clickhouse-go/v2 driver.
// Call New() to create an instance and then Open() to establish the
// database connection.
//
// Users must import the ClickHouse driver themselves:
//
//	import _ "github.com/ClickHouse/clickhouse-go/v2"
//
// When txConn is set (by ClickHouseTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type ClickHouseDB struct {
	db       *sql.DB
	dialect  *ClickHouseDialect
	opts     *driver.DriverOptions
	txConn   driver.Tx        // non-nil when operating inside a transaction
	hooks    *hook.Engine     // optional hook engine for lifecycle hooks
	registry *schema.Registry // cached table metadata to avoid repeated reflection
}

var _ driver.Driver = (*ClickHouseDB)(nil)
var _ driver.Preparer = (*ClickHouseDB)(nil)

// New creates a new unconnected ClickHouseDB. Call Open to establish a connection.
func New() *ClickHouseDB {
	return &ClickHouseDB{
		dialect:  &ClickHouseDialect{},
		registry: schema.NewRegistry(),
	}
}

// Name returns the driver identifier.
func (db *ClickHouseDB) Name() string { return "clickhouse" }

// Dialect returns the ClickHouse dialect.
func (db *ClickHouseDB) Dialect() driver.Dialect { return db.dialect }

// SupportsReturning returns false because ClickHouse does not support
// INSERT ... RETURNING.
func (db *ClickHouseDB) SupportsReturning() bool { return false }

// Open parses the DSN, applies configuration options, and opens the ClickHouse
// database connection. Unlike SQLite, ClickHouse does not need WAL mode or
// foreign key pragmas.
func (db *ClickHouseDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	sqlDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("clickhousedriver: open: %w", err)
	}

	if db.opts.PoolSize > 0 {
		sqlDB.SetMaxOpenConns(db.opts.PoolSize)
	}

	db.db = sqlDB
	return nil
}

// Close terminates all connections.
func (db *ClickHouseDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks (pre/post query and mutation).
// If engine is nil, hooks are disabled.
func (db *ClickHouseDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Ping verifies that the database is reachable.
func (db *ClickHouseDB) Ping(ctx context.Context) error {
	if db.db == nil {
		return fmt.Errorf("clickhousedriver: database is not initialized; call Open first")
	}
	return db.db.PingContext(ctx)
}

// Exec executes a query that does not return rows (INSERT, ALTER, DDL)
// and returns a driver.Result.
func (db *ClickHouseDB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if db.txConn != nil {
		return db.txConn.Exec(ctx, query, args...)
	}
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: exec: %w", err)
	}
	return &chResult{res: res}, nil
}

// Query executes a query that returns rows and wraps the result in a
// driver.Rows.
func (db *ClickHouseDB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if db.txConn != nil {
		return db.txConn.Query(ctx, query, args...)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: query: %w", err)
	}
	return &chRows{rows: rows}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *ClickHouseDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	row := db.db.QueryRowContext(ctx, query, args...)
	return &chRow{row: row}
}

// BeginTx starts a new database transaction with the specified options.
// Note: ClickHouse has limited transaction support.
func (db *ClickHouseDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	sqlOpts := &sql.TxOptions{}

	if opts != nil {
		sqlOpts.Isolation = mapIsolationLevel(opts.IsolationLevel)
		sqlOpts.ReadOnly = opts.ReadOnly
	}

	tx, err := db.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: begin tx: %w", err)
	}
	return &chTx{tx: tx}, nil
}

// GroveTx is the adapter method for grove.DB.BeginTx(). It bridges the
// grove package's generic transaction interface with ClickHouseDB's typed BeginTx.
// The isolationLevel parameter maps to driver.IsolationLevel constants.
func (db *ClickHouseDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return db.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *ClickHouseDB) GroveSelect(model ...any) any { return db.NewSelect(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *ClickHouseDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *ClickHouseDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *ClickHouseDB) GroveDelete(model any) any { return db.NewDelete(model) }

// Prepare creates a prepared statement for repeated execution.
// If operating within a transaction, it delegates to the transaction's Prepare.
func (db *ClickHouseDB) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if db.txConn != nil {
		if p, ok := db.txConn.(driver.Preparer); ok {
			return p.Prepare(ctx, query)
		}
		return nil, fmt.Errorf("clickhousedriver: transaction does not support Prepare")
	}
	stmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: prepare: %w", err)
	}
	return &chStmt{stmt: stmt}, nil
}

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
		// LevelDefault: return default isolation level.
		return sql.LevelDefault
	}
}
