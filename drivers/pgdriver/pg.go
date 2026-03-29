package pgdriver

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/schema"
)

// PgDB implements driver.Driver for PostgreSQL using pgxpool.
// Call New() to create an instance and then Open() to establish the
// connection pool.
//
// When txConn is set (by PgTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type PgDB struct {
	pool     *pgxpool.Pool
	dialect  *PgDialect
	opts     *driver.DriverOptions
	txConn   driver.Tx        // non-nil when operating inside a transaction
	hooks    *hook.Engine     // optional hook engine for lifecycle hooks
	registry *schema.Registry // cached table metadata to avoid repeated reflection
}

var _ driver.Driver = (*PgDB)(nil)
var _ driver.StreamCapable = (*PgDB)(nil)
var _ driver.Preparer = (*PgDB)(nil)
var _ driver.ConnAcquirer = (*PgDB)(nil)

// New creates a new unconnected PgDB. Call Open to establish a connection pool.
func New() *PgDB {
	return &PgDB{
		dialect:  &PgDialect{},
		registry: schema.NewRegistry(),
	}
}

// Name returns the driver identifier.
func (db *PgDB) Name() string { return "pg" }

// Dialect returns the PostgreSQL dialect.
func (db *PgDB) Dialect() driver.Dialect { return db.dialect }

// SupportsReturning returns true because PostgreSQL supports
// INSERT ... RETURNING.
func (db *PgDB) SupportsReturning() bool { return true }

// SupportsStreaming returns true because PostgreSQL supports server-side cursors.
func (db *PgDB) SupportsStreaming() bool { return true }

// SupportsCDC returns true because PostgreSQL supports logical replication
// for change data capture.
func (db *PgDB) SupportsCDC() bool { return true }

// Open parses the DSN, applies configuration options, creates the pgxpool
// connection pool, and verifies connectivity.
func (db *PgDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("pgdriver: parse dsn: %w", err)
	}

	// Apply pool size from options.
	if db.opts.PoolSize > 0 {
		poolCfg.MaxConns = int32(db.opts.PoolSize)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("pgdriver: open pool: %w", err)
	}

	db.pool = pool
	return nil
}

// Close terminates all connections in the pool.
func (db *PgDB) Close() error {
	if db.pool != nil {
		db.pool.Close()
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks (pre/post query and mutation).
// If engine is nil, hooks are disabled.
func (db *PgDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Ping verifies that the database is reachable.
func (db *PgDB) Ping(ctx context.Context) error {
	if db.pool == nil {
		return fmt.Errorf("pgdriver: connection pool is not initialized; call Open first")
	}
	return db.pool.Ping(ctx)
}

// Exec executes a query that does not return rows (INSERT, UPDATE, DELETE, DDL)
// and returns a driver.Result.
func (db *PgDB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if db.txConn != nil {
		return db.txConn.Exec(ctx, query, args...)
	}
	ct, err := db.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

// Query executes a query that returns rows and wraps the result in a
// driver.Rows.
func (db *PgDB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if db.txConn != nil {
		return db.txConn.Query(ctx, query, args...)
	}
	rows, err := db.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: query: %w", err)
	}
	return &pgRows{rows: rows}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *PgDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	row := db.pool.QueryRow(ctx, query, args...)
	return &pgRow{row: row}
}

// BeginTx starts a new database transaction with the specified options.
func (db *PgDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	pgxOpts := pgx.TxOptions{}

	if opts != nil {
		pgxOpts.IsoLevel = mapIsolationLevel(opts.IsolationLevel)
		if opts.ReadOnly {
			pgxOpts.AccessMode = pgx.ReadOnly
		}
	}

	tx, err := db.pool.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: begin tx: %w", err)
	}
	return &pgTx{tx: tx}, nil
}

// GroveTx is the adapter method for grove.DB.BeginTx(). It bridges the
// grove package's generic transaction interface with PgDB's typed BeginTx.
// The isolationLevel parameter maps to driver.IsolationLevel constants.
func (db *PgDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return db.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *PgDB) GroveSelect(model ...any) any { return db.NewSelect(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *PgDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *PgDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *PgDB) GroveDelete(model any) any { return db.NewDelete(model) }

// Prepare creates a prepared statement for repeated execution.
// If operating within a transaction, it delegates to the transaction's Prepare.
// For pool connections, it acquires a connection from the pool, prepares the
// statement on it, and returns a pgPoolStmt that releases the connection on Close.
func (db *PgDB) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if db.txConn != nil {
		if p, ok := db.txConn.(driver.Preparer); ok {
			return p.Prepare(ctx, query)
		}
		return nil, fmt.Errorf("pgdriver: transaction does not support Prepare")
	}
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: acquire conn for prepare: %w", err)
	}
	name := fmt.Sprintf("grove_ps_%p", conn)
	sd, err := conn.Conn().Prepare(ctx, name, query)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("pgdriver: prepare: %w", err)
	}
	return &pgPoolStmt{conn: conn, sd: sd}, nil
}

// AcquireConn acquires a dedicated connection from the pool.
// All operations on the returned DedicatedConn execute on the same
// underlying PostgreSQL session, making it safe for session-level state
// such as advisory locks.
func (db *PgDB) AcquireConn(ctx context.Context) (driver.DedicatedConn, error) {
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: acquire dedicated conn: %w", err)
	}
	return &pgDedicatedConn{conn: conn}, nil
}

// pgDedicatedConn wraps a single pgxpool.Conn to implement driver.DedicatedConn.
// All queries execute on the same underlying connection.
type pgDedicatedConn struct {
	conn *pgxpool.Conn
}

var _ driver.DedicatedConn = (*pgDedicatedConn)(nil)

func (c *pgDedicatedConn) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ct, err := c.conn.Exec(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: dedicated exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

func (c *pgDedicatedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: dedicated query: %w", err)
	}
	return &pgRows{rows: rows}, nil
}

func (c *pgDedicatedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	row := c.conn.QueryRow(ctx, query, args...)
	return &pgRow{row: row}
}

func (c *pgDedicatedConn) Release() {
	if c.conn != nil {
		c.conn.Release()
		c.conn = nil
	}
}

// mapIsolationLevel converts a driver.IsolationLevel to the corresponding
// pgx.TxIsoLevel string constant.
func mapIsolationLevel(level driver.IsolationLevel) pgx.TxIsoLevel {
	switch level {
	case driver.LevelReadUncommitted:
		return pgx.ReadUncommitted
	case driver.LevelReadCommitted:
		return pgx.ReadCommitted
	case driver.LevelRepeatableRead:
		return pgx.RepeatableRead
	case driver.LevelSerializable:
		return pgx.Serializable
	default:
		// LevelDefault: return empty string so pgx uses the database default.
		return ""
	}
}
