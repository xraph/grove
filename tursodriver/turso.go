package tursodriver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/schema"
)

// TursoDB implements driver.Driver for Turso/libSQL using database/sql.
// Users must import the libsql driver in their main package:
//
//	import _ "github.com/tursodatabase/go-libsql"
//
// When txConn is set (by TursoTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type TursoDB struct {
	db       *sql.DB
	dialect  *TursoDialect
	opts     *driver.DriverOptions
	topts    tursoOpts
	txConn   driver.Tx        // non-nil when operating inside a transaction
	hooks    *hook.Engine     // optional hook engine for lifecycle hooks
	registry *schema.Registry // cached table metadata to avoid repeated reflection
}

var _ driver.Driver = (*TursoDB)(nil)
var _ driver.Preparer = (*TursoDB)(nil)

// New creates a new unconnected TursoDB. Call Open to establish a connection.
func New() *TursoDB {
	return &TursoDB{
		dialect:  &TursoDialect{},
		registry: schema.NewRegistry(),
	}
}

// Name returns the driver identifier.
func (db *TursoDB) Name() string { return "turso" }

// Dialect returns the Turso/libSQL dialect (SQLite-compatible).
func (db *TursoDB) Dialect() driver.Dialect { return db.dialect }

// SupportsReturning returns true because Turso/libSQL (SQLite 3.35+) supports
// INSERT ... RETURNING.
func (db *TursoDB) SupportsReturning() bool { return true }

// Open parses the DSN, applies configuration options, opens the Turso/libSQL
// database, and configures WAL mode and foreign keys.
//
// The DSN can be a local file path for embedded mode or a Turso cloud URL
// (e.g., "libsql://your-db.turso.io"). If an auth token is provided via
// WithAuthToken, it is appended to the DSN as a query parameter.
//
// The driver tries "libsql" first, then falls back to "sqlite" for local files.
func (db *TursoDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	db.opts = driver.ApplyOptions(opts)
	db.topts = extractTursoOpts(db.opts)

	// Build DSN with auth token if provided.
	finalDSN := dsn
	if db.topts.authToken != "" && !strings.Contains(dsn, "authToken=") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		finalDSN = dsn + sep + "authToken=" + url.QueryEscape(db.topts.authToken)
	}

	// Try libsql driver first, fall back to sqlite for local files.
	driverName := "libsql"
	sqlDB, err := sql.Open(driverName, finalDSN)
	if err != nil {
		// Fallback to sqlite driver for local file paths.
		driverName = "sqlite"
		sqlDB, err = sql.Open(driverName, dsn)
		if err != nil {
			return fmt.Errorf("tursodriver: open: %w", err)
		}
	}

	// Verify connectivity.
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		// Try sqlite fallback if libsql failed.
		if driverName == "libsql" {
			sqlDB2, err2 := sql.Open("sqlite", dsn)
			if err2 == nil {
				if err3 := sqlDB2.PingContext(ctx); err3 == nil {
					sqlDB = sqlDB2
					goto connected
				}
				_ = sqlDB2.Close()
			}
		}
		return fmt.Errorf("tursodriver: ping: %w", err)
	}

connected:
	// Enable WAL and foreign keys for SQLite compatibility.
	// Non-fatal: remote Turso may not support PRAGMA.
	_, _ = sqlDB.ExecContext(ctx, "PRAGMA journal_mode=WAL")
	_, _ = sqlDB.ExecContext(ctx, "PRAGMA foreign_keys=ON")

	if db.opts.PoolSize > 0 {
		sqlDB.SetMaxOpenConns(db.opts.PoolSize)
	}

	db.db = sqlDB
	return nil
}

// Close terminates all connections.
func (db *TursoDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

// SetHooks attaches a hook engine for lifecycle hooks (pre/post query and mutation).
// If engine is nil, hooks are disabled.
func (db *TursoDB) SetHooks(engine *hook.Engine) {
	db.hooks = engine
}

// Ping verifies that the database is reachable.
func (db *TursoDB) Ping(ctx context.Context) error {
	if db.db == nil {
		return fmt.Errorf("tursodriver: database is not initialized; call Open first")
	}
	return db.db.PingContext(ctx)
}

// Exec executes a query that does not return rows (INSERT, UPDATE, DELETE, DDL)
// and returns a driver.Result.
func (db *TursoDB) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if db.txConn != nil {
		return db.txConn.Exec(ctx, query, args...)
	}
	res, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: exec: %w", err)
	}
	return &tursoResult{res: res}, nil
}

// Query executes a query that returns rows and wraps the result in a
// driver.Rows.
func (db *TursoDB) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if db.txConn != nil {
		return db.txConn.Query(ctx, query, args...)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: query: %w", err)
	}
	return &tursoRows{rows: rows}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *TursoDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	row := db.db.QueryRowContext(ctx, query, args...)
	return &tursoRow{row: row}
}

// BeginTx starts a new database transaction with the specified options.
func (db *TursoDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	sqlOpts := &sql.TxOptions{}

	if opts != nil {
		sqlOpts.Isolation = mapIsolationLevel(opts.IsolationLevel)
		sqlOpts.ReadOnly = opts.ReadOnly
	}

	tx, err := db.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: begin tx: %w", err)
	}
	return &tursoTx{tx: tx}, nil
}

// GroveTx is the adapter method for grove.DB.BeginTx(). It bridges the
// grove package's generic transaction interface with TursoDB's typed BeginTx.
// The isolationLevel parameter maps to driver.IsolationLevel constants.
func (db *TursoDB) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return db.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

// GroveSelect is the adapter method for grove.DB.NewSelect().
func (db *TursoDB) GroveSelect(model ...any) any { return db.NewSelect(model...) }

// GroveInsert is the adapter method for grove.DB.NewInsert().
func (db *TursoDB) GroveInsert(model any) any { return db.NewInsert(model) }

// GroveUpdate is the adapter method for grove.DB.NewUpdate().
func (db *TursoDB) GroveUpdate(model any) any { return db.NewUpdate(model) }

// GroveDelete is the adapter method for grove.DB.NewDelete().
func (db *TursoDB) GroveDelete(model any) any { return db.NewDelete(model) }

// Prepare creates a prepared statement for repeated execution.
// If operating within a transaction, it delegates to the transaction's Prepare.
func (db *TursoDB) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if db.txConn != nil {
		if p, ok := db.txConn.(driver.Preparer); ok {
			return p.Prepare(ctx, query)
		}
		return nil, fmt.Errorf("tursodriver: transaction does not support Prepare")
	}
	stmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: prepare: %w", err)
	}
	return &tursoStmt{stmt: stmt}, nil
}

// mapIsolationLevel converts a driver.IsolationLevel to the corresponding
// sql.IsolationLevel constant. SQLite/Turso only truly supports Serializable,
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
