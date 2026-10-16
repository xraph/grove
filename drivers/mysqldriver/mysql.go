package mysqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/go-sql-driver/mysql"
	gometrics "github.com/xraph/go-utils/metrics"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/observability"
	"github.com/xraph/grove/schema"
)

// MysqlDB implements driver.Driver for MySQL using database/sql.
// Call New() to create an instance and then Open() to establish the
// connection pool.
//
// When txConn is set (by MysqlTx), Exec/Query/QueryRow route through
// the transaction instead of the pool.
type MysqlDB struct {
	db       *sql.DB
	dialect  *MysqlDialect
	opts     *driver.DriverOptions
	txConn   driver.Tx        // non-nil when operating inside a transaction
	hooks    *hook.Engine     // optional hook engine for lifecycle hooks
	registry *schema.Registry // cached table metadata to avoid repeated reflection
	tel      *observability.Telemetry
}

var _ driver.Driver = (*MysqlDB)(nil)
var _ driver.Preparer = (*MysqlDB)(nil)
var _ driver.ConnAcquirer = (*MysqlDB)(nil)

// New creates a new unconnected MysqlDB. Call Open to establish a connection pool.
func New() *MysqlDB {
	return &MysqlDB{
		dialect:  &MysqlDialect{},
		registry: schema.NewRegistry(),
		tel:      newTelemetry(),
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

	// Apply pool options.
	if db.opts.PoolSize > 0 {
		sqlDB.SetMaxOpenConns(db.opts.PoolSize)
	}
	if db.opts.MinConns > 0 {
		sqlDB.SetMaxIdleConns(int(db.opts.MinConns))
	}
	if db.opts.MaxConnLifetime > 0 {
		sqlDB.SetConnMaxLifetime(db.opts.MaxConnLifetime)
	}
	if db.opts.MaxConnIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(db.opts.MaxConnIdleTime)
	}

	db.db = sqlDB
	db.tel.SetConnection(connectionOf(dsn))
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
	ctx, op := db.tel.StartSQL(ctx, query)
	res, err := db.db.ExecContext(ctx, query, args...)
	op.End(err)
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
	ctx, op := db.tel.StartSQL(ctx, query)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("mysqldriver: query: %w", err)
	}
	return &mysqlRows{rows: rows, op: op}, nil
}

// QueryRow executes a query expected to return at most one row.
func (db *MysqlDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	ctx, op := db.tel.StartSQL(ctx, query)
	row := db.db.QueryRowContext(ctx, query, args...)
	return &mysqlRow{row: row, op: op}
}

// BeginTx starts a new database transaction with the specified options.
func (db *MysqlDB) BeginTx(ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	sqlOpts := &sql.TxOptions{}

	if opts != nil {
		sqlOpts.Isolation = mapIsolationLevel(opts.IsolationLevel)
		sqlOpts.ReadOnly = opts.ReadOnly
	}

	// COMMIT and ROLLBACK get no context, so the transaction keeps the
	// caller's span to start theirs under.
	parent := trace.SpanFromContext(ctx)
	ctx, op := db.tel.Start(ctx, "BEGIN", "")
	tx, err := db.db.BeginTx(ctx, sqlOpts)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: begin tx: %w", err)
	}
	return &mysqlTx{tx: tx, tel: db.tel, parent: parent}, nil
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

// Prepare creates a prepared statement for repeated execution.
// If operating within a transaction, it delegates to the transaction's Prepare.
func (db *MysqlDB) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if db.txConn != nil {
		if p, ok := db.txConn.(driver.Preparer); ok {
			return p.Prepare(ctx, query)
		}
		return nil, fmt.Errorf("mysqldriver: transaction does not support Prepare")
	}
	stmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: prepare: %w", err)
	}
	return &mysqlStmt{stmt: stmt, query: query, tel: db.tel}, nil
}

// AcquireConn acquires a dedicated connection from the pool.
// All operations on the returned DedicatedConn execute on the same
// underlying MySQL session, making it safe for session-level state
// such as GET_LOCK advisory locks.
func (db *MysqlDB) AcquireConn(ctx context.Context) (driver.DedicatedConn, error) {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: acquire dedicated conn: %w", err)
	}
	return &mysqlDedicatedConn{conn: conn, tel: db.tel}, nil
}

// mysqlDedicatedConn wraps a single *sql.Conn to implement driver.DedicatedConn.
// All queries execute on the same underlying connection.
type mysqlDedicatedConn struct {
	conn *sql.Conn
	tel  *observability.Telemetry
}

var _ driver.DedicatedConn = (*mysqlDedicatedConn)(nil)

func (c *mysqlDedicatedConn) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, op := c.tel.StartSQL(ctx, query)
	res, err := c.conn.ExecContext(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: dedicated exec: %w", err)
	}
	return &mysqlResult{res: res}, nil
}

func (c *mysqlDedicatedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, op := c.tel.StartSQL(ctx, query)
	rows, err := c.conn.QueryContext(ctx, query, args...) //nolint:rowserrcheck // caller checks Err() via driver.Rows
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("mysqldriver: dedicated query: %w", err)
	}
	return &mysqlRows{rows: rows, op: op}, nil
}

func (c *mysqlDedicatedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, op := c.tel.StartSQL(ctx, query)
	row := c.conn.QueryRowContext(ctx, query, args...)
	return &mysqlRow{row: row, op: op}
}

func (c *mysqlDedicatedConn) Release() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
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
		// LevelDefault: return default so database uses its default.
		return sql.LevelDefault
	}
}

// SetMetrics records the db.client.operation.duration histogram on m.
// grove.Open calls it when you pass grove.WithMetrics.
func (db *MysqlDB) SetMetrics(m gometrics.Metrics) {
	db.tel.SetMetrics(m)
}

// newTelemetry builds the db.client telemetry for one MysqlDB. A MySQL
// server error reports its error number (1062 for a duplicate key) as
// db.response.status_code and error.type.
func newTelemetry() *observability.Telemetry {
	return observability.NewTelemetry("mysql", observability.WithStatusCode(errorNumber))
}

func errorNumber(err error) string {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return strconv.Itoa(int(myErr.Number))
	}
	return ""
}

// connectionOf reads the database name and the TCP address out of a DSN for
// the span attributes. A DSN it can't parse, or a unix socket, reports only
// what it can.
func connectionOf(dsn string) (namespace, address string, port int) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", 0
	}
	if cfg.Net != "tcp" {
		return cfg.DBName, "", 0
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return cfg.DBName, cfg.Addr, 0
	}
	port, _ = strconv.Atoi(portStr) //nolint:errcheck // a bad port is reported as none
	return cfg.DBName, host, port
}
