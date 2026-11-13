package pgdriver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	gometrics "github.com/xraph/go-utils/metrics"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/internal/queryctx"
	"github.com/xraph/grove/observability"
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
	tel      *observability.Telemetry
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
		tel:      newTelemetry(),
	}
}

// newTelemetry builds the db.client telemetry for one PgDB. A Postgres error
// reports its SQLSTATE as db.response.status_code and error.type.
func newTelemetry() *observability.Telemetry {
	return observability.NewTelemetry("postgresql", observability.WithStatusCode(sqlState))
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// SetMetrics records the db.client.operation.duration histogram on m.
// grove.Open calls it when you pass grove.WithMetrics.
func (db *PgDB) SetMetrics(m gometrics.Metrics) {
	db.tel.SetMetrics(m)
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

// buildPoolConfig applies pool settings in order: Grove defaults, connection
// string, then explicit driver options. ApplyOptions alone cannot distinguish
// an explicit option from a default, so options must be applied last here.
func buildPoolConfig(dsn string, opts ...driver.Option) (*pgxpool.Config, *driver.DriverOptions, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("pgdriver: parse dsn: %w", err)
	}

	// pgxpool removes pool parameters after parsing. Parse with pgx too to
	// detect their presence, including keyword DSNs, escaped URL keys and
	// service-file settings. Substring matching can mistake a database name
	// or password for a setting.
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("pgdriver: parse dsn: %w", err)
	}
	o := driver.DefaultDriverOptions()
	if _, ok := connCfg.RuntimeParams["pool_max_conns"]; ok {
		o.PoolSize = int(poolCfg.MaxConns)
	}
	if _, ok := connCfg.RuntimeParams["pool_max_conn_idle_time"]; ok {
		o.MaxConnIdleTime = poolCfg.MaxConnIdleTime
	}
	o.MinConns = poolCfg.MinConns
	o.MaxConnLifetime = poolCfg.MaxConnLifetime
	o.HealthCheckPeriod = poolCfg.HealthCheckPeriod
	poolCfg.MaxConnIdleTime = o.MaxConnIdleTime
	for _, opt := range opts {
		opt(o)
	}

	if o.PoolSize <= 0 {
		return nil, nil, fmt.Errorf("pgdriver: pool size must be positive")
	}
	poolCfg.MaxConns = int32(min(o.PoolSize, math.MaxInt32))
	if o.MinConns >= 0 {
		poolCfg.MinConns = o.MinConns
	}
	// Preserve the existing treatment of non-positive duration options as
	// unset. In pgx, a zero lifetime would expire every new connection.
	if o.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = o.MaxConnLifetime
	}
	if o.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = o.MaxConnIdleTime
	}
	if o.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = o.HealthCheckPeriod
	}
	if poolCfg.HealthCheckPeriod <= 0 {
		return nil, nil, fmt.Errorf("pgdriver: health check period must be positive")
	}
	return poolCfg, o, nil
}

// Open parses the DSN, applies configuration options and creates a lazy
// connection pool. Call Ping to verify connectivity.
// Open requires an unconnected handle; use New to create another pool.
func (db *PgDB) Open(ctx context.Context, dsn string, opts ...driver.Option) error {
	if db.pool != nil {
		return fmt.Errorf("pgdriver: already opened; use New to create another pool")
	}
	poolCfg, driverOpts, err := buildPoolConfig(dsn, opts...)
	if err != nil {
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("pgdriver: open pool: %w", err)
	}

	db.opts = driverOpts
	db.pool = pool
	cc := poolCfg.ConnConfig
	db.tel.SetConnection(cc.Database, cc.Host, int(cc.Port))
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
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	defer cancel()
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
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	defer cancel()
	ctx, op := db.tel.StartSQL(ctx, query)
	ct, err := db.pool.Exec(ctx, query, args...)
	op.End(err)
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
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	ctx, op := db.tel.StartSQL(ctx, query)
	rows, err := db.pool.Query(ctx, query, args...)
	if err != nil {
		cancel()
		op.End(err)
		return nil, fmt.Errorf("pgdriver: query: %w", err)
	}
	return queryctx.Rows(&pgRows{rows: rows, op: op}, cancel), nil
}

// QueryRow executes a query expected to return at most one row.
func (db *PgDB) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if db.txConn != nil {
		return db.txConn.QueryRow(ctx, query, args...)
	}
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	ctx, op := db.tel.StartSQL(ctx, query)
	rows, err := db.pool.Query(ctx, query, args...)
	return queryctx.NewRow(&pgRows{rows: rows}, err, pgx.ErrNoRows, op, cancel)
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

	// COMMIT and ROLLBACK get no context, so the transaction keeps the
	// caller's span to start theirs under.
	parent := trace.SpanFromContext(ctx)
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	defer cancel()
	ctx, op := db.tel.Start(ctx, "BEGIN", "")
	tx, err := db.pool.BeginTx(ctx, pgxOpts)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: begin tx: %w", err)
	}
	return &pgTx{opts: db.opts, tx: tx, tel: db.tel, parent: parent}, nil
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
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	defer cancel()
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
	name := stmtName("grove_ps_")
	sd, err := conn.Conn().Prepare(ctx, name, query)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("pgdriver: prepare: %w", err)
	}
	return &pgPoolStmt{opts: db.opts, conn: conn, sd: sd, query: query, tel: db.tel}, nil
}

// AcquireConn acquires a dedicated connection from the pool.
// All operations on the returned DedicatedConn execute on the same
// underlying PostgreSQL session, making it safe for session-level state
// such as advisory locks.
func (db *PgDB) AcquireConn(ctx context.Context) (driver.DedicatedConn, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, db.opts)
	defer cancel()
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: acquire dedicated conn: %w", err)
	}
	return &pgDedicatedConn{opts: db.opts, conn: conn, tel: db.tel}, nil
}

// pgDedicatedConn wraps a single pgxpool.Conn to implement driver.DedicatedConn.
// All queries execute on the same underlying connection.
type pgDedicatedConn struct {
	opts *driver.DriverOptions
	conn *pgxpool.Conn
	tel  *observability.Telemetry
}

var _ driver.DedicatedConn = (*pgDedicatedConn)(nil)

func (c *pgDedicatedConn) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, c.opts)
	defer cancel()
	ctx, op := c.tel.StartSQL(ctx, query)
	ct, err := c.conn.Exec(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: dedicated exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

func (c *pgDedicatedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, c.opts)
	ctx, op := c.tel.StartSQL(ctx, query)
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		cancel()
		op.End(err)
		return nil, fmt.Errorf("pgdriver: dedicated query: %w", err)
	}
	return queryctx.Rows(&pgRows{rows: rows, op: op}, cancel), nil
}

func (c *pgDedicatedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, cancel := queryctx.WithTimeout(ctx, c.opts)
	ctx, op := c.tel.StartSQL(ctx, query)
	rows, err := c.conn.Query(ctx, query, args...)
	return queryctx.NewRow(&pgRows{rows: rows}, err, pgx.ErrNoRows, op, cancel)
}

func (c *pgDedicatedConn) Release() {
	if c.conn != nil {
		c.conn.Release()
		c.conn = nil
	}
}

func (c *pgDedicatedConn) Discard() {
	if c.conn != nil {
		discardConn(c.conn.Conn())
		c.Release()
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

// SetQueryTimeout configures the timeout for subsequent queries. Configure it
// before using the driver concurrently. grove.Open calls it for an explicit
// grove.WithQueryTimeout option.
func (db *PgDB) SetQueryTimeout(timeout time.Duration) {
	if db.opts == nil {
		db.opts = driver.DefaultDriverOptions()
	}
	opts := *db.opts
	opts.QueryTimeout = timeout
	db.opts = &opts
}
