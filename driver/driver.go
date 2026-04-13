// Package driver defines the interfaces that every database backend must
// implement to work with Grove. It is deliberately thin: the core types are
// interfaces so that concrete drivers (PostgreSQL, MySQL, SQLite, MongoDB, ...)
// live in their own packages and depend on this contract without pulling in
// any specific database library.
package driver

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"
)

// Driver is the core interface every database backend implements.
type Driver interface {
	// Name returns the driver identifier (e.g., "pg", "mysql", "mongo").
	Name() string

	// Open initializes a connection using the given DSN.
	Open(ctx context.Context, dsn string, opts ...Option) error

	// Close terminates all connections.
	Close() error

	// Dialect returns the driver's SQL/query dialect.
	Dialect() Dialect

	// Ping checks connectivity.
	Ping(ctx context.Context) error

	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *TxOptions) (Tx, error)

	// Exec executes a query that doesn't return rows.
	Exec(ctx context.Context, query string, args ...any) (Result, error)

	// Query executes a query that returns rows.
	Query(ctx context.Context, query string, args ...any) (Rows, error)

	// QueryRow executes a query that returns at most one row.
	QueryRow(ctx context.Context, query string, args ...any) Row

	// SupportsReturning indicates if INSERT...RETURNING is supported.
	SupportsReturning() bool
}

// Option configures a driver during Open.
type Option func(*DriverOptions)

// DriverOptions holds driver-level configuration.
type DriverOptions struct { //nolint:revive // DriverOptions is the established public API name
	PoolSize          int
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	QueryTimeout      time.Duration
	Logger            log.Logger
	Extra             map[string]any // driver-specific options
}

// DefaultDriverOptions returns a DriverOptions with sensible defaults.
func DefaultDriverOptions() *DriverOptions {
	return &DriverOptions{
		PoolSize:     10,
		QueryTimeout: 30 * time.Second,
		Logger:       log.NewNoopLogger(),
	}
}

// WithPoolSize returns an Option that sets the connection pool size.
func WithPoolSize(n int) Option {
	return func(o *DriverOptions) {
		o.PoolSize = n
	}
}

// WithQueryTimeout returns an Option that sets the default query timeout.
func WithQueryTimeout(d time.Duration) Option {
	return func(o *DriverOptions) {
		o.QueryTimeout = d
	}
}

// WithMinConns returns an Option that sets the minimum number of connections
// in the pool. Connections above this count may be closed when idle.
func WithMinConns(n int32) Option {
	return func(o *DriverOptions) {
		o.MinConns = n
	}
}

// WithMaxConnLifetime returns an Option that sets the maximum lifetime of a
// connection. Connections older than this duration are closed and replaced.
func WithMaxConnLifetime(d time.Duration) Option {
	return func(o *DriverOptions) {
		o.MaxConnLifetime = d
	}
}

// WithMaxConnIdleTime returns an Option that sets the maximum time a
// connection can sit idle before it is closed.
func WithMaxConnIdleTime(d time.Duration) Option {
	return func(o *DriverOptions) {
		o.MaxConnIdleTime = d
	}
}

// WithHealthCheckPeriod returns an Option that sets the interval between
// automatic health checks on idle connections. On serverless databases like
// Neon, each health check costs bandwidth; consider setting this to 5m+.
func WithHealthCheckPeriod(d time.Duration) Option {
	return func(o *DriverOptions) {
		o.HealthCheckPeriod = d
	}
}

// WithLogger returns an Option that sets the structured logger.
func WithLogger(l log.Logger) Option {
	return func(o *DriverOptions) {
		o.Logger = l
	}
}

// ApplyOptions folds the given options onto a set of default DriverOptions and
// returns the resulting configuration.
func ApplyOptions(opts []Option) *DriverOptions {
	o := DefaultDriverOptions()
	for _, fn := range opts {
		fn(o)
	}
	return o
}

// StreamCapable is an optional interface that drivers implement
// to indicate support for streaming/cursor-based result iteration.
// Drivers that don't support streaming simply don't implement this.
type StreamCapable interface {
	// SupportsStreaming returns true if the driver supports server-side cursors
	// or equivalent streaming mechanisms.
	SupportsStreaming() bool

	// SupportsCDC returns true if the driver supports change data capture
	// (e.g., PG logical replication, Mongo change streams, MySQL binlog).
	SupportsCDC() bool
}

// Preparer is an optional interface for drivers that support prepared statements.
// Used for efficient bulk inserts via prepared-statement loops.
type Preparer interface {
	Prepare(ctx context.Context, query string) (Stmt, error)
}

// Stmt is a prepared statement that can be executed multiple times with
// different arguments. It must be closed when no longer needed.
type Stmt interface {
	Exec(ctx context.Context, args ...any) (Result, error)
	Close() error
}

// ConnAcquirer is an optional interface that pool-based drivers implement
// to provide a dedicated connection from the pool. This is needed for
// operations that require session-level state (e.g., advisory locks)
// to remain on a single connection across multiple queries.
type ConnAcquirer interface {
	// AcquireConn acquires a dedicated connection from the pool.
	// The returned DedicatedConn provides Exec/Query/QueryRow methods
	// that are guaranteed to run on the same underlying connection.
	// The caller MUST call Release() when done.
	AcquireConn(ctx context.Context) (DedicatedConn, error)
}

// DedicatedConn represents a single, dedicated database connection
// acquired from a pool. All operations execute on the same underlying
// connection, making it safe for session-level state like advisory locks.
type DedicatedConn interface {
	Exec(ctx context.Context, query string, args ...any) (Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
	Release()
}
