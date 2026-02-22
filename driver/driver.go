// Package driver defines the interfaces that every database backend must
// implement to work with Grove. It is deliberately thin: the core types are
// interfaces so that concrete drivers (PostgreSQL, MySQL, SQLite, MongoDB, ...)
// live in their own packages and depend on this contract without pulling in
// any specific database library.
package driver

import (
	"context"
	"log/slog"
	"time"
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
type DriverOptions struct {
	PoolSize     int
	QueryTimeout time.Duration
	Logger       *slog.Logger
}

// DefaultDriverOptions returns a DriverOptions with sensible defaults.
func DefaultDriverOptions() *DriverOptions {
	return &DriverOptions{
		PoolSize:     10,
		QueryTimeout: 30 * time.Second,
		Logger:       slog.Default(),
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

// WithLogger returns an Option that sets the structured logger.
func WithLogger(l *slog.Logger) Option {
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
