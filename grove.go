package grove

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/grove/hook"
)

// GroveDriver is the minimal interface that a database driver must satisfy
// to work with the top-level DB handle. This avoids importing the driver
// package directly, which would create a circular dependency
// (schema -> grove -> driver -> schema).
//
// The full driver.Driver interface (in the driver package) extends this
// with query execution methods.
type GroveDriver interface { //nolint:revive // GroveDriver is the established public API name
	// Name returns the driver identifier (e.g., "pg", "mysql", "mongo").
	Name() string

	// Close terminates all connections.
	Close() error

	// Ping checks connectivity.
	Ping(ctx context.Context) error
}

// ModelRegistry is the interface for model metadata caching.
// The concrete implementation is schema.Registry. This interface exists
// to avoid an import cycle between grove and schema packages.
type ModelRegistry interface {
	// Register registers a model and returns its cached metadata.
	Register(model any) (any, error)

	// Get returns cached metadata for a registered model, or nil.
	Get(model any) any
}

// DB is the top-level database handle. It manages the connection pool,
// model registry, hook engine, and provides the entry point for all
// database operations.
//
// Create a DB using Open:
//
//	pgdb := pgdriver.New()
//	pgdb.Open(ctx, "postgres://localhost:5432/mydb")
//	db, err := grove.Open(pgdb)
type DB struct {
	drv   GroveDriver
	opts  *options
	hooks *hook.Engine

	mu     sync.RWMutex
	closed bool

	// models tracks registered model types (simple map fallback).
	models map[string]any
}

// Open creates a new DB with the given driver. The driver must already be
// connected (call driver.Open before grove.Open).
//
//	pgdb := pgdriver.New()
//	pgdb.Open(ctx, "postgres://localhost:5432/mydb", driver.WithPoolSize(20))
//	db, err := grove.Open(pgdb)
func Open(drv GroveDriver, opts ...Option) (*DB, error) {
	o := defaultOptions()
	o.apply(opts)

	if drv == nil {
		return nil, fmt.Errorf("grove: driver must not be nil")
	}

	db := &DB{
		drv:    drv,
		opts:   o,
		hooks:  hook.NewEngine(),
		models: make(map[string]any),
	}

	return db, nil
}

// Driver returns the underlying driver.
func (db *DB) Driver() GroveDriver {
	return db.drv
}

// Hooks returns the hook engine for registering pre/post query and mutation hooks.
//
//	db.Hooks().AddHook(&TenantIsolation{}, hook.Scope{Tables: []string{"users"}})
func (db *DB) Hooks() *hook.Engine {
	return db.hooks
}

// Close closes the database connection pool and releases all resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDriverClosed
	}
	db.closed = true

	return db.drv.Close()
}

// Ping verifies the database connection is alive.
func (db *DB) Ping(ctx context.Context) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrDriverClosed
	}

	return db.drv.Ping(ctx)
}

// RegisterModel registers model types for later use.
// Reflection is performed once per model type and cached by the driver.
//
//	db.RegisterModel((*User)(nil), (*Post)(nil))
func (db *DB) RegisterModel(models ...any) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, model := range models {
		key := fmt.Sprintf("%T", model)
		db.models[key] = model
	}
}

// TxOptions holds transaction configuration for BeginTx.
type TxOptions struct {
	IsolationLevel int
	ReadOnly       bool
}

// Tx wraps a driver transaction, exposing Commit and Rollback.
type Tx struct {
	underlying any
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	type committer interface{ Commit() error }
	if c, ok := tx.underlying.(committer); ok {
		return c.Commit()
	}
	return fmt.Errorf("grove: transaction does not support Commit")
}

// Rollback rolls back the transaction.
func (tx *Tx) Rollback() error {
	type rollbacker interface{ Rollback() error }
	if r, ok := tx.underlying.(rollbacker); ok {
		return r.Rollback()
	}
	return fmt.Errorf("grove: transaction does not support Rollback")
}

// Raw returns the underlying driver transaction for advanced usage.
// The returned value should be type-asserted to the driver-specific Tx type
// (e.g., driver.Tx for pgdriver).
func (tx *Tx) Raw() any {
	return tx.underlying
}

// txBeginner is the adapter interface for drivers that support transactions.
// Drivers implement this with matching signature to avoid import cycles.
// See PgDB.GroveTx for the PostgreSQL implementation.
type txBeginner interface {
	GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error)
}

// BeginTx starts a new transaction. The returned Tx wraps the driver
// transaction. Use pgdriver.Unwrap(db).BeginTx() for full driver.Tx access.
//
//	tx, err := db.BeginTx(ctx, nil)
//	defer tx.Rollback()
//	// ... use pgdriver.Unwrap with tx ...
//	tx.Commit()
func (db *DB) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrDriverClosed
	}

	starter, ok := db.drv.(txBeginner)
	if !ok {
		return nil, ErrNotSupported
	}

	isoLevel := 0
	readOnly := false
	if opts != nil {
		isoLevel = opts.IsolationLevel
		readOnly = opts.ReadOnly
	}

	raw, err := starter.GroveTx(ctx, isoLevel, readOnly)
	if err != nil {
		return nil, err
	}

	return &Tx{underlying: raw}, nil
}

// queryBuilder is the adapter interface for drivers that provide typed query
// builders. Drivers implement these methods returning any to bridge the gap
// between concrete return types and this generic interface.
// See PgDB.GroveSelect, GroveInsert, GroveUpdate, GroveDelete.
type queryBuilder interface {
	GroveSelect(model ...any) any
	GroveInsert(model any) any
	GroveUpdate(model any) any
	GroveDelete(model any) any
}

// NewSelect creates a new SELECT query builder via the driver.
// The returned value should be type-asserted to the driver-specific builder
// (e.g., *pgdriver.SelectQuery).
//
// For typed access, prefer: pgdriver.Unwrap(db).NewSelect(model)
func (db *DB) NewSelect(model ...any) any {
	if qb, ok := db.drv.(queryBuilder); ok {
		return qb.GroveSelect(model...)
	}
	return nil
}

// NewInsert creates a new INSERT query builder via the driver.
// The returned value should be type-asserted to the driver-specific builder.
//
// For typed access, prefer: pgdriver.Unwrap(db).NewInsert(model)
func (db *DB) NewInsert(model any) any {
	if qb, ok := db.drv.(queryBuilder); ok {
		return qb.GroveInsert(model)
	}
	return nil
}

// NewUpdate creates a new UPDATE query builder via the driver.
// The returned value should be type-asserted to the driver-specific builder.
//
// For typed access, prefer: pgdriver.Unwrap(db).NewUpdate(model)
func (db *DB) NewUpdate(model any) any {
	if qb, ok := db.drv.(queryBuilder); ok {
		return qb.GroveUpdate(model)
	}
	return nil
}

// NewDelete creates a new DELETE query builder via the driver.
// The returned value should be type-asserted to the driver-specific builder.
//
// For typed access, prefer: pgdriver.Unwrap(db).NewDelete(model)
func (db *DB) NewDelete(model any) any {
	if qb, ok := db.drv.(queryBuilder); ok {
		return qb.GroveDelete(model)
	}
	return nil
}
