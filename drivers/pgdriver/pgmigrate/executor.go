// Package pgmigrate provides a PostgreSQL-specific migration executor
// for the Grove migration system.
package pgmigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

func init() {
	migrate.RegisterExecutor(driverName, func(drv any) migrate.Executor {
		d, ok := drv.(driver.Driver)
		if !ok {
			return unusableExecutor{err: fmt.Errorf("pgmigrate: %T does not implement driver.Driver", drv)}
		}
		return New(d)
	})
	migrate.RegisterRenderer(driverName, Renderer{})
}

const (
	migrationTableName = "grove_migrations"
	lockTableName      = "grove_migration_locks"
)

// Executor implements migrate.Executor for PostgreSQL.
//
// When the underlying driver supports [driver.ConnAcquirer], the executor
// acquires a dedicated connection in [AcquireLock] and routes ALL subsequent
// operations through it until [ReleaseLock]. This guarantees that the
// session-level pg_try_advisory_lock is acquired and released on the same
// connection, preventing advisory-lock leaks in pooled environments.
//
// That same connection carries the transaction a migration runs in (see
// [Executor.BeginTx]), which is why the transaction lives here and not with
// the caller: every statement has to land on the session that holds the lock.
type Executor struct {
	drv       driver.Driver
	dedicated driver.DedicatedConn // non-nil while migration lock is held
	inTx      bool                 // a BEGIN on dedicated has not been committed or rolled back
}

var (
	_ migrate.Executor      = (*Executor)(nil)
	_ migrate.LockInspector = (*Executor)(nil)
	_ migrate.TxExecutor    = (*Executor)(nil)
	_ migrate.DriverNamer   = (*Executor)(nil)
)

// New creates a new PostgreSQL migration executor.
func New(drv driver.Driver) *Executor {
	return &Executor{drv: drv}
}

// --- routing helpers: prefer dedicated conn when available ---

func (e *Executor) exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	if e.dedicated != nil {
		return e.dedicated.Exec(ctx, query, args...)
	}
	return e.drv.Exec(ctx, query, args...)
}

func (e *Executor) query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if e.dedicated != nil {
		return e.dedicated.Query(ctx, query, args...)
	}
	return e.drv.Query(ctx, query, args...)
}

func (e *Executor) queryRow(ctx context.Context, query string, args ...any) driver.Row {
	if e.dedicated != nil {
		return e.dedicated.QueryRow(ctx, query, args...)
	}
	return e.drv.QueryRow(ctx, query, args...)
}

func (e *Executor) releaseDedicated() {
	e.inTx = false
	if e.dedicated != nil {
		// Safety net: release ALL advisory locks held on this session before
		// returning the connection to the pool. This prevents lock leaks if
		// pg_advisory_unlock(1) failed or was skipped for any reason.
		_, _ = e.dedicated.Exec(context.Background(), "SELECT pg_advisory_unlock_all()") //nolint:errcheck // best-effort cleanup
		e.dedicated.Release()
		e.dedicated = nil
	}
}

// DriverName reports the driver this executor runs on. The builders use it to
// pick the schema renderer registered for Postgres.
func (e *Executor) DriverName() string { return driverName }

// BeginTx opens a transaction on the connection that holds the migration lock.
// Exec, Query, RecordApplied and RemoveApplied all prefer that connection, so
// until CommitTx or RollbackTx they run inside the transaction.
//
// Without a dedicated connection the transaction and the lock can't share a
// session, so BeginTx returns [migrate.ErrTxUnsupported] and the migration
// runs without one.
func (e *Executor) BeginTx(ctx context.Context) error {
	if e.dedicated == nil {
		return migrate.ErrTxUnsupported
	}
	if e.inTx {
		return errors.New("pgmigrate: begin: a transaction is already open")
	}
	if _, err := e.dedicated.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("pgmigrate: begin: %w", err)
	}
	e.inTx = true
	return nil
}

// CommitTx commits the transaction BeginTx opened.
func (e *Executor) CommitTx(ctx context.Context) error {
	if !e.inTx || e.dedicated == nil {
		return errors.New("pgmigrate: commit: no transaction is open")
	}
	if _, err := e.dedicated.Exec(ctx, "COMMIT"); err != nil {
		// The transaction may still be open (a dropped connection) or already
		// over (a deferred constraint failed at commit). Leave it marked open
		// so RollbackTx, which the orchestrator calls next, settles it either way.
		return fmt.Errorf("pgmigrate: commit: %w", err)
	}
	e.inTx = false
	return nil
}

// RollbackTx abandons the transaction BeginTx opened. It returns nil when none
// is open. After a failed CommitTx the transaction still counts as open, so it
// sends ROLLBACK anyway, which Postgres answers with a warning, not an error,
// when the server has already ended the transaction.
func (e *Executor) RollbackTx(ctx context.Context) error {
	if !e.inTx || e.dedicated == nil {
		return nil
	}
	if _, err := e.dedicated.Exec(ctx, "ROLLBACK"); err != nil {
		return fmt.Errorf("pgmigrate: rollback: %w", err)
	}
	e.inTx = false
	return nil
}

// Exec executes a SQL statement that does not return rows.
func (e *Executor) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return e.exec(ctx, query, args...)
}

// Query executes a SQL statement that returns rows.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return e.query(ctx, query, args...)
}

// EnsureMigrationTable creates the grove_migrations table if it doesn't exist.
func (e *Executor) EnsureMigrationTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          BIGSERIAL PRIMARY KEY,
		version     VARCHAR(14) NOT NULL,
		name        VARCHAR(255) NOT NULL,
		"group"     VARCHAR(255) NOT NULL,
		migrated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE(version, "group")
	)`, migrationTableName)
	return e.ensureTable(ctx, query)
}

// EnsureLockTable creates the grove_migration_locks table if it doesn't exist.
func (e *Executor) EnsureLockTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INT PRIMARY KEY DEFAULT 1,
		locked_at   TIMESTAMPTZ,
		locked_by   VARCHAR(255),
		CONSTRAINT single_lock CHECK (id = 1)
	)`, lockTableName)
	return e.ensureTable(ctx, query)
}

// bootstrapLockKey is the transaction-level advisory lock ensureTable holds
// while it creates grove's own tables. It is the FNV-1a 64-bit hash of
// "github.com/xraph/grove/drivers/pgdriver/pgmigrate.bootstrap"
// (0x547753172061a306) read as an int64, so it stays clear of the migration
// lock's key 1 and of keys an application is likely to pick.
const bootstrapLockKey int64 = 6086424780207137542

// ensureTable runs one of grove's CREATE TABLE IF NOT EXISTS statements under
// bootstrapLockKey.
//
// The migration lock can't protect these statements: it records its holder
// in grove_migration_locks, so the tables have to exist before anyone takes
// it. And Postgres doesn't make CREATE TABLE IF NOT EXISTS atomic against a
// concurrent create. Two sessions both pass the existence check, and the
// loser fails with 42P07 on the table or its serial sequence, 42710 on its
// row type, or 23505 on a catalog index. On a fresh database every replica
// booting at once runs that race.
//
// pg_advisory_xact_lock queues the sessions instead. Each one takes the lock
// in its own short transaction, so the next one in line runs its check after
// the previous CREATE has committed and finds the table. Postgres drops a
// transaction-level lock at COMMIT or ROLLBACK, so nothing can leak back into
// the pool, and the lock is held for one DDL statement at a time, never for
// a whole migration run.
//
// A process on an older grove takes no bootstrap lock and can still collide
// with this one, so the loser's errors are still treated as success (see
// [ignoreConcurrentCreate]).
//
// Once AcquireLock has a dedicated connection the tables already exist (the
// lock is recorded in one of them), so the statement runs there directly.
func (e *Executor) ensureTable(ctx context.Context, query string) error {
	if e.dedicated != nil {
		_, err := e.exec(ctx, query)
		return ignoreConcurrentCreate(err)
	}

	tx, err := e.drv.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgmigrate: bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck // no-op after Commit; on error the transaction is abandoned anyway

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", bootstrapLockKey); err != nil {
		return fmt.Errorf("pgmigrate: bootstrap lock: %w", err)
	}
	if _, err := tx.Exec(ctx, query); err != nil {
		return ignoreConcurrentCreate(err)
	}
	return tx.Commit()
}

// ignoreConcurrentCreate swallows the errors a concurrent CREATE TABLE IF NOT
// EXISTS produces in the session that loses the race:
//
//   - 23505 unique_violation, when both sessions insert the table's row type
//     and the loser collides on pg_type_typname_nsp_index
//   - 42P07 duplicate_table, when the winner's table, serial sequence or
//     unique index commits between the loser's existence check and its own
//     create
//   - 42710 duplicate_object, the same for the table's row type
//
// Each one means another session has committed the table, so the ensure step
// has effectively succeeded. [Executor.ensureTable] keeps grove's own
// executors out of this race; the codes still matter when a process running
// an older grove creates the same tables at the same moment (e.g. forge dev
// starting several extensions on one database during a rolling upgrade).
func ignoreConcurrentCreate(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if slices.Contains(concurrentCreateCodes, pgErr.Code) {
			return nil
		}
		return err
	}
	// Fallback when the driver doesn't surface a typed *pgconn.PgError.
	msg := err.Error()
	for _, code := range concurrentCreateCodes {
		if strings.Contains(msg, "SQLSTATE "+code) {
			return nil
		}
	}
	return err
}

// concurrentCreateCodes are the SQLSTATEs ignoreConcurrentCreate accepts.
var concurrentCreateCodes = []string{"23505", "42P07", "42710"}

// AcquireLock attempts to acquire the distributed migration lock using
// PostgreSQL advisory locks for immediate feedback.
//
// If the driver implements [driver.ConnAcquirer], a dedicated connection is
// acquired first so that the advisory lock and all subsequent migration
// operations share the same session. This prevents lock leaks when using
// connection pools.
func (e *Executor) AcquireLock(ctx context.Context, lockedBy string) error {
	// Acquire a dedicated connection if the driver supports it.
	if acq, ok := e.drv.(driver.ConnAcquirer); ok {
		conn, err := acq.AcquireConn(ctx)
		if err != nil {
			return fmt.Errorf("pgmigrate: acquire dedicated conn: %w", err)
		}
		e.dedicated = conn
	}

	// Use PostgreSQL advisory lock with a fixed key.
	// pg_try_advisory_lock returns true if lock acquired, false otherwise.
	row := e.queryRow(ctx, "SELECT pg_try_advisory_lock(1)")
	var acquired bool
	if err := row.Scan(&acquired); err != nil {
		e.releaseDedicated()
		return fmt.Errorf("pgmigrate: advisory lock: %w", err)
	}
	if !acquired {
		e.releaseDedicated()
		return fmt.Errorf("pgmigrate: %w", migrate.ErrLockHeld)
	}

	// Record who holds the lock for debugging.
	query := fmt.Sprintf(`INSERT INTO %s (id, locked_at, locked_by)
		VALUES (1, NOW(), $1)
		ON CONFLICT (id) DO UPDATE SET locked_at = NOW(), locked_by = $1`,
		lockTableName)
	if _, err := e.exec(ctx, query, lockedBy); err != nil {
		// Best-effort unlock before releasing the connection. Use a
		// background context so the unlock succeeds even if ctx is cancelled.
		_, _ = e.exec(context.Background(), "SELECT pg_advisory_unlock(1)") //nolint:errcheck // best-effort cleanup on error path
		e.releaseDedicated()
		return err
	}

	return nil
}

// ReleaseLock releases the distributed migration lock.
// If a dedicated connection was acquired in [AcquireLock], the advisory lock
// is released on that same connection before the connection is returned to
// the pool.
func (e *Executor) ReleaseLock(ctx context.Context) error {
	// Use an uncancellable context so the advisory lock is always released,
	// even if the caller's context was cancelled mid-migration.
	cleanCtx := context.WithoutCancel(ctx)

	// A transaction nobody settled would swallow the record update below and
	// hand a dirty connection back to the pool. The orchestrator always ends
	// its own, so this only catches a caller driving the executor by hand.
	_ = e.RollbackTx(cleanCtx) //nolint:errcheck // best-effort, the connection goes back to the pool next

	// Clear the lock record.
	query := fmt.Sprintf(`UPDATE %s SET locked_at = NULL, locked_by = NULL WHERE id = 1`,
		lockTableName)
	_, _ = e.exec(cleanCtx, query) //nolint:errcheck // best-effort lock record clearing

	// Release the advisory lock (on the SAME connection that acquired it).
	_, err := e.exec(cleanCtx, "SELECT pg_advisory_unlock(1)")

	// Release the dedicated connection back to the pool.
	e.releaseDedicated()

	return err
}

// ListApplied returns all migrations that have been applied, ordered by
// migrated_at ascending.
func (e *Executor) ListApplied(ctx context.Context) ([]*migrate.AppliedMigration, error) {
	query := fmt.Sprintf(
		`SELECT id, version, name, "group", migrated_at::text FROM %s ORDER BY id ASC`,
		migrationTableName)

	rows, err := e.query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []*migrate.AppliedMigration
	for rows.Next() {
		a := &migrate.AppliedMigration{}
		if err := rows.Scan(&a.ID, &a.Version, &a.Name, &a.Group, &a.MigratedAt); err != nil {
			return nil, fmt.Errorf("pgmigrate: scan applied: %w", err)
		}
		applied = append(applied, a)
	}
	return applied, rows.Err()
}

// RecordApplied records that a migration was successfully applied.
func (e *Executor) RecordApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (version, name, "group") VALUES ($1, $2, $3)`,
		migrationTableName)
	_, err := e.exec(ctx, query, m.Version, m.Name, m.Group)
	return err
}

// LockInfo reports the current migration lock holder recorded in the lock table.
// It satisfies migrate.LockInspector, letting the orchestrator produce a
// diagnosable error when the lock-wait budget is exhausted.
func (e *Executor) LockInfo(ctx context.Context) (*migrate.LockInfo, error) {
	query := fmt.Sprintf(
		`SELECT locked_by, locked_at::text FROM %s WHERE id = 1`, lockTableName)
	row := e.queryRow(ctx, query)
	var by, at *string
	if err := row.Scan(&by, &at); err != nil {
		// No row means the lock table exists but has no id=1 record yet
		// (i.e. the lock has never been acquired). Treat this as not held.
		// Any other error (connection reset, ctx cancelled, schema mismatch,
		// etc.) is a real failure and must be surfaced to the caller.
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return &migrate.LockInfo{Held: false}, nil //nolint:nilerr // no-rows == not held
		}
		return nil, fmt.Errorf("pgmigrate: read lock info: %w", err)
	}
	info := &migrate.LockInfo{Held: by != nil}
	if by != nil {
		info.LockedBy = *by
	}
	if at != nil {
		info.LockedAt = *at
	}
	return info, nil
}

// RemoveApplied removes the record of an applied migration (for rollback).
func (e *Executor) RemoveApplied(ctx context.Context, m *migrate.Migration) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE version = $1 AND "group" = $2`,
		migrationTableName)
	_, err := e.exec(ctx, query, m.Version, m.Group)
	return err
}

// unusableExecutor is what the factory returns for a driver that isn't a
// driver.Driver. The factory has no error to return: a nil Executor comes back
// from migrate.NewExecutorFor with a nil error, and the orchestrator would
// dereference it on its first call. Failing every method with the reason
// surfaces the mistake from the orchestrator's first step instead.
type unusableExecutor struct{ err error }

var _ migrate.Executor = unusableExecutor{}

func (u unusableExecutor) Exec(context.Context, string, ...any) (driver.Result, error) {
	return nil, u.err
}

func (u unusableExecutor) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, u.err
}

func (u unusableExecutor) EnsureMigrationTable(context.Context) error { return u.err }
func (u unusableExecutor) EnsureLockTable(context.Context) error      { return u.err }
func (u unusableExecutor) AcquireLock(context.Context, string) error  { return u.err }
func (u unusableExecutor) ReleaseLock(context.Context) error          { return u.err }

func (u unusableExecutor) ListApplied(context.Context) ([]*migrate.AppliedMigration, error) {
	return nil, u.err
}

func (u unusableExecutor) RecordApplied(context.Context, *migrate.Migration) error { return u.err }
func (u unusableExecutor) RemoveApplied(context.Context, *migrate.Migration) error { return u.err }
