package grove

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock driver: minimal (only GroveDriver — no tx or query builder support)
// ---------------------------------------------------------------------------

type minimalDriver struct {
	name   string
	closed bool
}

func (d *minimalDriver) Name() string                 { return d.name }
func (d *minimalDriver) Close() error                 { d.closed = true; return nil }
func (d *minimalDriver) Ping(_ context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Mock driver: full (GroveDriver + txBeginner + queryBuilder)
// ---------------------------------------------------------------------------

type fullDriver struct {
	minimalDriver
	txErr error // if non-nil, GroveTx returns this error
}

func (d *fullDriver) GroveTx(_ context.Context, _ int, _ bool) (any, error) {
	if d.txErr != nil {
		return nil, d.txErr
	}
	return &mockTx{}, nil
}

func (d *fullDriver) GroveSelect(_ ...any) any { return "select-builder" }
func (d *fullDriver) GroveInsert(_ any) any    { return "insert-builder" }
func (d *fullDriver) GroveUpdate(_ any) any    { return "update-builder" }
func (d *fullDriver) GroveDelete(_ any) any    { return "delete-builder" }

// ---------------------------------------------------------------------------
// Mock transaction (supports Commit and Rollback)
// ---------------------------------------------------------------------------

type mockTx struct {
	committed  bool
	rolledBack bool
}

func (tx *mockTx) Commit() error   { tx.committed = true; return nil }
func (tx *mockTx) Rollback() error { tx.rolledBack = true; return nil }

// ---------------------------------------------------------------------------
// Mock transaction without Commit/Rollback (plain struct)
// ---------------------------------------------------------------------------

type bareUnderlying struct{}

// =========================================================================
// Tests: Open
// =========================================================================

func TestOpen_NilDriver(t *testing.T) {
	_, err := Open(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "driver must not be nil")
}

func TestOpen_Success(t *testing.T) {
	db, err := Open(&minimalDriver{name: "test"})
	require.NoError(t, err)
	require.NotNil(t, db)
	assert.Equal(t, "test", db.Driver().Name())
}

// =========================================================================
// Tests: Hooks()
// =========================================================================

func TestDB_Hooks_NonNil(t *testing.T) {
	db, err := Open(&minimalDriver{name: "test"})
	require.NoError(t, err)
	assert.NotNil(t, db.Hooks(), "Hooks() should return a non-nil engine")
}

// =========================================================================
// Tests: BeginTx
// =========================================================================

func TestBeginTx_DriverSupports(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// Verify Raw() returns the mock transaction.
	raw := tx.Raw()
	require.NotNil(t, raw)
	_, ok := raw.(*mockTx)
	assert.True(t, ok, "Raw() should return the underlying *mockTx")
}

func TestBeginTx_Commit(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	mt := tx.Raw().(*mockTx)
	assert.True(t, mt.committed, "underlying tx should have been committed")
}

func TestBeginTx_Rollback(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	mt := tx.Raw().(*mockTx)
	assert.True(t, mt.rolledBack, "underlying tx should have been rolled back")
}

func TestBeginTx_WithOptions(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	opts := &TxOptions{IsolationLevel: 2, ReadOnly: true}
	tx, err := db.BeginTx(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, tx)
}

func TestBeginTx_DriverDoesNotSupport(t *testing.T) {
	db, err := Open(&minimalDriver{name: "minimal"})
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	assert.Nil(t, tx)
	assert.ErrorIs(t, err, ErrNotSupported)
}

func TestBeginTx_DriverError(t *testing.T) {
	txErr := errors.New("connection refused")
	db, err := Open(&fullDriver{
		minimalDriver: minimalDriver{name: "full"},
		txErr:         txErr,
	})
	require.NoError(t, err)

	tx, err := db.BeginTx(context.Background(), nil)
	assert.Nil(t, tx)
	assert.ErrorIs(t, err, txErr)
}

func TestBeginTx_ClosedDB(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	require.NoError(t, db.Close())

	tx, err := db.BeginTx(context.Background(), nil)
	assert.Nil(t, tx)
	assert.ErrorIs(t, err, ErrDriverClosed)
}

// =========================================================================
// Tests: Tx Commit/Rollback on bare underlying (no interface)
// =========================================================================

func TestTx_Commit_NoInterface(t *testing.T) {
	tx := &Tx{underlying: &bareUnderlying{}}
	err := tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support Commit")
}

func TestTx_Rollback_NoInterface(t *testing.T) {
	tx := &Tx{underlying: &bareUnderlying{}}
	err := tx.Rollback()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support Rollback")
}

// =========================================================================
// Tests: NewSelect / NewInsert / NewUpdate / NewDelete
// =========================================================================

func TestNewSelect_DriverSupports(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	result := db.NewSelect()
	assert.NotNil(t, result, "NewSelect should return non-nil when driver supports queryBuilder")
	assert.Equal(t, "select-builder", result)
}

func TestNewSelect_DriverDoesNotSupport(t *testing.T) {
	db, err := Open(&minimalDriver{name: "minimal"})
	require.NoError(t, err)

	result := db.NewSelect()
	assert.Nil(t, result, "NewSelect should return nil when driver does not support queryBuilder")
}

func TestNewInsert_DriverSupports(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	result := db.NewInsert("model")
	assert.NotNil(t, result)
	assert.Equal(t, "insert-builder", result)
}

func TestNewInsert_DriverDoesNotSupport(t *testing.T) {
	db, err := Open(&minimalDriver{name: "minimal"})
	require.NoError(t, err)

	result := db.NewInsert("model")
	assert.Nil(t, result)
}

func TestNewUpdate_DriverSupports(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	result := db.NewUpdate("model")
	assert.NotNil(t, result)
	assert.Equal(t, "update-builder", result)
}

func TestNewUpdate_DriverDoesNotSupport(t *testing.T) {
	db, err := Open(&minimalDriver{name: "minimal"})
	require.NoError(t, err)

	result := db.NewUpdate("model")
	assert.Nil(t, result)
}

func TestNewDelete_DriverSupports(t *testing.T) {
	db, err := Open(&fullDriver{minimalDriver: minimalDriver{name: "full"}})
	require.NoError(t, err)

	result := db.NewDelete("model")
	assert.NotNil(t, result)
	assert.Equal(t, "delete-builder", result)
}

func TestNewDelete_DriverDoesNotSupport(t *testing.T) {
	db, err := Open(&minimalDriver{name: "minimal"})
	require.NoError(t, err)

	result := db.NewDelete("model")
	assert.Nil(t, result)
}

// =========================================================================
// Tests: Close and Ping
// =========================================================================

func TestDB_Close(t *testing.T) {
	drv := &minimalDriver{name: "test"}
	db, err := Open(drv)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
	assert.True(t, drv.closed, "driver should have been closed")

	// Double close returns ErrDriverClosed.
	err = db.Close()
	assert.ErrorIs(t, err, ErrDriverClosed)
}

func TestDB_Ping_ClosedDB(t *testing.T) {
	db, err := Open(&minimalDriver{name: "test"})
	require.NoError(t, err)

	require.NoError(t, db.Close())

	err = db.Ping(context.Background())
	assert.ErrorIs(t, err, ErrDriverClosed)
}

func TestDB_Ping_Success(t *testing.T) {
	db, err := Open(&minimalDriver{name: "test"})
	require.NoError(t, err)

	err = db.Ping(context.Background())
	require.NoError(t, err)
}

// =========================================================================
// Tests: RegisterModel
// =========================================================================

func TestDB_RegisterModel(t *testing.T) {
	db, err := Open(&minimalDriver{name: "test"})
	require.NoError(t, err)

	type User struct{}
	type Post struct{}

	db.RegisterModel((*User)(nil), (*Post)(nil))

	db.mu.RLock()
	defer db.mu.RUnlock()
	assert.Len(t, db.models, 2)
}
