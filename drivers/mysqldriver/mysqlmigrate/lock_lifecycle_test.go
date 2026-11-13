package mysqlmigrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

type lockDriver struct {
	driver.Driver
	conn     *lockConn
	acquires int
}

func (d *lockDriver) AcquireConn(context.Context) (driver.DedicatedConn, error) {
	d.acquires++
	return d.conn, nil
}

type lockConn struct {
	driver.DedicatedConn
	releases, discards, unlocks int
	unlockErr                   error
}

func (c *lockConn) QueryRow(context.Context, string, ...any) driver.Row { return lockRow{} }
func (c *lockConn) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	if strings.Contains(query, "unlock") || strings.Contains(query, "RELEASE_LOCK") {
		c.unlocks++
		return nil, c.unlockErr
	}
	return nil, nil
}
func (c *lockConn) Release() { c.releases++ }
func (c *lockConn) Discard() { c.discards++ }

type lockRow struct{}

func (lockRow) Scan(dest ...any) error {
	switch ptr := dest[0].(type) {
	case *bool:
		*ptr = true
	case *int:
		*ptr = 1
	}
	return nil
}

func TestLockRejectsRepeatedAcquisition(t *testing.T) {
	conn := &lockConn{}
	d := &lockDriver{conn: conn}
	e := New(d)
	require.NoError(t, e.AcquireLock(t.Context(), "first"))
	require.ErrorIs(t, e.AcquireLock(t.Context(), "second"), migrate.ErrLockHeld)
	require.Equal(t, 1, d.acquires, "must keep the original dedicated connection")
	require.NoError(t, e.ReleaseLock(t.Context()))
	require.NoError(t, e.ReleaseLock(t.Context()))
	require.Equal(t, 1, conn.releases)
	require.Equal(t, 1, conn.unlocks, "release once without a redundant unlock round trip")
}

func TestUnlockFailureDiscardsConnection(t *testing.T) {
	conn := &lockConn{}
	e := New(&lockDriver{conn: conn})
	require.NoError(t, e.AcquireLock(t.Context(), "first"))
	conn.unlockErr = errors.New("unlock timed out before sending")
	require.ErrorIs(t, e.ReleaseLock(t.Context()), conn.unlockErr)
	require.Equal(t, 1, conn.discards, "a possibly locked session must not go back into the pool")
	require.Zero(t, conn.releases)
	require.Nil(t, e.dedicated)
}
