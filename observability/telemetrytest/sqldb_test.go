package telemetrytest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLDB_QueryReturnsOneRow(t *testing.T) {
	db := SQLDB(t, nil)

	rows, err := db.QueryContext(t.Context(), "SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, cols)

	var got []int64
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		got = append(got, n)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []int64{1}, got)
}

func TestSQLDB_ErrNoRowsMeansEmptyResult(t *testing.T) {
	db := SQLDB(t, func(string) error { return sql.ErrNoRows })

	var n int64
	err := db.QueryRowContext(t.Context(), "SELECT n").Scan(&n)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestSQLDB_FailDecidesPerStatement(t *testing.T) {
	boom := errors.New("constraint violated")
	db := SQLDB(t, func(q string) error {
		if strings.Contains(q, "bad") {
			return boom
		}

		return nil
	})

	res, err := db.ExecContext(t.Context(), "UPDATE good")
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	_, err = db.ExecContext(t.Context(), "UPDATE bad")
	require.ErrorIs(t, err, boom)

	_, err = db.QueryContext(t.Context(), "SELECT bad") //nolint:rowserrcheck,sqlclosecheck // the query fails, there are no rows
	require.ErrorIs(t, err, boom)
}

func TestSQLDB_CanceledContext(t *testing.T) {
	db := SQLDB(t, nil)
	// Open the connection first so the cancellation reaches the fake conn.
	require.NoError(t, db.PingContext(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := db.ExecContext(ctx, "UPDATE x")
	require.ErrorIs(t, err, context.Canceled)
}

func TestSQLDB_TransactionsAndPreparedStatements(t *testing.T) {
	db := SQLDB(t, nil)

	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "INSERT x")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	// The fake connection implements the context interfaces; the legacy
	// driver paths are exercised directly.
	conn, err := (&fakeConnector{fail: func(string) error { return nil }}).Driver().Open("")
	require.NoError(t, err)
	defer conn.Close()

	legacyTx, err := conn.Begin() //nolint:staticcheck // exercising the deprecated driver.Conn method
	require.NoError(t, err)
	require.NoError(t, legacyTx.Commit())

	stmt, err := conn.Prepare("SELECT n")
	require.NoError(t, err)
	defer stmt.Close()
	assert.Equal(t, -1, stmt.NumInput())

	r, err := stmt.Exec(nil) //nolint:staticcheck // exercising the deprecated driver.Stmt method
	require.NoError(t, err)
	affected, err := r.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	rows, err := stmt.Query(nil) //nolint:staticcheck // exercising the deprecated driver.Stmt method
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, rows.Columns())
	require.NoError(t, rows.Close())
}
