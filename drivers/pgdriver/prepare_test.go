package pgdriver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
)

// openSingleConnDB opens a pool capped at one connection, so every
// transaction and statement in a test lands on the same server session.
// That's where prepared statement names collide.
func openSingleConnDB(t *testing.T) *pgdriver.PgDB {
	t.Helper()
	return openPgDB(t, startPostgres(t)+"&pool_max_conns=1")
}

func beginPreparer(t *testing.T, db *pgdriver.PgDB) (driver.Tx, driver.Preparer) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	p, ok := tx.(driver.Preparer)
	require.True(t, ok, "pgdriver transaction must implement driver.Preparer")
	return tx, p
}

// groveStatements counts the statements grove prepared that the session
// still holds. pgx's own statement cache is left out of the count.
func groveStatements(t *testing.T, db *pgdriver.PgDB) int {
	t.Helper()
	var n int
	row := db.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'grove\_%'`)
	require.NoError(t, row.Scan(&n))
	return n
}

// Two different statements prepared in one transaction used to share the
// name grove_txps_<tx pointer>, and the second PREPARE failed with 42P05.
func TestTxPrepare_DistinctStatementsInOneTx(t *testing.T) {
	db := openSingleConnDB(t)
	ctx := context.Background()

	tx, p := beginPreparer(t, db)
	defer func() { _ = tx.Rollback() }()

	first, err := p.Prepare(ctx, "SELECT $1::int + 1")
	require.NoError(t, err)
	second, err := p.Prepare(ctx, "SELECT $1::int + 2")
	require.NoError(t, err, "second Prepare in the same transaction")

	_, err = first.Exec(ctx, 1)
	require.NoError(t, err)
	_, err = second.Exec(ctx, 1)
	require.NoError(t, err)

	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	require.NoError(t, tx.Commit())
}

// Statements prepared in a transaction must not outlive it on the pooled
// connection, whether the caller closed them, forgot to, or the
// transaction failed. Leftovers pile up for the life of the connection,
// and under the old naming a later transaction could reuse a name.
func TestTxPrepare_LeavesNoStatementsOnConnection(t *testing.T) {
	db := openSingleConnDB(t)
	ctx := context.Background()

	for i := range 30 {
		tx, p := beginPreparer(t, db)
		stmt, err := p.Prepare(ctx, fmt.Sprintf("SELECT $1::int + %d", i))
		require.NoError(t, err, "transaction %d", i)
		_, err = stmt.Exec(ctx, i)
		require.NoError(t, err, "transaction %d", i)

		switch i % 3 {
		case 0: // closed, then committed
			require.NoError(t, stmt.Close())
			require.NoError(t, tx.Commit())
		case 1: // never closed, committed
			require.NoError(t, tx.Commit())
		case 2: // transaction aborted by a failing statement, never closed
			_, err = stmt.Exec(ctx, "not a number")
			require.Error(t, err)
			require.NoError(t, tx.Rollback())
		}
	}

	require.Equal(t, 0, groveStatements(t, db))
}

type prepUser struct {
	grove.BaseModel `grove:"table:prep_users"`
	ID              int    `grove:"id,pk"`
	Name            string `grove:"name"`
}

type prepPost struct {
	grove.BaseModel `grove:"table:prep_posts"`
	ID              int    `grove:"id,pk"`
	Title           string `grove:"title"`
}

// The path that hit this in production: two slice inserts in one
// transaction each run a prepared-statement loop.
func TestTxBulkInsert_TwoTablesInOneTx(t *testing.T) {
	db := openSingleConnDB(t)
	ctx := context.Background()

	_, err := db.Exec(ctx, `CREATE TABLE prep_users (id int PRIMARY KEY, name text)`)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `CREATE TABLE prep_posts (id int PRIMARY KEY, title text)`)
	require.NoError(t, err)

	tx, err := db.BeginTxQuery(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	users := []prepUser{{ID: 1, Name: "ada"}, {ID: 2, Name: "lin"}}
	_, err = tx.NewInsert(&users).Exec(ctx)
	require.NoError(t, err)

	posts := []prepPost{{ID: 1, Title: "one"}, {ID: 2, Title: "two"}}
	_, err = tx.NewInsert(&posts).Exec(ctx)
	require.NoError(t, err, "second bulk insert in the same transaction")

	require.NoError(t, tx.Commit())

	var n int
	require.NoError(t, db.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM prep_users) + (SELECT count(*) FROM prep_posts)`).Scan(&n))
	require.Equal(t, 4, n)
	require.Equal(t, 0, groveStatements(t, db))
}

// A statement prepared on the pool is deallocated when it's closed, so the
// connection it held goes back clean.
func TestPoolPrepare_CloseDeallocates(t *testing.T) {
	db := openSingleConnDB(t)
	ctx := context.Background()

	for i := range 5 {
		stmt, err := db.Prepare(ctx, fmt.Sprintf("SELECT $1::int + %d", i))
		require.NoError(t, err)
		_, err = stmt.Exec(ctx, i)
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
	}

	require.Equal(t, 0, groveStatements(t, db))
}
