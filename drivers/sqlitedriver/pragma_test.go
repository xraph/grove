package sqlitedriver

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

// pooledConns are how many connections the tests below hold open at once, so
// database/sql has to dial a new one for each instead of reusing the first.
const pooledConns = 4

// holdConns checks out n connections from the pool and keeps every one of
// them busy until the test ends, so each is a distinct SQLite connection.
func holdConns(t *testing.T, db *SqliteDB, n int) []*sql.Conn {
	t.Helper()
	ctx := context.Background()
	conns := make([]*sql.Conn, 0, n)
	for range n {
		c, err := db.db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		conns = append(conns, c)
	}
	return conns
}

func foreignKeysOn(t *testing.T, c *sql.Conn) int {
	t.Helper()
	var on int
	require.NoError(t, c.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&on))
	return on
}

// Foreign keys are a per-connection setting in SQLite. Every connection in
// the pool has to have them on, or ON DELETE CASCADE fires on some queries
// and silently doesn't on others.
func TestOpen_foreignKeysOnEveryPooledConnection(t *testing.T) {
	ctx := context.Background()
	db := New()
	require.NoError(t, db.Open(ctx, filepath.Join(t.TempDir(), "fk.db"), driver.WithPoolSize(pooledConns)))
	t.Cleanup(func() { _ = db.Close() })

	_, err := db.Exec(ctx, `CREATE TABLE parents (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `CREATE TABLE children (
		id        INTEGER PRIMARY KEY,
		parent_id INTEGER NOT NULL REFERENCES parents(id) ON DELETE CASCADE
	)`)
	require.NoError(t, err)
	for i := 1; i <= pooledConns; i++ {
		_, err = db.Exec(ctx, `INSERT INTO parents (id) VALUES (?)`, i)
		require.NoError(t, err)
		_, err = db.Exec(ctx, `INSERT INTO children (id, parent_id) VALUES (?, ?)`, i, i)
		require.NoError(t, err)
	}

	conns := holdConns(t, db, pooledConns)
	for i, c := range conns {
		assert.Equal(t, 1, foreignKeysOn(t, c), "PRAGMA foreign_keys on pooled connection %d", i)

		parent := i + 1
		_, err := c.ExecContext(ctx, `DELETE FROM parents WHERE id = ?`, parent)
		require.NoError(t, err)
		var left int
		require.NoError(t, c.QueryRowContext(ctx, `SELECT count(*) FROM children WHERE parent_id = ?`, parent).Scan(&left))
		assert.Zero(t, left, "ON DELETE CASCADE on pooled connection %d left %d child rows", i, left)
	}
}

// A DSN the caller already shaped keeps its own parameters, and a caller who
// turns foreign keys off on purpose gets that on every connection.
func TestOpen_keepsCallerDSN(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	tests := []struct {
		name   string
		dsn    string
		wantFK int
	}{
		{"plain path", filepath.Join(dir, "plain.db"), 1},
		{"path with a query", filepath.Join(dir, "query.db") + "?_pragma=busy_timeout(5000)", 1},
		{"file URI with a query", "file:" + filepath.Join(dir, "uri.db") + "?mode=rwc&_pragma=busy_timeout(5000)", 1},
		{"trailing question mark", filepath.Join(dir, "trailing.db") + "?", 1},
		{"in memory", ":memory:", 1},
		{"empty", "", 1},
		{"caller turns foreign keys off", filepath.Join(dir, "off.db") + "?_pragma=foreign_keys(0)", 0},
		{"caller uses the = form", filepath.Join(dir, "eq.db") + "?_pragma=FOREIGN_KEYS%3DOFF", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := New()
			require.NoError(t, db.Open(ctx, tt.dsn, driver.WithPoolSize(pooledConns)))
			t.Cleanup(func() { _ = db.Close() })

			for i, c := range holdConns(t, db, pooledConns) {
				assert.Equal(t, tt.wantFK, foreignKeysOn(t, c), "PRAGMA foreign_keys on pooled connection %d", i)
				if tt.name == "path with a query" || tt.name == "file URI with a query" {
					var busy int
					require.NoError(t, c.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy))
					assert.Equal(t, 5000, busy, "caller's busy_timeout on pooled connection %d", i)
				}
			}
		})
	}
}
func TestWithForeignKeys(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"app.db", "app.db?_pragma=foreign_keys%281%29"},
		{"app.db?", "app.db?_pragma=foreign_keys%281%29"},
		{"app.db?_pragma=busy_timeout(5000)", "app.db?_pragma=busy_timeout(5000)&_pragma=foreign_keys%281%29"},
		{"app.db?_txlock=immediate&", "app.db?_txlock=immediate&_pragma=foreign_keys%281%29"},
		{"file:app.db?mode=ro", "file:app.db?mode=ro&_pragma=foreign_keys%281%29"},
		{":memory:", ":memory:?_pragma=foreign_keys%281%29"},
		{"", "file:?_pragma=foreign_keys%281%29"},
		{"app.db?_pragma=foreign_keys(0)", "app.db?_pragma=foreign_keys(0)"},
		{"app.db?_pragma=%20Foreign_Keys%20%3D%20off", "app.db?_pragma=%20Foreign_Keys%20%3D%20off"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, withForeignKeys(tt.dsn), "withForeignKeys(%q)", tt.dsn)
	}
}
