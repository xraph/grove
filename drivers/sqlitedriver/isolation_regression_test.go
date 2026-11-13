package sqlitedriver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
)

type isolationRow struct {
	grove.BaseModel `grove:"table:isolation_rows"`
	ID              int64  `grove:"id,pk"`
	TenantID        int64  `grove:"tenant_id"`
	Name            string `grove:"name"`
}

type isolationTenantHook struct{}

func (isolationTenantHook) BeforeQuery(ctx context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	return isolationFilter(ctx), nil
}
func (isolationTenantHook) BeforeMutation(ctx context.Context, _ *hook.QueryContext, _ any) (*hook.HookResult, error) {
	return isolationFilter(ctx), nil
}

func isolationDB(t *testing.T, withHook bool) *SqliteDB {
	t.Helper()
	db := New()
	require.NoError(t, db.Open(t.Context(), filepath.Join(t.TempDir(), "audit.db"), driver.WithPoolSize(1)))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := db.Exec(t.Context(), "CREATE TABLE isolation_rows (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT)")
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), "INSERT INTO isolation_rows VALUES (10, 1, 'own'), (20, 2, 'other')")
	require.NoError(t, err)
	if withHook {
		wrapped, openErr := grove.Open(db)
		require.NoError(t, openErr)
		wrapped.Hooks().AddHook(isolationTenantHook{}, hook.Scope{})
	}
	return db
}

func TestRegressionMutationFilters(t *testing.T) {
	db := isolationDB(t, true)
	result, err := db.NewDelete((*isolationRow)(nil)).Where("id = ?", 20).Exec(t.Context())
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, affected, "tenant 1 must not delete tenant 2's row")
}

func TestRegressionOrTenantIsolation(t *testing.T) {
	db := isolationDB(t, true)
	var rows []isolationRow
	q := db.NewSelect(&rows).Where("id = ?", 20).WhereOr("id = ?", 10)
	require.NoError(t, q.Scan(t.Context()))
	require.Len(t, rows, 1, "all OR branches must be tenant-scoped")
}

func TestRegressionTransactionHooks(t *testing.T) {
	db := isolationDB(t, true)
	var normal []isolationRow
	require.NoError(t, db.NewSelect(&normal).Scan(t.Context()))
	require.Len(t, normal, 1, "hook works outside transaction")
	tx, err := db.BeginTxQuery(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	var rows []isolationRow
	require.NoError(t, tx.NewSelect(&rows).Scan(t.Context()))
	require.Len(t, rows, 1, "transaction must retain tenant isolation")
}

func TestRegressionCountHooks(t *testing.T) {
	db := isolationDB(t, true)
	count, err := db.NewSelect((*isolationRow)(nil)).Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "count must apply the same tenant filter as Scan")
}

func TestRegressionQueryTimeout(t *testing.T) {
	db := isolationDB(t, false)
	db.SetQueryTimeout(20 * time.Millisecond)
	held, err := db.db.Conn(t.Context())
	require.NoError(t, err)
	defer held.Close()

	operations := map[string]func(context.Context) error{
		"exec": func(ctx context.Context) error { _, err := db.Exec(ctx, "SELECT 1"); return err },
		"query": func(ctx context.Context) error {
			rows, err := db.Query(ctx, "SELECT 1")
			if rows != nil {
				rows.Close()
			}
			return err
		},
		"query row": func(ctx context.Context) error { var n int; return db.QueryRow(ctx, "SELECT 1").Scan(&n) },
		"prepare": func(ctx context.Context) error {
			prepared, err := db.Prepare(ctx, "SELECT 1")
			if prepared != nil {
				prepared.Close()
			}
			return err
		},
		"ping": db.Ping,
	}
	for name, run := range operations {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			started := time.Now()
			require.ErrorIs(t, run(ctx), context.DeadlineExceeded)
			require.Less(t, time.Since(started), 500*time.Millisecond)
		})
	}
}

func TestRegressionStructColumnOrder(t *testing.T) {
	db := isolationDB(t, false)
	var row isolationRow
	require.NoError(t, db.NewSelect(&row).Column("tenant_id", "id", "name").Where("id = ?", 10).Scan(t.Context()))
	require.Equal(t, int64(10), row.ID, "column order must not swap ID and TenantID")
	require.Equal(t, int64(1), row.TenantID)
}

// Reusing a builder must resolve the current caller's policy each time.
func TestRegressionPolicyQueryReuse(t *testing.T) {
	db := isolationDB(t, true)
	var rows []isolationRow
	q := db.NewSelect(&rows).Where("id = ? OR id = ?", 20, 10)
	for _, tenant := range []int64{1, 2, 1} {
		ctx := context.WithValue(t.Context(), isolationTenantKey{}, tenant)
		rows = nil
		require.NoError(t, q.Scan(ctx))
		require.Len(t, rows, 1)
		require.Equal(t, tenant, rows[0].TenantID)
		count, err := q.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	}
}

type isolationTenantKey struct{}

func isolationFilter(ctx context.Context) *hook.HookResult {
	tenant, ok := ctx.Value(isolationTenantKey{}).(int64)
	if !ok {
		tenant = 1
	}
	return &hook.HookResult{Decision: hook.Modify, Filters: []hook.ExtraFilter{{Clause: "tenant_id = ?", Args: []any{tenant}}}}
}

func TestRegressionMutationPolicyPaths(t *testing.T) {
	db := isolationDB(t, true)
	model := &isolationRow{Name: "changed"}
	update := db.NewUpdate(model).Set("name = ?", "changed").Where("id = ? OR id = ?", 20, 20)
	result, err := update.Exec(t.Context())
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, affected)
	var id int64
	require.ErrorIs(t, update.Returning("id").Scan(t.Context(), &id), sql.ErrNoRows)
	require.ErrorIs(t, db.NewDelete(model).Where("id = ? OR id = ?", 20, 20).Returning("id").Scan(t.Context(), &id), sql.ErrNoRows)
	// A permitted execution of the same builder proves a previous policy didn't stick.
	ctx := context.WithValue(t.Context(), isolationTenantKey{}, int64(2))
	require.NoError(t, update.Scan(ctx, &id))
	require.Equal(t, int64(20), id)
	var name string
	require.NoError(t, db.QueryRow(t.Context(), "SELECT name FROM isolation_rows WHERE id = 20").Scan(&name))
	require.Equal(t, "changed", name)
}

type isolationSoftRow struct {
	grove.BaseModel `grove:"table:isolation_rows"`
	ID              int64      `grove:"id,pk"`
	DeletedAt       *time.Time `grove:"deleted_at,soft_delete"`
}

func TestRegressionSoftDeleteOr(t *testing.T) {
	db := isolationDB(t, false)
	_, err := db.Exec(t.Context(), "ALTER TABLE isolation_rows ADD COLUMN deleted_at TEXT")
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), "UPDATE isolation_rows SET deleted_at = 'deleted' WHERE id = 20")
	require.NoError(t, err)
	var rows []isolationSoftRow
	q := db.NewSelect(&rows).Column("id").Where("id = ?", 20).WhereOr("id = ?", 10)
	sqlBefore, argsBefore, err := q.Build()
	require.NoError(t, err)
	require.NoError(t, q.Scan(t.Context()))
	require.Len(t, rows, 1)
	require.Equal(t, int64(10), rows[0].ID)
	count, err := q.Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	sqlAfter, argsAfter, err := q.Build()
	require.NoError(t, err)
	require.Equal(t, sqlBefore, sqlAfter)
	require.Equal(t, argsBefore, argsAfter)
	// An explicit soft-delete condition in one OR branch isn't a guard for all branches.
	count, err = db.NewSelect((*isolationSoftRow)(nil)).Where(`"deleted_at" IS NULL`).WhereOr("id = ?", 20).Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	// Build and Scan must not persist a soft-delete condition into the builder.
	count, err = q.WithDeleted().Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestRegressionStructProjection(t *testing.T) {
	db := isolationDB(t, false)
	var row isolationRow
	require.NoError(t, db.NewSelect(&row).Column("name", "id").Where("id = ?", 10).Scan(t.Context()))
	require.Equal(t, isolationRow{ID: 10, Name: "own"}, row)
	require.NoError(t, db.NewSelect(&row).Column("id AS tenant_id", "tenant_id AS id", "42 AS extra").Where("id = ?", 10).Scan(t.Context()))
	require.Equal(t, int64(1), row.ID)
	require.Equal(t, int64(10), row.TenantID)
	require.ErrorIs(t, db.NewSelect(&row).Column("id").Where("id = ?", -1).Scan(t.Context()), sql.ErrNoRows)
}

type isolationCountHook struct {
	denied bool
	after  int
}

func (h *isolationCountHook) BeforeQuery(context.Context, *hook.QueryContext) (*hook.HookResult, error) {
	if h.denied {
		return &hook.HookResult{Decision: hook.Deny}, nil
	}
	return nil, nil
}
func (h *isolationCountHook) AfterQuery(_ context.Context, qc *hook.QueryContext, result any) error {
	h.after++
	if qc.Operation != hook.OpSelect || !strings.Contains(qc.RawQuery, "COUNT(*)") || result != int64(2) {
		return fmt.Errorf("unexpected count hook context or result: %v %v", qc, result)
	}
	return nil
}
func TestRegressionCountLifecycle(t *testing.T) {
	db := isolationDB(t, false)
	wrapped, err := grove.Open(db)
	require.NoError(t, err)
	h := &isolationCountHook{}
	wrapped.Hooks().AddHook(h, hook.Scope{})
	count, err := db.NewSelect((*isolationRow)(nil)).Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.Equal(t, 1, h.after)
	h.denied = true
	_, err = db.NewSelect((*isolationRow)(nil)).Count(t.Context())
	require.Error(t, err)
	require.Equal(t, 1, h.after)
}
