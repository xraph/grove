package tursodriver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/observability/telemetrytest"
)

type isolationModel struct {
	grove.BaseModel `grove:"table:isolated"`
	N               int64 `grove:"n,pk"`
}
type isolationHook struct {
	t             *testing.T
	inTx          bool
	deny          bool
	before, after int
}

func (h *isolationHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	require.Equal(h.t, h.inTx, qc.InTransaction)
	h.before++
	if h.deny {
		return &hook.HookResult{Decision: hook.Deny}, nil
	}
	return &hook.HookResult{Decision: hook.Modify, Filters: []hook.ExtraFilter{{Clause: "tenant_id = ?", Args: []any{1}}}}, nil
}
func (h *isolationHook) BeforeMutation(ctx context.Context, qc *hook.QueryContext, _ any) (*hook.HookResult, error) {
	return h.BeforeQuery(ctx, qc)
}
func (h *isolationHook) AfterQuery(_ context.Context, qc *hook.QueryContext, _ any) error {
	require.Contains(h.t, qc.RawQuery, "WHERE (n = ? OR n = ?) AND (tenant_id = ?)")
	require.Equal(h.t, []any{1, 2, 1}, qc.RawArgs)
	h.after++
	return nil
}

func TestRegressionPolicyAndTransactionPaths(t *testing.T) {
	db := New()
	var executed string
	db.db = telemetrytest.SQLDB(t, func(q string) error { executed = q; return nil })
	wrapped, err := grove.Open(db)
	require.NoError(t, err)
	h := &isolationHook{t: t}
	wrapped.Hooks().AddHook(h, hook.Scope{})
	model := &isolationModel{N: 1}
	update := db.NewUpdate(model).Set("n = ?", 3).Where("n = ? OR n = ?", 1, 2)
	for i := 0; i < 2; i++ {
		_, err = update.Exec(t.Context())
		require.NoError(t, err)
		require.Contains(t, executed, "WHERE (n = ? OR n = ?) AND (tenant_id = ?)")
	}
	_, err = db.NewDelete(model).Where("n = ?", 1).WhereOr("n = ?", 2).Exec(t.Context())
	require.NoError(t, err)
	require.Contains(t, executed, "WHERE (n = ? OR n = ?) AND (tenant_id = ?)")
	count, err := db.NewSelect(model).Where("n = ?", 1).WhereOr("n = ?", 2).Count(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.Equal(t, 1, h.after)
	var rows []isolationModel
	require.NoError(t, db.NewSelect(&rows).Where("n = ?", 1).WhereOr("n = ?", 2).Scan(t.Context()))
	require.Len(t, rows, 1)
	require.Equal(t, 2, h.after)
	h.deny = true
	h.inTx = true
	tx, err := db.BeginTxQuery(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	before := h.before
	executed = ""
	_, err = tx.NewInsert(model).Exec(t.Context())
	require.Error(t, err)
	_, err = tx.NewUpdate(model).Set("n = ?", 2).Where("n = ?", 1).Exec(t.Context())
	require.Error(t, err)
	_, err = tx.NewDelete(model).Where("n = ?", 1).Exec(t.Context())
	require.Error(t, err)
	require.Error(t, tx.NewSelect(model).Scan(t.Context()))
	_, err = tx.NewSelect(model).Count(t.Context())
	require.Error(t, err)
	require.Equal(t, before+5, h.before)
	require.Empty(t, executed, "denied transaction queries must never reach the driver")
	require.Equal(t, 2, h.after)
}

func TestRegressionQueryTimeout(t *testing.T) {
	db := New()
	db.db = telemetrytest.SQLDB(t, nil)
	db.db.SetMaxOpenConns(1)
	db.opts = driver.DefaultDriverOptions()
	db.SetQueryTimeout(20 * time.Millisecond)
	stmt, err := db.Prepare(t.Context(), "UPDATE isolated SET n = ?")
	require.NoError(t, err)
	defer stmt.Close()
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
		"prepared exec": func(ctx context.Context) error { _, err := stmt.Exec(ctx, 1); return err },
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
