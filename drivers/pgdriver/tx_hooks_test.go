package pgdriver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/hook"
)

// These tests pin two long-standing gaps:
//
//  1. Queries built through PgTx used to run with a hook-less PgDB clone
//     (txDB dropped the hooks field), so privacy/tenant hooks silently
//     never fired inside transactions.
//  2. Hooks could not tell the pool path from the transaction path; the
//     new QueryContext.InTransaction flag carries that.
//
// The hooks run BEFORE any connection use, so a denying hook lets us
// exercise both paths without a database.

var errStopHook = errors.New("stopped by recording hook")

// recordingDenyHook captures every QueryContext and denies, so execution
// never reaches the (absent) connection pool.
type recordingDenyHook struct {
	qcs []*hook.QueryContext
}

func (h *recordingDenyHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	h.qcs = append(h.qcs, qc)
	return &hook.HookResult{Decision: hook.Deny, Error: errStopHook}, nil
}

func (h *recordingDenyHook) BeforeMutation(_ context.Context, qc *hook.QueryContext, _ any) (*hook.HookResult, error) {
	h.qcs = append(h.qcs, qc)
	return &hook.HookResult{Decision: hook.Deny, Error: errStopHook}, nil
}

// stubTx satisfies driver.Tx; the deny hook guarantees it is never used.
type stubTx struct{}

func (stubTx) Exec(context.Context, string, ...any) (driver.Result, error) {
	return nil, errors.New("stubTx must not execute")
}
func (stubTx) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("stubTx must not execute")
}
func (stubTx) QueryRow(context.Context, string, ...any) driver.Row { return nil }
func (stubTx) Commit() error                                       { return nil }
func (stubTx) Rollback() error                                     { return nil }

func hookedDB(t *testing.T) (*PgDB, *recordingDenyHook) {
	t.Helper()
	db := New()
	h := &recordingDenyHook{}
	engine := hook.NewEngine()
	engine.AddHook(h)
	db.SetHooks(engine)
	return db, h
}

func TestHooks_FireOnPoolPath_NotInTransaction(t *testing.T) {
	db, h := hookedDB(t)

	var users []TestUser
	err := db.NewSelect(&users).Where("role = ?", "admin").Scan(context.Background())
	require.ErrorIs(t, err, errStopHook)
	require.Len(t, h.qcs, 1)
	assert.False(t, h.qcs[0].InTransaction, "pool-path query must report InTransaction=false")
	assert.Equal(t, "users", h.qcs[0].Table)
}

func TestHooks_FireInsideTransactions(t *testing.T) {
	db, h := hookedDB(t)
	ptx := &PgTx{db: db, tx: stubTx{}}

	var users []TestUser
	err := ptx.NewSelect(&users).Where("tenant_id = ?", "t1").Scan(context.Background())
	require.ErrorIs(t, err, errStopHook,
		"hooks must fire for queries built through PgTx (txDB used to drop the engine)")
	require.Len(t, h.qcs, 1)
	assert.True(t, h.qcs[0].InTransaction, "tx-path query must report InTransaction=true")
}

func TestHooks_FireForTxMutations(t *testing.T) {
	db, h := hookedDB(t)
	ptx := &PgTx{db: db, tx: stubTx{}}

	user := &TestUser{Name: "n", Email: "e"}
	_, err := ptx.NewInsert(user).Exec(context.Background())
	require.ErrorIs(t, err, errStopHook)

	_, err = ptx.NewUpdate(user).Where("id = ?", 1).Exec(context.Background())
	require.ErrorIs(t, err, errStopHook)

	_, err = ptx.NewDelete((*TestUser)(nil)).Where("id = ?", 1).Exec(context.Background())
	require.ErrorIs(t, err, errStopHook)

	require.Len(t, h.qcs, 3)
	for i, qc := range h.qcs {
		assert.True(t, qc.InTransaction, "mutation %d must report InTransaction=true", i)
	}
	assert.Equal(t, hook.OpInsert, h.qcs[0].Operation)
	assert.Equal(t, hook.OpUpdate, h.qcs[1].Operation)
	assert.Equal(t, hook.OpDelete, h.qcs[2].Operation)
}

func TestHookContext_ConditionsPopulated(t *testing.T) {
	db := New()

	q := db.NewSelect(&[]TestUser{}).
		Where("tenant_id = ?", "t1").
		Where(`"role" != ?`, "ghost").
		Where("age >= ?", 18).
		WhereArray("id", "= ANY", []int64{1, 2})

	qc := q.buildSelectHookContext()
	require.Len(t, qc.Conditions, 4)

	assert.Equal(t, hook.Condition{Column: "tenant_id", Operator: "=", Value: "t1"}, qc.Conditions[0])
	assert.Equal(t, hook.Condition{Column: "role", Operator: "!=", Value: "ghost"}, qc.Conditions[1])
	assert.Equal(t, hook.Condition{Column: "age", Operator: ">=", Value: 18}, qc.Conditions[2])
	assert.Equal(t, "id", qc.Conditions[3].Column)
	assert.Equal(t, "= ANY", qc.Conditions[3].Operator)
}

func TestHookContext_ConditionsBestEffort(t *testing.T) {
	db := New()

	// Complex fragments are skipped, simple ones still extracted —
	// Conditions is informational, never a parser of last resort.
	q := db.NewSelect(&[]TestUser{}).
		Where("(name ILIKE ? OR email ILIKE ?)", "%a%", "%a%").
		Where("deleted_at IS NULL").
		Where("tenant_id = ?", "t1")

	qc := q.buildSelectHookContext()
	cols := map[string]string{}
	for _, c := range qc.Conditions {
		cols[c.Column] = c.Operator
	}
	assert.Equal(t, "=", cols["tenant_id"])
	assert.Equal(t, "IS", cols["deleted_at"])
	assert.NotContains(t, cols, "(name")
}

func TestHookContext_UpdateDeleteConditions(t *testing.T) {
	db := New()

	uq := db.NewUpdate(&TestUser{}).Where("tenant_id = ?", "t1")
	uqc := uq.buildUpdateHookContext()
	require.Len(t, uqc.Conditions, 1)
	assert.Equal(t, "tenant_id", uqc.Conditions[0].Column)

	dq := db.NewDelete((*TestUser)(nil)).Where("tenant_id = ?", "t1")
	dqc := dq.buildDeleteHookContext()
	require.Len(t, dqc.Conditions, 1)
	assert.Equal(t, "tenant_id", dqc.Conditions[0].Column)
}
