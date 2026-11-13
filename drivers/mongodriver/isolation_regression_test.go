package mongodriver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xraph/grove"
	"github.com/xraph/grove/hook"
)

type isolationModel struct {
	grove.BaseModel `grove:"table:isolated" bson:"-"`
	ID              int    `grove:"id,pk" bson:"_id"`
	Tenant          int    `grove:"tenant_id" bson:"tenant_id"`
	Name            string `grove:"name" bson:"name"`
}
type isolationHook struct {
	tenant int
	denied bool
	counts int
}

func (h *isolationHook) BeforeQuery(context.Context, *hook.QueryContext) (*hook.HookResult, error) {
	if h.denied {
		return &hook.HookResult{Decision: hook.Deny}, nil
	}
	// Both native BSON forms must work, and multiple policies must intersect.
	return &hook.HookResult{Decision: hook.Modify, Filters: []hook.ExtraFilter{
		{NativeFilter: bson.D{{Key: "tenant_id", Value: h.tenant}}},
		{NativeFilter: bson.M{"$or": bson.A{bson.M{"_id": 10}, bson.M{"_id": 20}}}},
	}}, nil
}
func (h *isolationHook) BeforeMutation(ctx context.Context, qc *hook.QueryContext, _ any) (*hook.HookResult, error) {
	return h.BeforeQuery(ctx, qc)
}
func (h *isolationHook) AfterQuery(_ context.Context, _ *hook.QueryContext, result any) error {
	if _, ok := result.(int64); ok {
		h.counts++
	}
	return nil
}

func TestRegressionNativePolicyIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}
	ctx := t.Context()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db := New()
	require.NoError(t, db.Open(ctx, uri, WithDatabase("isolation")))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Collection("isolated").InsertMany(ctx, []any{
		bson.M{"_id": 10, "tenant_id": 1, "name": "own"},
		bson.M{"_id": 20, "tenant_id": 2, "name": "other"},
	})
	require.NoError(t, err)
	wrapped, err := grove.Open(db)
	require.NoError(t, err)
	policy := &isolationHook{tenant: 1}
	wrapped.Hooks().AddHook(policy, hook.Scope{})
	filter := bson.M{"$or": bson.A{bson.M{"_id": 20}, bson.M{"_id": 10}}}
	var rows []isolationModel
	find := db.NewFind(&rows).Filter(filter)
	for _, tenant := range []int{1, 2, 1} {
		policy.tenant = tenant
		rows = nil
		require.NoError(t, find.Scan(ctx))
		require.Len(t, rows, 1)
		require.Equal(t, tenant, rows[0].Tenant)
		count, countErr := find.Count(ctx)
		require.NoError(t, countErr)
		require.Equal(t, int64(1), count)
	}
	require.Equal(t, 3, policy.counts)
	// A contradictory caller filter must not be overwritten by the policy.
	count, err := db.NewFind((*isolationModel)(nil)).Filter(bson.M{"tenant_id": 2}).Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	update := db.NewUpdate(&isolationModel{}).Filter(bson.M{"_id": 20}).Set("name", "changed")
	res, err := update.Exec(ctx)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, affected)
	res, err = db.NewDelete((*isolationModel)(nil)).Filter(bson.M{"_id": 20}).Exec(ctx)
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, affected)
	policy.tenant = 2
	res, err = update.Exec(ctx)
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "reusing a mutation must not retain the previous caller's policy")
	policy.denied = true
	_, err = find.Count(ctx)
	require.Error(t, err)
	require.Equal(t, 4, policy.counts, "a denied count must not invoke post-query hooks")
	require.Equal(t, bson.M{"$or": bson.A{bson.M{"_id": 20}, bson.M{"_id": 10}}}, filter)
}
