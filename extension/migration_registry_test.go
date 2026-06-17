package extension

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/migrate"
)

// --- fake driver for registry tests ---

type fakeRegDriver struct{}

func (f *fakeRegDriver) Name() string              { return "reg_test_driver" }
func (f *fakeRegDriver) Close() error              { return nil }
func (f *fakeRegDriver) Ping(_ context.Context) error { return nil }

// --- recordingExecutor: succeeds every operation ---

type recordingExecutor struct{}

func (r *recordingExecutor) AcquireLock(_ context.Context, _ string) error { return nil }
func (r *recordingExecutor) ReleaseLock(_ context.Context) error           { return nil }
func (r *recordingExecutor) EnsureMigrationTable(_ context.Context) error  { return nil }
func (r *recordingExecutor) EnsureLockTable(_ context.Context) error       { return nil }
func (r *recordingExecutor) ListApplied(_ context.Context) ([]*migrate.AppliedMigration, error) {
	return nil, nil
}
func (r *recordingExecutor) RecordApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (r *recordingExecutor) RemoveApplied(_ context.Context, _ *migrate.Migration) error {
	return nil
}
func (r *recordingExecutor) Exec(_ context.Context, _ string, _ ...any) (driver.Result, error) {
	return nil, nil
}
func (r *recordingExecutor) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil
}

func init() {
	migrate.RegisterExecutor("reg_test_driver", func(_ any) migrate.Executor {
		return &recordingExecutor{}
	})
}

// TestMigrationRegistry_MergesGroupsAndOrders proves cross-extension DependsOn
// is resolved when both groups share one orchestrator via the registry.
func TestMigrationRegistry_MergesGroupsAndOrders(t *testing.T) {
	var order []string

	identity := migrate.NewGroup("identity/postgres")
	if err := identity.Register(&migrate.Migration{
		Version: "0001",
		Name:    "users",
		Up: func(ctx context.Context, e migrate.Executor) error {
			order = append(order, "identity")
			return nil
		},
	}); err != nil {
		t.Fatalf("identity.Register: %v", err)
	}

	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	if err := trove.Register(&migrate.Migration{
		Version: "0001",
		Name:    "items",
		Up: func(ctx context.Context, e migrate.Executor) error {
			order = append(order, "trove")
			return nil
		},
	}); err != nil {
		t.Fatalf("trove.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("", &fakeRegDriver{}, identity)
	r.Contribute("", &fakeRegDriver{}, trove)

	res, err := r.RunAll(context.Background())
	if err != nil {
		t.Fatalf("RunAll should resolve cross-extension DependsOn, got %v", err)
	}

	if res.Applied != 2 {
		t.Errorf("Applied = %d, want 2", res.Applied)
	}

	if len(order) != 2 || order[0] != "identity" || order[1] != "trove" {
		t.Errorf("execution order = %v, want [identity trove]", order)
	}
}

// TestMigrationRegistry_UnresolvedDepFails proves that a missing DependsOn
// group fails loudly rather than silently succeeding.
func TestMigrationRegistry_UnresolvedDepFails(t *testing.T) {
	// Contribute only trove whose dep "identity/postgres" is absent.
	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	if err := trove.Register(&migrate.Migration{
		Version: "0001",
		Name:    "items",
		Up:      func(ctx context.Context, e migrate.Executor) error { return nil },
	}); err != nil {
		t.Fatalf("trove.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("", &fakeRegDriver{}, trove)

	_, err := r.RunAll(context.Background())
	if err == nil {
		t.Fatal("RunAll should fail when a DependsOn group is missing")
	}
	if !strings.Contains(err.Error(), "unknown group") {
		t.Errorf("error should mention 'unknown group', got: %v", err)
	}
}

// TestMigrationRegistry_RollbackReverse verifies RollbackAll runs without error.
// Because recordingExecutor.ListApplied returns nil, the orchestrator's rollback
// is a no-op (nothing applied), so RolledBack == 0.
func TestMigrationRegistry_RollbackReverse(t *testing.T) {
	identity := migrate.NewGroup("identity/postgres")
	if err := identity.Register(&migrate.Migration{
		Version: "0001",
		Name:    "users",
		Up:      func(ctx context.Context, e migrate.Executor) error { return nil },
	}); err != nil {
		t.Fatalf("identity.Register: %v", err)
	}

	trove := migrate.NewGroup("trove/postgres", migrate.DependsOn("identity/postgres"))
	if err := trove.Register(&migrate.Migration{
		Version: "0001",
		Name:    "items",
		Up:      func(ctx context.Context, e migrate.Executor) error { return nil },
	}); err != nil {
		t.Fatalf("trove.Register: %v", err)
	}

	r := NewMigrationRegistry()
	r.Contribute("", &fakeRegDriver{}, identity)
	r.Contribute("", &fakeRegDriver{}, trove)

	// Run forward first.
	if _, err := r.RunAll(context.Background()); err != nil {
		t.Fatalf("RunAll: %v", err)
	}

	// Now rollback — recordingExecutor returns no applied migrations so this is a no-op.
	res, err := r.RollbackAll(context.Background())
	if err != nil {
		t.Fatalf("RollbackAll: %v", err)
	}
	// No-op executor returns 0 rolled back — that's the honest assertion.
	if res.RolledBack != 0 {
		t.Errorf("RolledBack = %d, want 0 (no-op executor)", res.RolledBack)
	}
}
